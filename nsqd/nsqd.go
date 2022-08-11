package nsqd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/twmb/murmur3"
	"github.com/youzan/nsq/internal/clusterinfo"
	"github.com/youzan/nsq/internal/dirlock"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/statsd"
	"github.com/youzan/nsq/internal/util"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqd/engine"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

const (
	jobQueueChLen = 100
)

type errStore struct {
	err error
}

var (
	ErrTopicPartitionMismatch = errors.New("topic partition mismatch")
	ErrTopicNotExist          = errors.New("topic does not exist")
)

var DEFAULT_RETENTION_DAYS = 3

var EnableDelayedQueue = int32(1)

const (
	FLUSH_DISTANCE = 4
)

type INsqdNotify interface {
	NotifyDeleteTopic(*Topic)
	NotifyStateChanged(v interface{}, needPersist bool)
	ReqToEnd(*Channel, *Message, time.Duration) error
	NotifyScanChannel(c *Channel, wait bool) bool
	PushTopicJob(string, func())
}

type ReqToEndFunc func(*Channel, *Message, time.Duration) error

type NSQD struct {
	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	errValue  atomic.Value
	startTime time.Time

	topicMap           map[string]map[int]*Topic
	localCacheTopicMap map[string]map[int]*Topic
	magicCodeMutex     sync.Mutex

	poolSize         int
	topicJobPoolSize int
	topicJobChList   [16]chan func()

	MetaNotifyChan       chan interface{}
	OptsNotificationChan chan struct{}
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper

	ci               *clusterinfo.ClusterInfo
	exiting          bool
	pubLoopFunc      func(t *Topic)
	reqToEndCB       ReqToEndFunc
	scanTriggerChan  chan *Channel
	persistNotifyCh  chan struct{}
	persistClosed    chan struct{}
	persistWaitGroup util.WaitGroupWrapper
	metaStorage      IMetaStorage
	kvTopicStorage   engine.KVEngine
	sharedCfg        engine.SharedRockConfig
}

func New(opts *Options) (*NSQD, error) {
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
		opts.DataPath = dataPath
	}
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		nsqLog.LogErrorf("failed to create directory: %v ", err)
		return nil, err
	}
	if opts.RetentionSizePerDay > 0 {
		DEFAULT_RETENTION_DAYS = int(opts.RetentionDays)
	}
	if opts.QueueReadBufferSize > 0 {
		readBufferSize = int(opts.QueueReadBufferSize)
	}
	if opts.QueueWriteBufferSize > 0 {
		writeBufSize = int(opts.QueueWriteBufferSize)
	}
	if opts.PubQueueSize > 0 {
		PubQueue = opts.PubQueueSize
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]map[int]*Topic),
		localCacheTopicMap:   make(map[string]map[int]*Topic),
		exitChan:             make(chan int),
		MetaNotifyChan:       make(chan interface{}, 128),
		OptsNotificationChan: make(chan struct{}, 1),
		ci:                   clusterinfo.New(opts.Logger, http_api.NewClient(nil)),
		dl:                   dirlock.New(dataPath),
		scanTriggerChan:      make(chan *Channel, 100),
		persistNotifyCh:      make(chan struct{}, 2),
		persistClosed:        make(chan struct{}),
	}
	for i := 0; i < len(n.topicJobChList); i++ {
		n.topicJobChList[i] = make(chan func(), jobQueueChLen)
	}
	n.SwapOpts(opts)

	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		nsqLog.LogErrorf("FATAL: --data-path=%s in use (possibly by another instance of nsqd: %v", dataPath, err)
		return nil, err
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		nsqLog.LogErrorf("FATAL: --max-deflate-level must be [1,9]")
		return nil, errors.New("configure invalid")
	}

	if opts.ID < 0 || opts.ID >= MAX_NODE_ID {
		nsqLog.LogErrorf("FATAL: --worker-id must be [0,%d)", MAX_NODE_ID)
		return nil, errors.New("configure invalid")
	}
	nsqLog.Logf("broadcast option: %s, %s", opts.BroadcastAddress, opts.BroadcastInterface)

	n.metaStorage, err = NewShardedDBMetaStorage(path.Join(dataPath, "shared_meta"))
	if err != nil {
		nsqLog.LogErrorf("FATAL: init shared meta storage failed: %v", err.Error())
		return nil, err
	}
	if opts.KVEnabled {
		kvPath := path.Join(dataPath, "shared_kvdata")
		err = os.MkdirAll(kvPath, 0755)
		if err != nil {
			nsqLog.LogErrorf("failed to create directory %s: %s ", kvPath, err)
			return nil, err
		}

		cfg := engine.NewRockConfig()
		// we disable wal here, because we use this for index data, and we will
		// auto recovery it from disk queue data
		cfg.DisableWAL = true
		cfg.UseSharedCache = true
		cfg.UseSharedRateLimiter = true
		if opts.KVMaxWriteBufferNumber > 0 {
			cfg.MaxWriteBufferNumber = int(opts.KVMaxWriteBufferNumber)
		}
		if opts.KVWriteBufferSize > 0 {
			cfg.WriteBufferSize = int(opts.KVWriteBufferSize)
		}
		if opts.KVBlockCache > 0 {
			cfg.BlockCache = opts.KVBlockCache
		}
		sharedCfg, err := engine.NewSharedEngConfig(cfg.RockOptions)
		if err != nil {
			nsqLog.LogErrorf("failed to init engine config %v: %s ", cfg, err)
		}
		n.sharedCfg = sharedCfg
		cfg.SharedConfig = sharedCfg
		cfg.DataDir = kvPath
		engine.FillDefaultOptions(&cfg.RockOptions)
		eng, err := engine.NewKVEng(cfg)
		if err != nil {
			nsqLog.LogErrorf("failed to create engine: %s ", err)
			return nil, err
		}
		err = eng.OpenEng()
		if err != nil {
			nsqLog.LogErrorf("failed to open engine: %s ", err)
			return nil, err
		}
		n.kvTopicStorage = eng
	}

	if opts.StatsdPrefix != "" {
		var port string
		if opts.ReverseProxyPort != "" {
			port = opts.ReverseProxyPort
		} else {
			_, port, err = net.SplitHostPort(opts.HTTPAddress)
			if err != nil {
				nsqLog.LogErrorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
				return nil, err
			}
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
		nsqLog.Infof("using the stats prefix: %v", opts.StatsdPrefix)
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	return n, nil
}

func (n *NSQD) SetReqToEndCB(reqToEndCB ReqToEndFunc) {
	n.Lock()
	n.reqToEndCB = reqToEndCB
	n.Unlock()
}

func (n *NSQD) SetPubLoop(loop func(t *Topic)) {
	n.Lock()
	n.pubLoopFunc = loop
	n.Unlock()
}

func (n *NSQD) GetOpts() *Options {
	if n.opts.Load() == nil {
		return nil
	}
	return n.opts.Load().(*Options)
}

func (n *NSQD) SwapOpts(opts *Options) {
	nsqLog.SetLevel(opts.LogLevel)
	n.opts.Store(opts)
}

func (n *NSQD) TriggerOptsNotification() {
	select {
	case n.OptsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// should be protected by read lock
func (n *NSQD) GetTopicMapRef() map[string]map[int]*Topic {
	return n.topicMap
}

func (n *NSQD) GetLocalCacheTopicMapRef() map[string]map[int]*Topic {
	return n.localCacheTopicMap
}

func (n *NSQD) GetTopicPartitions(topicName string) map[int]*Topic {
	tmpMap := make(map[int]*Topic)
	n.RLock()
	parts, ok := n.topicMap[topicName]
	if ok {
		for p, t := range parts {
			tmpMap[p] = t
		}
	}
	n.RUnlock()
	return tmpMap
}

func (n *NSQD) GetTopicMapCopy() []*Topic {
	n.RLock()
	tmpList := make([]*Topic, 0, len(n.topicMap)*2)
	for _, topics := range n.topicMap {
		for _, t := range topics {
			tmpList = append(tmpList, t)
		}
	}
	n.RUnlock()
	return tmpList
}

func (n *NSQD) Start() {
	n.waitGroup.Wrap(func() { n.queueScanLoop() })
	n.waitGroup.Wrap(func() { n.queueTopicJobLoop() })
	n.persistWaitGroup.Wrap(func() { n.persistLoop() })
}

func (n *NSQD) LoadMetadata(disabled int32) {
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)
	fn := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			nsqLog.LogErrorf("failed to read channel metadata from %s - %s", fn, err)
		}
		return
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return
	}

	topics, err := js.Get("topics").Array()
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}
		if !protocol.IsValidTopicName(topicName) {
			nsqLog.LogWarningf("skipping creation of invalid topic %s", topicName)
			continue
		}
		part, err := topicJs.Get("partition").Int()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}
		ext, err := topicJs.Get("ext").Bool()
		if err != nil {
			nsqLog.Infof("failed to parse topic extend metadata, set to false - %s", err)
			ext = false
		}
		ordered, err := topicJs.Get("ordered").Bool()
		if err != nil {
			ordered = false
		}
		disableChannelAutoCreate, err := topicJs.Get("disbale_channel_auto_create").Bool()
		if err != nil {
			disableChannelAutoCreate = false
		}

		topic := n.internalGetTopic(topicName, part, ext, ordered, disableChannelAutoCreate, disabled)

		if topic == nil {
			nsqLog.LogErrorf("failed to init topic %v - %s", topicName, err)
			continue
		}

		// old meta should also be loaded
		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				nsqLog.LogErrorf("failed to parse metadata - %s", err)
				return
			}
			if !protocol.IsValidChannelName(channelName) {
				nsqLog.LogWarningf("skipping creation of invalid channel %s", channelName)
				continue
			}
			// should not use GetChannel() which will init save meta while init channel
			topic.channelLock.Lock()
			channel, _ := topic.getOrCreateChannel(channelName)
			topic.channelLock.Unlock()

			paused, _ := channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}

			skipped, _ := channelJs.Get("skipped").Bool()
			if skipped {
				channel.Skip()
			}

			zanTestSkipped, _ := channelJs.Get("zanTestSkipped").Bool()
			if !zanTestSkipped {
				channel.UnskipZanTest()
			}

		}
		// we load channels from the new meta file
		topic.LoadChannelMeta()
	}
}

func (n *NSQD) LoadLocalCacheTopic() {
	fn := fmt.Sprintf(path.Join(n.GetOpts().DataPath))
	data, _ := ioutil.ReadDir(fn)
	for _, topicFile := range data {
		topicFilePath := fmt.Sprintf(path.Join(n.GetOpts().DataPath, topicFile.Name()))

		cacheFiles, _ := ioutil.ReadDir(topicFilePath)
		for _, file := range cacheFiles {
			if find := strings.Contains(file.Name(), ".diskqueue"); find {
				str := strings.Split(file.Name(), ".diskqueue")
				topicDes := strings.Split(str[0], "-")
				topicName := topicDes[0]
				partition, _ := strconv.Atoi(topicDes[1])

				topics, ok := n.localCacheTopicMap[topicName]
				if ok {
					_, ok1 := topics[partition]
					if ok1 {
						continue
					}
				} else {
					topics = make(map[int]*Topic)
					n.localCacheTopicMap[topicName] = topics
				}
				topics[partition] = nil
			}
		}
	}
}

func (n *NSQD) persistLoop() {
	for {
		select {
		case <-n.persistClosed:
			tmpMap := n.GetTopicMapCopy()
			n.persistMetadata(tmpMap)
			return
		case <-n.persistNotifyCh:
			tmpMap := n.GetTopicMapCopy()
			n.persistMetadata(tmpMap)
		}
	}
}

func (n *NSQD) NotifyPersistMetadata() {
	select {
	case n.persistNotifyCh <- struct{}{}:
	default:
	}
}

func (n *NSQD) persistMetadata(currentTopics []*Topic) error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
	nsqLog.Logf("NSQ: persisting topic/channel metadata to %s", fileName)
	defer nsqLog.Logf("NSQ: persisted metadata")

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range currentTopics {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.GetTopicName()
		topicData["partition"] = topic.GetTopicPart()
		topicData["ext"] = topic.IsExt()
		topicData["ordered"] = topic.IsOrdered()
		// we save the channels to topic, but for compatible we need save empty channels to json
		channels := []interface{}{}
		topicData["channels"] = channels
		topicData["disbale_channel_auto_create"] = topic.IsChannelAutoCreateDisabled()
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["enabled_delayedqueue"] = atomic.LoadInt32(&EnableDelayedQueue)
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = util.AtomicRename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
	n.Lock()
	if n.exiting {
		n.Unlock()
		return
	}
	n.exiting = true
	n.Unlock()

	close(n.persistClosed)
	n.persistWaitGroup.Wait()
	topics := n.GetTopicMapCopy()
	nsqLog.Logf("NSQ: closing topics")
	for _, topic := range topics {
		topic.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()
	n.metaStorage.Close()
	if n.kvTopicStorage != nil {
		n.kvTopicStorage.CloseAll()
	}
	if n.sharedCfg != nil {
		n.sharedCfg.Destroy()
	}

	n.dl.Unlock()
	nsqLog.Logf("NSQ: exited")
}

func (n *NSQD) GetTopicDefaultPart(topicName string) int {
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		if len(topics) > 0 {
			for _, t := range topics {
				n.RUnlock()
				return t.GetTopicPart()
			}
		}
	}
	n.RUnlock()
	return -1
}

func (n *NSQD) GetTopicIgnPart(topicName string) *Topic {
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		if len(topics) > 0 {
			for _, t := range topics {
				n.RUnlock()
				return t
			}
		}
	}
	n.RUnlock()
	return n.GetTopic(topicName, 0, false)
}

func (n *NSQD) GetTopicWithDisabled(topicName string, part int, ext bool, ordered bool, disableChannelAutoCreate bool) *Topic {
	return n.internalGetTopic(topicName, part, ext, ordered, disableChannelAutoCreate, 1)
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string, part int, ordered bool) *Topic {
	return n.internalGetTopic(topicName, part, false, false, false, 0)
}

func (n *NSQD) GetTopicWithExt(topicName string, part int, ordered bool) *Topic {
	return n.internalGetTopic(topicName, part, true, ordered, false, 0)
}

func (n *NSQD) getKVStorageForTopic(topicName string) engine.KVEngine {
	// check if kv topic is enabled for this topic, and check if a isolated storage is needed for this topic
	// should not be changed after the data is written
	return n.kvTopicStorage
}

func (n *NSQD) internalGetTopic(topicName string, part int, ext bool, ordered bool, disableChannelAutoCreate bool, disabled int32) *Topic {
	if part > MAX_TOPIC_PARTITION || part < 0 {
		return nil
	}
	if topicName == "" {
		nsqLog.Logf("TOPIC name is empty")
		return nil
	}
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		t, ok2 := topics[part]
		if ok2 {
			n.RUnlock()
			return t
		}
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()

	topics, ok = n.topicMap[topicName]
	if ok {
		t, ok := topics[part]
		if ok {
			return t
		}
	} else {
		topics = make(map[int]*Topic)
		n.topicMap[topicName] = topics
	}
	var t *Topic
	t = NewTopicWithExtAndDisableChannelAutoCreate(topicName, part, ext, ordered,
		disableChannelAutoCreate, n.GetOpts(), disabled, n.metaStorage,
		n.getKVStorageForTopic(topicName), n,
		n.pubLoopFunc)
	if t == nil {
		nsqLog.Errorf("TOPIC(%s): create failed", topicName)
	} else {
		topics[part] = t
		nsqLog.Logf("TOPIC(%s): created", t.GetFullName())

	}
	if t != nil {
		// update messagePump state
		t.NotifyReloadChannels()
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string, part int) (*Topic, error) {
	var err error
	var topic *Topic
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		topic, ok = topics[part]
		if !ok {
			err = ErrTopicNotExist
		}
	} else {
		err = ErrTopicNotExist
	}
	n.RUnlock()
	return topic, err
}

func (n *NSQD) deleteTopic(topicName string, part int) {
	n.Lock()
	defer n.Unlock()
	topics, ok := n.topicMap[topicName]
	if !ok {
		return
	}
	delete(topics, part)
	if len(topics) == 0 {
		delete(n.topicMap, topicName)
	}
}

// this just close the topic and remove from map, but keep the data for later.
func (n *NSQD) CloseExistingTopic(topicName string, partition int) error {
	topic, err := n.GetExistingTopic(topicName, partition)
	if err != nil {
		return err
	}
	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Close()

	n.deleteTopic(topicName, partition)
	return nil
}

func (n *NSQD) ForceDeleteTopicData(name string, partition int) error {
	topic, err := n.GetExistingTopic(name, partition)
	if err != nil {
		// not exist, create temp for check
		n.Lock()
		topic = NewTopicForDelete(name, partition, n.GetOpts(), 1, n.metaStorage,
			n,
			n.pubLoopFunc)
		if topic != nil && !topic.IsOrdered() {
			// init delayed so we can remove it later
			topic.GetOrCreateDelayedQueueForReadNoLock()
		}
		n.Unlock()
		if topic == nil {
			return errors.New("failed to init new topic")
		}
	}
	topic.Delete()
	n.deleteTopic(name, partition)
	return nil
}

func (n *NSQD) CheckMagicCode(name string, partition int, code int64, isExt bool, tryFix bool) (string, error) {
	localTopic, err := n.GetExistingTopic(name, partition)
	if err != nil {
		// not exist, create temp for check
		n.Lock()
		localTopic = NewTopicWithExt(name, partition, isExt, false, n.GetOpts(), 1, n.metaStorage, n.getKVStorageForTopic(name), n,
			n.pubLoopFunc)
		n.Unlock()
		if localTopic == nil {
			return "", errors.New("failed to init new topic")
		}
		defer localTopic.Close()
	}
	magicCodeWrong := false
	localMagicCode := localTopic.GetMagicCode()
	if localMagicCode != 0 && localMagicCode != code {
		nsqLog.Infof("local topic %v magic code is not matching with the current:%v-%v", localTopic.GetFullName(), localTopic.GetMagicCode(), code)
		magicCodeWrong = true
	}
	if magicCodeWrong {
		if !tryFix {
			return "", errors.New("magic code is wrong")
		} else {
			nsqLog.Warningf("local topic %v removed for wrong magic code: %v vs %v", localTopic.GetFullName(), localTopic.GetMagicCode(), code)
			n.deleteTopic(localTopic.GetTopicName(), localTopic.GetTopicPart())
			localTopic.Close()
			removedPath, err := localTopic.MarkAsRemoved()
			return removedPath, err
		}
	}
	return "", nil
}

func (n *NSQD) SetTopicMagicCode(t *Topic, code int64) error {
	n.magicCodeMutex.Lock()
	err := t.SetMagicCode(code)
	n.magicCodeMutex.Unlock()

	return err
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string, part int) error {
	topic, err := n.GetExistingTopic(topicName, part)
	if err != nil {
		return err
	}

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.deleteTopic(topicName, part)
	return nil
}

func (n *NSQD) CleanClientPubStats(remote string, protocol string) {
	topics := n.GetTopicMapCopy()
	for _, t := range topics {
		t.detailStats.RemovePubStats(remote, protocol)
	}
}

func (n *NSQD) flushAll(all bool, flushCnt int) {
	match := flushCnt % FLUSH_DISTANCE
	topics := n.GetTopicMapCopy()
	for _, t := range topics {
		if !all && t.IsWriteDisabled() {
			continue
		}
		if !all && (((t.GetTopicPart() + 1) % FLUSH_DISTANCE) != match) {
			continue
		}
		t.ForceFlush()
	}
	if all {
		n.metaStorage.Sync()
		if n.kvTopicStorage != nil {
			n.kvTopicStorage.FlushAll()
		}
	}
}

func (n *NSQD) ReqToEnd(ch *Channel, msg *Message, t time.Duration) error {
	if n.reqToEndCB != nil {
		n.PushTopicJob(ch.GetTopicName(), func() {
			n.reqToEndCB(ch, msg, t)
		})
	}
	return nil
}

func (n *NSQD) NotifyDeleteTopic(t *Topic) {
	n.DeleteExistingTopic(t.GetTopicName(), t.GetTopicPart())
}

func (n *NSQD) NotifyScanChannel(ch *Channel, wait bool) bool {
	if wait {
		select {
		case n.scanTriggerChan <- ch:
		case <-n.exitChan:
		}
		return true
	}
	select {
	case n.scanTriggerChan <- ch:
		return true
	default:
		return false
	}
}

func (n *NSQD) NotifyStateChanged(v interface{}, needPersist bool) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	// we try unblock first to avoid use a new goroutine
	select {
	case n.MetaNotifyChan <- v:
		if !persist || !needPersist {
			return
		}
		n.NotifyPersistMetadata()
		return
	default:
		// full, we try in a goroutine
	}
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.MetaNotifyChan <- v:
			if !persist || !needPersist {
				return
			}
			n.NotifyPersistMetadata()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	topics := n.GetTopicMapCopy()
	for _, t := range topics {
		t.channelLock.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.channelLock.RUnlock()
	}
	return channels
}

type responseData struct {
	isDirty       bool
	needCheckFast bool
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan responseData, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize > n.GetOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.GetOpts().QueueScanWorkerPoolMax
	}
	n.poolSize = n.resizeWorkerPool(idealPoolSize, n.poolSize, closeCh, func(cc chan int) {
		n.queueScanWorker(workCh, responseCh, cc)
	})
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the in-flight queues
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan responseData, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty, checkFast := c.processInFlightQueue(now)
			responseCh <- responseData{isDirty: dirty, needCheckFast: checkFast}
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight
// . It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.GetOpts().QueueScanSelectionCount)
	responseCh := make(chan responseData, n.GetOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.GetOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.GetOpts().QueueScanRefreshInterval)
	flushTicker := time.NewTicker(n.GetOpts().SyncTimeout)

	fastTimer := time.NewTimer(n.GetOpts().QueueScanInterval)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)
	flushCnt := 0
	var fastCh <-chan time.Time
	checkFast := false

	for {
		if checkFast {
			fastTimer.Reset(n.GetOpts().QueueScanInterval/100 + time.Millisecond)
			fastCh = fastTimer.C
			checkFast = false
		} else {
			fastCh = nil
		}
		select {
		case triggedCh := <-n.scanTriggerChan:
			if nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqLog.Logf("QUEUESCAN wakeup by scan trigger: %v", triggedCh.GetName())
			}
			select {
			case workCh <- triggedCh:
			case <-n.exitChan:
				goto exit
			}
			select {
			case <-responseCh:
			case <-n.exitChan:
				goto exit
			}
			continue
		case <-fastCh:
			if len(channels) == 0 {
				continue
			}
			if nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqLog.Logf("QUEUESCAN wakeup fast")
			}
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-flushTicker.C:
			n.flushAll(flushCnt%100 == 0, flushCnt)
			flushCnt++
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.GetOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			select {
			case workCh <- channels[i]:
			case <-n.exitChan:
				goto exit
			}
		}

		numDirty := 0
		numFast := 0
		for i := 0; i < num; i++ {
			select {
			case r := <-responseCh:
				if r.isDirty {
					numDirty++
				}
				if r.needCheckFast {
					numFast++
				}
			case <-n.exitChan:
				goto exit
			}
		}

		if float64(numDirty)/float64(num) > n.GetOpts().QueueScanDirtyPercent {
			goto loop
		}

		if numFast > 0 {
			checkFast = true
		}
	}

exit:
	nsqLog.Logf("QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
	flushTicker.Stop()
	fastTimer.Stop()
}

func (n *NSQD) resizeWorkerPool(idealPoolSize int, actualSize int, closeCh chan int, workerFunc func(chan int)) int {
	if idealPoolSize < 1 {
		idealPoolSize = 1
	}
	for {
		if idealPoolSize == actualSize {
			break
		} else if idealPoolSize < actualSize {
			// contract
			closeCh <- 1
			actualSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				workerFunc(closeCh)
			})
			actualSize++
		}
	}
	return actualSize
}

func (n *NSQD) resizeTopicJobPool(tnum int, jobCh chan func(), closeCh chan int) {
	idealPoolSize := int(float64(tnum) * 0.1)
	if idealPoolSize > n.GetOpts().QueueTopicJobWorkerPoolMax {
		idealPoolSize = n.GetOpts().QueueTopicJobWorkerPoolMax
	}
	n.topicJobPoolSize = n.resizeWorkerPool(idealPoolSize, n.topicJobPoolSize, closeCh, func(cc chan int) {
		n.topicJobLoop(jobCh, cc)
	})
}

func (n *NSQD) PushTopicJob(shardingName string, job func()) {
	h := int(murmur3.Sum32([]byte(shardingName)))
	index := h % len(n.topicJobChList)
	for i := 0; i < len(n.topicJobChList); i++ {
		ch := n.topicJobChList[(index+i)%len(n.topicJobChList)]
		select {
		case ch <- job:
			return
		default:
		}
	}
	nsqLog.LogDebugf("%v topic job push ignored: %T", shardingName, job)
}

func doJob(job func()) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			nsqLog.Warningf("topic job panic %s:%v", buf, e)
		}
	}()
	job()
}

func (n *NSQD) topicJobLoop(jobCh chan func(), closeCh chan int) {
	for {
		select {
		case job := <-jobCh:
			doJob(job)
		case <-closeCh:
			return
		}
	}
}

func (n *NSQD) queueTopicJobLoop() {
	closeCh := make(chan int)
	topics := n.GetTopicMapCopy()
	refreshTicker := time.NewTicker(n.GetOpts().QueueScanRefreshInterval)
	aggJobCh := make(chan func(), len(n.topicJobChList)*jobQueueChLen+1)
	for i := 0; i < len(n.topicJobChList); i++ {
		go func(c chan func()) {
			for {
				select {
				case job := <-c:
					aggJobCh <- job
				case <-n.exitChan:
					return
				}
			}
		}(n.topicJobChList[i])
	}
	n.resizeTopicJobPool(len(topics), aggJobCh, closeCh)
	for {
		select {
		case <-refreshTicker.C:
			topics := n.GetTopicMapCopy()
			n.resizeTopicJobPool(len(topics), aggJobCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	nsqLog.Logf("QUEUE topic job loop: closing")
	close(closeCh)
	refreshTicker.Stop()
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.GetOpts().AuthHTTPAddresses) != 0
}
