package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/quantile"
	"github.com/youzan/nsq/internal/util"
	"github.com/youzan/nsq/nsqd/engine"
)

const (
	MAX_TOPIC_PARTITION    = 1023
	HISTORY_STAT_FILE_NAME = ".stat.history.dat"
	slowCost               = time.Millisecond * 50
)

var (
	ErrInvalidMessageID           = errors.New("message id is invalid")
	ErrWriteOffsetMismatch        = errors.New("write offset mismatch")
	ErrOperationInvalidState      = errors.New("the operation is not allowed under current state")
	ErrMessageInvalidDelayedState = errors.New("the message is invalid for delayed")
	PubQueue                      = 500
	errChannelNotExist            = errors.New("channel does not exist")
)

func writeMessageToBackend(writeExt bool, buf *bytes.Buffer, msg *Message, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	_, err := msg.WriteTo(buf, writeExt)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	return bq.PutV2(buf.Bytes())
}

func writeMessageToBackendWithCheck(writeExt bool, buf *bytes.Buffer, msg *Message, checkSize int64, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	wsize, err := msg.WriteTo(buf, writeExt)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	// there are 4bytes data length on disk.
	if checkSize > 0 && wsize+4 != checkSize {
		return 0, 0, diskQueueEndInfo{}, fmt.Errorf("message write size mismatch %v vs %v", checkSize, wsize+4)
	}
	return bq.PutV2(buf.Bytes())
}

type MsgIDGenerator interface {
	NextID() uint64
	Reset(uint64)
}

type TopicDynamicConf struct {
	AutoCommit               int32
	RetentionDay             int32
	SyncEvery                int64
	OrderedMulti             bool
	MultiPart                bool
	DisableChannelAutoCreate bool
	Ext                      bool
}

type PubInfo struct {
	Done       chan struct{}
	MsgBody    []byte
	ExtContent ext.IExtContent
	StartPub   time.Time
	Err        error
}

type PubInfoChan chan *PubInfo

type MPubInfo struct {
	Done     chan struct{}
	Msgs     []*Message
	StartPub time.Time
	Err      error
}
type MPubInfoChan chan *MPubInfo

type ChannelMetaInfo struct {
	Name           string `json:"name"`
	Paused         bool   `json:"paused"`
	Skipped        bool   `json:"skipped"`
	ZanTestSkipped bool   `json:"zanTestSkipped"`
}

func (cm *ChannelMetaInfo) IsZanTestSkipepd() bool {
	return cm.ZanTestSkipped
}

type Topic struct {
	sync.Mutex

	tname       string
	fullName    string
	partition   int
	channelMap  map[string]*Channel
	channelLock sync.RWMutex
	backend     *diskQueueWriter
	dataPath    string
	flushChan   chan int
	exitFlag    int32

	ephemeral bool
	deleter   sync.Once

	nsqdNotify      INsqdNotify
	option          *Options
	msgIDCursor     MsgIDGenerator
	defaultIDSeq    uint64
	needFlush       int32
	EnableTrace     int32
	lastSyncCnt     int64
	putBuffer       bytes.Buffer
	bp              sync.Pool
	writeDisabled   int32
	dynamicConf     *TopicDynamicConf
	isOrdered       int32
	magicCode       int64
	committedOffset atomic.Value
	detailStats     *DetailStatsInfo
	needFixData     int32
	pubWaitingChan  PubInfoChan
	mpubWaitingChan MPubInfoChan
	quitChan        chan struct{}
	pubLoopFunc     func(v *Topic)
	wg              sync.WaitGroup

	delayedQueue                 atomic.Value
	isExt                        int32
	isChannelAutoCreatedDisabled int32
	pubFailedCnt                 int64
	metaStorage                  IMetaStorage
	// the pub data waiting pub ok returned
	pubWaitingBytes int64
	tpLog           *levellogger.LevelLogger
	kvTopic         *KVTopic
}

func (t *Topic) setExt() {
	atomic.StoreInt32(&t.isExt, 1)
}

func (t *Topic) IsExt() bool {
	return atomic.LoadInt32(&t.isExt) == 1
}

func (t *Topic) DisableChannelAutoCreate() {
	atomic.StoreInt32(&t.isChannelAutoCreatedDisabled, 1)
}

func (t *Topic) EnableChannelAutoCreate() {
	atomic.StoreInt32(&t.isChannelAutoCreatedDisabled, 0)
}

func (t *Topic) IsChannelAutoCreateDisabled() bool {
	return atomic.LoadInt32(&t.isChannelAutoCreatedDisabled) == 1
}

func (t *Topic) IncrPubWaitingBytes(sz int64) int64 {
	return atomic.AddInt64(&t.pubWaitingBytes, sz)
}

func (t *Topic) IncrPubFailed() {
	atomic.AddInt64(&t.pubFailedCnt, 1)
	TopicPubFailedCnt.With(prometheus.Labels{
		"topic":     t.GetTopicName(),
		"partition": strconv.Itoa(t.GetTopicPart()),
	}).Inc()
}

func (t *Topic) PubFailed() int64 {
	return atomic.LoadInt64(&t.pubFailedCnt)
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

func NewTopicForDelete(topicName string, part int, opt *Options,
	writeDisabled int32, metaStorage IMetaStorage,
	notify INsqdNotify, loopFunc func(v *Topic)) *Topic {
	return NewTopicWithExt(topicName, part, false, false, opt, writeDisabled, metaStorage, nil, notify, loopFunc)
}

func NewTopicWithExt(topicName string, part int, ext bool, ordered bool, opt *Options,
	writeDisabled int32, metaStorage IMetaStorage,
	kvEng engine.KVEngine,
	notify INsqdNotify, loopFunc func(v *Topic)) *Topic {
	return NewTopicWithExtAndDisableChannelAutoCreate(topicName, part, ext, ordered, false, opt, writeDisabled, metaStorage, kvEng, notify, loopFunc)
}

// Topic constructor
func NewTopicWithExtAndDisableChannelAutoCreate(topicName string, part int, ext bool, ordered bool,
	disbaleChannelAutoCreate bool, opt *Options,
	writeDisabled int32, metaStorage IMetaStorage,
	kvEng engine.KVEngine,
	notify INsqdNotify, loopFunc func(v *Topic)) *Topic {
	if part > MAX_TOPIC_PARTITION {
		return nil
	}
	t := &Topic{
		tname:           topicName,
		partition:       part,
		channelMap:      make(map[string]*Channel),
		flushChan:       make(chan int, 10),
		option:          opt,
		dynamicConf:     &TopicDynamicConf{SyncEvery: opt.SyncEvery, AutoCommit: 1},
		putBuffer:       bytes.Buffer{},
		nsqdNotify:      notify,
		writeDisabled:   writeDisabled,
		pubWaitingChan:  make(PubInfoChan, PubQueue),
		mpubWaitingChan: make(MPubInfoChan, PubQueue/2),
		quitChan:        make(chan struct{}),
		pubLoopFunc:     loopFunc,
		metaStorage:     metaStorage,
		kvTopic:         NewKVTopicWithEngine(topicName, part, ext, kvEng),
	}
	t.fullName = GetTopicFullName(t.tname, t.partition)
	t.tpLog = nsqLog.WrappedWithPrefix("["+t.fullName+"]", 0)

	if metaStorage == nil {
		t.metaStorage = &fileMetaStorage{}
	}
	if ext {
		t.setExt()
	}
	if ordered {
		atomic.StoreInt32(&t.isOrdered, 1)
	}
	if t.dynamicConf.SyncEvery < 1 {
		t.dynamicConf.SyncEvery = 1
	}
	if disbaleChannelAutoCreate {
		t.DisableChannelAutoCreate()
	}
	t.bp.New = func() interface{} {
		return &bytes.Buffer{}
	}

	t.dataPath = path.Join(opt.DataPath, topicName)
	err := os.MkdirAll(t.dataPath, 0755)
	if err != nil {
		t.tpLog.LogErrorf("failed to create directory: %v ", err)
		return nil
	}

	backendName := getBackendName(t.tname, t.partition)
	queue, err := newDiskQueueWriterWithMetaStorage(backendName,
		t.dataPath,
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength,
		opt.SyncEvery,
		t.metaStorage,
	)

	if err != nil {
		t.tpLog.LogErrorf("failed to init disk queue: %v ", err)
		if err == ErrNeedFixQueueStart || err == ErrNeedFixQueueEnd {
			t.SetDataFixState(true)
		} else {
			return nil
		}
	}
	t.backend = queue.(*diskQueueWriter)

	if err != ErrNeedFixQueueEnd {
		t.UpdateCommittedOffset(t.backend.GetQueueWriteEnd())
	}
	if _, err := t.tryFixKVTopic(); err != nil {
		t.tpLog.LogErrorf("failed to auto fix kv topic while init: %s", err)
		t.SetDataFixState(true)
	}
	err = t.loadMagicCode()
	if err != nil {
		t.tpLog.LogErrorf("failed to load magic code: %v", err)
		return nil
	}
	t.detailStats = NewDetailStatsInfo(t.tname, strconv.Itoa(t.partition), t.TotalDataSize(), t.getHistoryStatsFileName())
	t.nsqdNotify.NotifyStateChanged(t, true)
	t.tpLog.LogDebugf("new topic created: %v", t.tname)

	if t.pubLoopFunc != nil {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.pubLoopFunc(t)
		}()
	}
	t.LoadChannelMeta()
	return t
}

func (t *Topic) GetDelayedQueue() *DelayQueue {
	if t.IsOrdered() {
		return nil
	}

	if t.delayedQueue.Load() == nil {
		return nil
	}
	return t.delayedQueue.Load().(*DelayQueue)
}

func (t *Topic) GetOrCreateDelayedQueueForReadNoLock() (*DelayQueue, error) {
	if t.delayedQueue.Load() == nil {
		delayedQueue, err := NewDelayQueueForRead(t.tname, t.partition, t.dataPath, t.option, nil, t.IsExt())
		if err == nil {
			t.delayedQueue.Store(delayedQueue)
		} else {
			t.tpLog.LogWarningf("init delayed queue error %v", err)
			return nil, err
		}
	}
	return t.delayedQueue.Load().(*DelayQueue), nil
}

func (t *Topic) GetOrCreateDelayedQueueNoLock(idGen MsgIDGenerator) (*DelayQueue, error) {
	if t.delayedQueue.Load() == nil {
		delayedQueue, err := NewDelayQueue(t.tname, t.partition, t.dataPath, t.option, idGen, t.IsExt())
		if err == nil {
			t.delayedQueue.Store(delayedQueue)
			t.channelLock.RLock()
			for _, ch := range t.channelMap {
				ch.SetDelayedQueue(delayedQueue)
			}
			t.channelLock.RUnlock()
		} else {
			t.tpLog.LogWarningf("init delayed queue error %v", err)
			return nil, err
		}
	}
	return t.delayedQueue.Load().(*DelayQueue), nil
}

func (t *Topic) GetWaitChan() PubInfoChan {
	return t.pubWaitingChan
}
func (t *Topic) IsWaitChanFull() bool {
	return len(t.pubWaitingChan) >= cap(t.pubWaitingChan)-1
}
func (t *Topic) IsMWaitChanFull() bool {
	return len(t.mpubWaitingChan) >= cap(t.mpubWaitingChan)-1
}
func (t *Topic) GetMWaitChan() MPubInfoChan {
	return t.mpubWaitingChan
}
func (t *Topic) QuitChan() <-chan struct{} {
	return t.quitChan
}

func (t *Topic) IsDataNeedFix() bool {
	return atomic.LoadInt32(&t.needFixData) == 1
}

func (t *Topic) SetDataFixState(needFix bool) {
	if needFix {
		atomic.StoreInt32(&t.needFixData, 1)
	} else {
		atomic.StoreInt32(&t.needFixData, 0)
	}
}

func (t *Topic) getMagicCodeFileName() string {
	return path.Join(t.dataPath, "magic"+strconv.Itoa(t.partition))
}

func (t *Topic) saveMagicCode() error {
	var f *os.File
	var err error

	fileName := t.getMagicCodeFileName()
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%d\n",
		atomic.LoadInt64(&t.magicCode))
	if err != nil {
		return err
	}
	f.Sync()
	t.tpLog.Infof("saved as magic code: %v", atomic.LoadInt64(&t.magicCode))
	return nil
}

func (t *Topic) removeMagicCode() {
	fileName := t.getMagicCodeFileName()
	err := os.Remove(fileName)
	if err != nil {
		t.tpLog.Infof("remove the magic file %v failed:%v", fileName, err)
	}
}

func (t *Topic) loadMagicCode() error {
	var f *os.File
	var err error

	fileName := t.getMagicCodeFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var code int64
	_, err = fmt.Fscanf(f, "%d\n",
		&code)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&t.magicCode, code)
	t.tpLog.Infof("loading topic as magic code: %v", code)
	return nil
}

func (t *Topic) removeHistoryStat() {
	fileName := t.getHistoryStatsFileName()
	err := os.Remove(fileName)
	if err != nil {
		t.tpLog.Infof("remove file %v failed:%v", fileName, err)
	}
}

func (t *Topic) MarkAsRemoved() (string, error) {
	t.Lock()
	defer t.Unlock()
	atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1)
	t.tpLog.Logf("TOPIC(%s): deleting", t.GetFullName())
	// since we are explicitly deleting a topic (not just at system exit time)
	// de-register this from the lookupd
	t.nsqdNotify.NotifyStateChanged(t, true)

	t.channelLock.Lock()
	for _, channel := range t.channelMap {
		t.channelMap[channel.name] = nil
		delete(t.channelMap, channel.name)
		channel.Delete()
	}
	t.channelLock.Unlock()
	// we should move our partition only
	renamePath := t.dataPath + "-removed-" + strconv.Itoa(int(time.Now().Unix()))
	t.tpLog.Warningf("mark the topic as removed: %v", renamePath)
	os.MkdirAll(renamePath, 0755)
	err := t.backend.RemoveTo(renamePath)
	if err != nil {
		t.tpLog.Errorf("failed to mark the topic as removed %v failed: %v", renamePath, err)
	}
	util.AtomicRename(t.getMagicCodeFileName(), path.Join(renamePath, "magic"+strconv.Itoa(t.partition)))
	t.removeHistoryStat()
	t.RemoveChannelMeta()
	t.removeMagicCode()
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().Delete()
	}
	return renamePath, err
}

// should be protected by the topic lock for all partitions
func (t *Topic) SetMagicCode(code int64) error {
	if t.magicCode == code {
		return nil
	} else if t.magicCode > 0 {
		t.tpLog.Errorf("magic code set more than once :%v, %v", t.magicCode, code)
		return errors.New("magic code can not be changed.")
	}
	t.magicCode = code
	return t.saveMagicCode()
}

// should be protected by the topic lock for all partitions
func (t *Topic) GetMagicCode() int64 {
	return t.magicCode
}

func (t *Topic) SetTrace(enable bool) {
	if enable {
		atomic.StoreInt32(&t.EnableTrace, 1)
	} else {
		atomic.StoreInt32(&t.EnableTrace, 0)
	}
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().SetTrace(enable)
	}
}

func (t *Topic) getChannelMetaFileName() string {
	return path.Join(t.dataPath, "channel_meta"+strconv.Itoa(t.partition))
}

func (t *Topic) LoadChannelMeta() error {
	fn := t.getChannelMetaFileName()
	channels, err := t.metaStorage.LoadChannelMeta(fn)
	if err != nil {
		if IsMetaNotFound(err) {
			return nil
		}
		t.tpLog.LogWarningf("failed to load metadata - %s", err)
		return err
	}

	for _, ch := range channels {
		channelName := ch.Name
		if !protocol.IsValidChannelName(channelName) {
			t.tpLog.LogWarningf("skipping creation of invalid channel %s", channelName)
			continue
		}
		// should not use GetChannel() which will init save meta while init channel
		t.channelLock.Lock()
		channel, _ := t.getOrCreateChannel(channelName)
		t.channelLock.Unlock()

		if ch.Paused {
			channel.Pause()
		}

		if ch.Skipped {
			channel.Skip()
		}
		//unskip zan test message according to meta file
		if !ch.IsZanTestSkipepd() {
			channel.UnskipZanTest()
		}
	}
	return nil
}

func (t *Topic) UpdateChannelMeta(ch *Channel, paused int, skipped int, zanTestSkipped int) error {
	switch paused {
	case 1:
		ch.Pause()
	case 0:
		ch.UnPause()
	}

	switch skipped {
	case 1:
		ch.Skip()
	case 0:
		ch.UnSkip()
	}

	switch int32(zanTestSkipped) {
	case ZanTestSkip:
		ch.SkipZanTest()
	case ZanTestUnskip:
		ch.UnskipZanTest()
	}
	return t.SaveChannelMeta()
}

func (t *Topic) GetChannelMeta() []ChannelMetaInfo {
	t.channelLock.RLock()
	channels := make([]ChannelMetaInfo, 0, len(t.channelMap))
	for _, channel := range t.channelMap {
		channel.RLock()
		if !channel.ephemeral {
			meta := ChannelMetaInfo{
				Name:           channel.name,
				Paused:         channel.IsPaused(),
				Skipped:        channel.IsSkipped(),
				ZanTestSkipped: channel.IsZanTestSkipped(),
			}
			channels = append(channels, meta)
		}
		channel.RUnlock()
	}
	t.channelLock.RUnlock()
	return channels
}

func (t *Topic) SaveChannelMeta() error {
	channels := make([]*ChannelMetaInfo, 0)
	t.channelLock.RLock()
	for _, channel := range t.channelMap {
		channel.RLock()
		if !channel.ephemeral {
			meta := &ChannelMetaInfo{
				Name:           channel.name,
				Paused:         channel.IsPaused(),
				Skipped:        channel.IsSkipped(),
				ZanTestSkipped: channel.IsZanTestSkipped(),
			}
			channels = append(channels, meta)
		}
		channel.RUnlock()
	}
	t.channelLock.RUnlock()
	fileName := t.getChannelMetaFileName()
	err := t.metaStorage.SaveChannelMeta(fileName, t.option.UseFsync, channels)
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) RemoveChannelMeta() {
	fileName := t.getChannelMetaFileName()
	t.metaStorage.RemoveChannelMeta(fileName)
}

func (t *Topic) getHistoryStatsFileName() string {
	return path.Join(t.dataPath, t.fullName+HISTORY_STAT_FILE_NAME)
}

func (t *Topic) GetDetailStats() *DetailStatsInfo {
	return t.detailStats
}

func (t *Topic) SaveHistoryStats() error {
	if t.Exiting() {
		return ErrExiting
	}
	t.Lock()
	defer t.Unlock()
	return t.detailStats.SaveHistory(t.getHistoryStatsFileName())
}

func (t *Topic) LoadHistoryStats() error {
	return t.detailStats.LoadHistory(t.getHistoryStatsFileName())
}

func (t *Topic) GetCommitted() BackendQueueEnd {
	l := t.committedOffset.Load()
	if l == nil {
		return nil
	}
	return l.(BackendQueueEnd)
}

// note: multiple writer should be protected by lock
func (t *Topic) UpdateCommittedOffset(offset BackendQueueEnd) {
	if offset == nil {
		return
	}
	cur := t.GetCommitted()
	if cur != nil && offset.Offset() < cur.Offset() {
		t.tpLog.LogDebugf("committed is rollbacked: %v, %v", cur, offset)
	}
	t.committedOffset.Store(offset)
	syncEvery := atomic.LoadInt64(&t.dynamicConf.SyncEvery)
	if syncEvery == 1 ||
		offset.TotalMsgCnt()-atomic.LoadInt64(&t.lastSyncCnt) >= syncEvery {
		if !t.IsWriteDisabled() {
			t.flushBuffer(true)
		}
	} else {
		t.notifyChEndChanged(false)
	}
}

func (t *Topic) GetDiskQueueSnapshot(checkCommit bool) *DiskQueueSnapshot {
	e := t.backend.GetQueueReadEnd()
	if checkCommit {
		commit := t.GetCommitted()
		if commit != nil && e.Offset() > commit.Offset() {
			e = commit
		}
	}
	start := t.backend.GetQueueReadStart()
	d := NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, e)
	d.SetQueueStart(start)
	return d
}

func (t *Topic) BufferPoolGet(capacity int) *bytes.Buffer {
	b := t.bp.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(capacity)
	return b
}

func (t *Topic) BufferPoolPut(b *bytes.Buffer) {
	t.bp.Put(b)
}

func (t *Topic) GetChannelMapCopy() map[string]*Channel {
	tmpMap := make(map[string]*Channel)
	t.channelLock.RLock()
	for k, v := range t.channelMap {
		tmpMap[k] = v
	}
	t.channelLock.RUnlock()
	return tmpMap
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) GetDynamicInfo() TopicDynamicConf {
	t.Lock()
	info := *t.dynamicConf
	t.Unlock()
	return info
}

func (t *Topic) IsOrdered() bool {
	return atomic.LoadInt32(&t.isOrdered) == 1
}

func (t *Topic) SetDynamicInfo(dynamicConf TopicDynamicConf, idGen MsgIDGenerator) {
	t.Lock()
	if idGen != nil {
		t.msgIDCursor = idGen
	}
	atomic.StoreInt64(&t.dynamicConf.SyncEvery, dynamicConf.SyncEvery)
	atomic.StoreInt32(&t.dynamicConf.AutoCommit, dynamicConf.AutoCommit)
	atomic.StoreInt32(&t.dynamicConf.RetentionDay, dynamicConf.RetentionDay)
	t.dynamicConf.OrderedMulti = dynamicConf.OrderedMulti
	if dynamicConf.OrderedMulti {
		atomic.StoreInt32(&t.isOrdered, 1)
	} else {
		atomic.StoreInt32(&t.isOrdered, 0)
	}
	t.dynamicConf.MultiPart = dynamicConf.MultiPart

	dq := t.GetDelayedQueue()
	if dq != nil {
		atomic.StoreInt64(&dq.SyncEvery, dynamicConf.SyncEvery)
		if dynamicConf.Ext {
			dq.setExt()
		}
	}
	t.dynamicConf.Ext = dynamicConf.Ext
	if dynamicConf.Ext {
		t.setExt()
		if t.kvTopic != nil {
			t.kvTopic.setExt()
		}
	}
	t.dynamicConf.DisableChannelAutoCreate = dynamicConf.DisableChannelAutoCreate
	channelAutoCreateDisabled := t.IsChannelAutoCreateDisabled()
	if dynamicConf.DisableChannelAutoCreate && !channelAutoCreateDisabled {
		t.DisableChannelAutoCreate()
	} else if !dynamicConf.DisableChannelAutoCreate && channelAutoCreateDisabled {
		t.EnableChannelAutoCreate()
	}
	t.tpLog.Logf("topic dynamic configure changed to %v", dynamicConf)
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		ext := dynamicConf.Ext
		ch.SetExt(ext)
	}
	t.channelLock.RUnlock()
	t.Unlock()
	t.nsqdNotify.NotifyStateChanged(t, true)
}

func (t *Topic) nextMsgID() MessageID {
	id := uint64(0)
	if t.msgIDCursor != nil {
		id = t.msgIDCursor.NextID()
	} else {
		id = atomic.AddUint64(&t.defaultIDSeq, 1)
	}
	return MessageID(id)
}

func (t *Topic) GetFullName() string {
	return t.fullName
}

func (t *Topic) GetTopicName() string {
	return t.tname
}

func (t *Topic) GetTopicPart() int {
	return t.partition
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.channelLock.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.channelLock.Unlock()

	if isNew {
		if !channel.IsEphemeral() {
			t.SaveChannelMeta()
		}
		// update messagePump state
		t.NotifyReloadChannels()
	}

	return channel
}

func (t *Topic) NotifyReloadChannels() {
}

func (t *Topic) GetTopicChannelDebugStat(channelName string) string {
	statStr := ""
	t.channelLock.RLock()
	for n, channel := range t.channelMap {
		if channelName == "" || channelName == n {
			statStr += channel.GetChannelDebugStats()
		}
	}
	t.channelLock.RUnlock()
	dq := t.GetDelayedQueue()
	if dq != nil {
		t.tpLog.Logf("delayed queue stats: %v", dq.Stats())
	}
	return statStr
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		readEnd := t.backend.GetQueueReadEnd()
		curCommit := t.GetCommitted()
		if curCommit != nil && readEnd.Offset() > curCommit.Offset() {
			if t.tpLog.Level() >= levellogger.LOG_DEBUG {
				t.tpLog.Logf("channel %v, end to commit: %v, read end: %v", channelName, curCommit, readEnd)
			}
			readEnd = curCommit
		}

		var ext int32
		if t.IsExt() {
			ext = 1
		} else {
			ext = 0
		}
		start := t.backend.GetQueueReadStart()
		channel = NewChannel(t.GetTopicName(), t.GetTopicPart(), t.IsOrdered(), channelName, readEnd,
			t.option, deleteCallback, t.flushForChannelMoreData, atomic.LoadInt32(&t.writeDisabled),
			t.nsqdNotify, ext, start, t.metaStorage, t.kvTopic, !t.IsDataNeedFix())

		channel.UpdateQueueEnd(readEnd, false)
		channel.SetDelayedQueue(t.GetDelayedQueue())
		if t.IsWriteDisabled() {
			channel.DisableConsume(true)
		}
		t.channelMap[channelName] = channel
		t.tpLog.Logf("TOPIC new channel(%s), end: %v",
			channel.name, channel.GetChannelEnd())
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.channelLock.RLock()
	channel, ok := t.channelMap[channelName]
	t.channelLock.RUnlock()
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

func (t *Topic) CloseExistingChannel(channelName string, deleteData bool) error {
	t.channelLock.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		numChannels := len(t.channelMap)
		t.channelLock.Unlock()
		if numChannels == 0 && t.ephemeral == true {
			go t.deleter.Do(func() { t.nsqdNotify.NotifyDeleteTopic(t) })
		}
		return errChannelNotExist
	}
	t.channelMap[channelName] = nil
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.channelLock.Unlock()

	t.tpLog.Logf("TOPIC deleting channel %s", channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	if deleteData {
		channel.Delete()
	} else {
		channel.Close()
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.nsqdNotify.NotifyDeleteTopic(t) })
	}

	return nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	err := t.CloseExistingChannel(channelName, true)
	if err == errChannelNotExist {
		return nil
	}
	if err == nil {
		return t.SaveChannelMeta()
	}
	return err
}

func (t *Topic) RollbackNoLock(vend BackendOffset, diffCnt uint64) error {
	old := t.backend.GetQueueWriteEnd()
	t.tpLog.Logf("reset the backend from %v to : %v, %v", old, vend, diffCnt)
	dend, err := t.backend.RollbackWriteV2(vend, diffCnt)
	if err == nil {
		if t.kvTopic != nil {
			t.kvTopic.ResetBackendEnd(vend, dend.TotalMsgCnt())
		}
		t.UpdateCommittedOffset(&dend)
		t.updateChannelsEnd(true, true)
	}
	return err
}

func (t *Topic) ResetBackendEndNoLock(vend BackendOffset, totalCnt int64) error {
	old := t.backend.GetQueueWriteEnd()
	if old.Offset() == vend && old.TotalMsgCnt() == totalCnt {
		return nil
	}
	t.tpLog.Logf("topic reset the backend from %v to : %v, %v", old, vend, totalCnt)
	dend, err := t.backend.ResetWriteEndV2(vend, totalCnt)
	if err != nil {
		t.tpLog.LogErrorf("reset backend to %v error: %v", vend, err)
	} else {
		if t.kvTopic != nil {
			t.kvTopic.ResetBackendEnd(vend, totalCnt)
		}
		t.UpdateCommittedOffset(&dend)
		t.updateChannelsEnd(true, true)
	}

	return err
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if m.ID > 0 {
		t.tpLog.Logf("should not pass id in message while pub: %v", m.ID)
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	t.Lock()
	defer t.Unlock()
	if m.DelayedType >= MinDelayedType {
		return 0, 0, 0, nil, ErrMessageInvalidDelayedState
	}

	id, offset, writeBytes, dend, err := t.PutMessageNoLock(m)
	return id, offset, writeBytes, dend, err
}

func (t *Topic) PutMessageNoLock(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, nil, errors.New("exiting")
	}
	if m.ID > 0 {
		t.tpLog.Logf("should not pass id in message while pub: %v", m.ID)
		return 0, 0, 0, nil, ErrInvalidMessageID
	}

	id, offset, writeBytes, dend, err := t.put(m, true, 0)
	return id, offset, writeBytes, &dend, err
}

func (t *Topic) flushForChannelMoreData(c *Channel) {
	if c.IsSkipped() || c.IsPaused() {
		return
	}
	hasData := t.backend.FlushBuffer()
	if hasData {
		e := t.getCommittedEnd()
		updateChannelEnd(false, e, c)
	}
}

func (t *Topic) ForceFlushForChannels(wait bool) {
	// flush buffer only to allow the channel read recent write
	// no need sync to disk, since sync is heavy IO.
	if wait {
		hasData := t.backend.FlushBuffer()
		if hasData {
			t.updateChannelsEnd(false, false)
		}
	} else {
		t.notifyChEndChanged(false)
	}
}

func (t *Topic) notifyChEndChanged(force bool) {
	t.nsqdNotify.PushTopicJob(t.GetTopicName(), func() { t.flushForChannels(force) })
}

func (t *Topic) flushForChannels(forceUpdate bool) {
	if t.IsWriteDisabled() {
		return
	}
	needFlush := false
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		if ch.IsWaitingMoreData() {
			needFlush = true
			break
		}
	}
	t.channelLock.RUnlock()
	hasData := false
	if needFlush {
		hasData = t.backend.FlushBuffer()
	}
	if hasData || forceUpdate {
		// any channel which trigged the flush need force update the end for all channels, or it may miss
		// the end update event since no more data to be flush until next message come.
		t.updateChannelsEnd(false, true)
	}
}

func (t *Topic) PutRawDataOnReplica(rawData []byte, offset BackendOffset, checkSize int64, msgNum int32) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := t.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	_, writeBytes, dend, err := t.backend.PutRawV2(rawData, msgNum)
	if err != nil {
		t.tpLog.LogErrorf("topic write to disk error: %v, %v", offset, err.Error())
		return &dend, err
	}
	if checkSize > 0 && int64(writeBytes) != checkSize {
		t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
		return &dend, fmt.Errorf("message write size mismatch %v vs %v", checkSize, writeBytes)
	}
	atomic.StoreInt32(&t.needFlush, 1)
	if t.kvTopic != nil {
		kvEnd, kverr := t.kvTopic.PutRawDataOnReplica(rawData, offset, checkSize, msgNum)
		if kverr != nil {
			t.tpLog.LogWarningf("kv topic write failed: %s, %v", kverr, rawData)
			t.SetDataFixState(true)
			return &dend, kverr
		} else if kvEnd.Offset() != dend.Offset() || kvEnd.TotalMsgCnt() != dend.TotalMsgCnt() {
			t.tpLog.LogWarningf("kv topic write end mismatch: %v, %v", kvEnd, dend)
			t.SetDataFixState(true)
		}
	}
	if atomic.LoadInt32(&t.dynamicConf.AutoCommit) == 1 {
		t.UpdateCommittedOffset(&dend)
	}

	return &dend, nil
}

func (t *Topic) PutMessageOnReplica(m *Message, offset BackendOffset, checkSize int64) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := t.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	_, _, _, dend, err := t.put(m, false, checkSize)
	if err != nil {
		return nil, err
	}
	return &dend, nil
}

func (t *Topic) PutMessagesOnReplica(msgs []*Message, offset BackendOffset, checkSize int64) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		t.tpLog.LogErrorf(
			"TOPIC write message offset mismatch %v, %v",
			offset, wend)
		return nil, ErrWriteOffsetMismatch
	}

	var dend diskQueueEndInfo
	var err error
	wsize := int32(0)
	wsizeTotal := int32(0)
	for _, m := range msgs {
		_, _, wsize, dend, err = t.put(m, false, 0)
		if err != nil {
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return nil, err
		}
		wsizeTotal += wsize
	}
	if checkSize > 0 && int64(wsizeTotal) != checkSize {
		t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
		return nil, fmt.Errorf("batch message size mismatch: %v vs %v", checkSize, wsizeTotal)
	}

	return &dend, nil
}

func (t *Topic) PutMessagesNoLock(msgs []*Message) (MessageID, BackendOffset, int32, int64, BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, 0, nil, ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	firstMsgID := MessageID(0)
	firstOffset := BackendOffset(-1)
	firstCnt := int64(0)
	var diskEnd diskQueueEndInfo
	batchBytes := int32(0)
	for _, m := range msgs {
		if m.ID > 0 {
			t.tpLog.Logf("should not pass id in message while pub: %v", m.ID)
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return 0, 0, 0, 0, nil, ErrInvalidMessageID
		}
		id, offset, bytes, end, err := t.put(m, true, 0)
		if err != nil {
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, err
		}
		diskEnd = end
		batchBytes += bytes
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			firstCnt = diskEnd.TotalMsgCnt()
		}
	}
	return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) (MessageID, BackendOffset, int32, int64, BackendQueueEnd, error) {
	s := time.Now()
	t.Lock()
	defer t.Unlock()
	firstMsgID, firstOffset, batchBytes, totalCnt, dend, err := t.PutMessagesNoLock(msgs)
	cost := time.Since(s)
	if cost >= slowCost {
		t.tpLog.Infof("topic put batch local cost: %v", cost)
	}
	return firstMsgID, firstOffset, batchBytes, totalCnt, dend, err
}

func (t *Topic) put(m *Message, trace bool, checkSize int64) (MessageID, BackendOffset, int32, diskQueueEndInfo, error) {
	if m.ID <= 0 {
		m.ID = t.nextMsgID()
	}
	offset, writeBytes, dend, err := writeMessageToBackendWithCheck(t.IsExt(), &t.putBuffer, m, checkSize, t.backend)
	atomic.StoreInt32(&t.needFlush, 1)
	if err != nil {
		t.tpLog.LogErrorf(
			"TOPIC failed to write message to backend - %s",
			err)
		return m.ID, offset, writeBytes, dend, err
	}

	if t.kvTopic != nil {
		kvOffset, kvSize, kvEnd, kverr := t.kvTopic.put(m, checkSize)
		if kverr != nil {
			t.tpLog.LogWarningf("kv topic write failed: %s", kverr)
		} else if kvEnd != dend.TotalMsgCnt() || kvOffset != dend.Offset() || kvSize != writeBytes {
			t.tpLog.LogWarningf("kv topic write end mismatch: %v-%v vs %v, %v vs %v", kvEnd, kvOffset, dend, kvSize, writeBytes)
		}
	}
	if atomic.LoadInt32(&t.dynamicConf.AutoCommit) == 1 {
		t.UpdateCommittedOffset(&dend)
	}

	if trace {
		if m.TraceID != 0 || atomic.LoadInt32(&t.EnableTrace) == 1 || t.tpLog.Level() >= levellogger.LOG_DETAIL {
			nsqMsgTracer.TracePub(t.GetTopicName(), t.GetTopicPart(), "PUB", m.TraceID, m, offset, dend.TotalMsgCnt())
		}
	}
	// TODO: handle delayed type for dpub and transaction message
	// should remove from delayed queue after written on disk file
	return m.ID, offset, writeBytes, dend, nil
}

func updateChannelEnd(forceReload bool, e BackendQueueEnd, ch *Channel) {
	if e == nil {
		return
	}
	err := ch.UpdateQueueEnd(e, forceReload)
	if err != nil {
		if err != ErrExiting {
			nsqLog.LogErrorf(
				"%v failed to update topic end to channel(%s) - %s",
				ch.GetTopicName(),
				ch.name, err)
		}
	}
}

func (t *Topic) getCommittedEnd() BackendQueueEnd {
	e := t.backend.GetQueueReadEnd()
	curCommit := t.GetCommitted()
	// if not committed, we need wait to notify channel.
	if curCommit != nil && e.Offset() > curCommit.Offset() {
		if t.tpLog.Level() >= levellogger.LOG_DEBUG {
			t.tpLog.Logf("topic end to commit: %v, read end: %v", curCommit, e)
		}
		e = curCommit
	}
	return e
}

func (t *Topic) updateChannelsEnd(forceReload bool, forceUpdate bool) {
	s := time.Now()
	e := t.getCommittedEnd()
	t.channelLock.RLock()
	if e != nil {
		for _, channel := range t.channelMap {
			if forceUpdate || channel.IsWaitingMoreData() {
				updateChannelEnd(forceReload, e, channel)
			}
		}
	}
	t.channelLock.RUnlock()
	cost := time.Since(s)
	if cost > time.Second/2 {
		t.tpLog.LogWarningf("topic update channels end cost: %v", cost)
	}
}

func (t *Topic) TotalMessageCnt() uint64 {
	return uint64(t.backend.GetQueueWriteEnd().TotalMsgCnt())
}

func (t *Topic) GetQueueReadStart() int64 {
	return int64(t.backend.GetQueueReadStart().Offset())
}

func (t *Topic) TotalDataSize() int64 {
	e := t.backend.GetQueueWriteEnd()
	if e == nil {
		return 0
	}
	return int64(e.Offset())
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.tpLog.Logf("TOPIC deleting")
		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.nsqdNotify.NotifyStateChanged(t, true)
	} else {
		t.tpLog.Logf("TOPIC closing")
	}
	close(t.quitChan)
	// this will wait pub loop,
	// pub loop may be blocked by cluster write which may hold the write lock for coordinator,
	// we need avoid wait close/delete topic in coordinator.
	t.wg.Wait()

	t.Lock()
	defer t.Unlock()

	if deleted {
		t.channelLock.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.channelLock.Unlock()

		if t.GetDelayedQueue() != nil {
			t.GetDelayedQueue().Delete()
		}
		// empty the queue (deletes the backend files, too)
		t.Empty()
		t.removeHistoryStat()
		t.RemoveChannelMeta()
		t.removeMagicCode()
		return t.backend.Delete()
	}

	// write anything leftover to disk
	t.flushData()
	t.updateChannelsEnd(false, true)
	t.tpLog.Logf("[TRACE_DATA] exiting topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.SaveChannelMeta()
	t.channelLock.RLock()
	// close all the channels
	for _, channel := range t.channelMap {
		t.tpLog.Logf("[TRACE_DATA] exiting channel : %v, %v, %v, %v", channel.GetName(), channel.GetConfirmed(), channel.Depth(), channel.backend.GetQueueReadEnd())
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.tpLog.Logf(" channel(%s) close - %s", channel.name, err)
		}
	}
	t.channelLock.RUnlock()

	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().Close()
	}
	return t.backend.Close()
}

func (t *Topic) IsWriteDisabled() bool {
	return atomic.LoadInt32(&t.writeDisabled) == 1
}

// for leader, we can continue consume on disabled leader
func (t *Topic) DisableForSlave(keepConsume bool) {
	if atomic.CompareAndSwapInt32(&t.writeDisabled, 0, 1) {
		t.tpLog.Logf("[TRACE_DATA] while disable topic end: %v, cnt: %v, queue start: %v",
			t.TotalDataSize(), t.TotalMessageCnt(), t.backend.GetQueueReadStart())
	}

	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(!keepConsume)
		d, ok := c.backend.(*diskQueueReader)
		var curRead BackendQueueEnd
		if ok {
			curRead = d.GetQueueCurrentRead()
		}

		t.tpLog.Logf("[TRACE_DATA] while disable channel : %v, %v, %v, %v, %v", c.GetName(),
			c.GetConfirmed(), c.Depth(), c.backend.GetQueueReadEnd(), curRead)
	}
	t.channelLock.RUnlock()
	// notify de-register from lookup
	t.nsqdNotify.NotifyStateChanged(t, false)
}

func (t *Topic) EnableForMaster() {
	if atomic.CompareAndSwapInt32(&t.writeDisabled, 1, 0) {
		t.tpLog.Logf("[TRACE_DATA] while enable topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	}
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(false)
		d, ok := c.backend.(*diskQueueReader)
		var curRead BackendQueueEnd
		if ok {
			curRead = d.GetQueueCurrentRead()
		}
		t.tpLog.Logf("[TRACE_DATA] while enable channel : %v, %v, %v, %v, %v", c.GetName(),
			c.GetConfirmed(), c.Depth(), c.backend.GetQueueReadEnd(), curRead)
	}
	t.channelLock.RUnlock()
	// notify re-register to lookup
	t.nsqdNotify.NotifyStateChanged(t, false)
}

func (t *Topic) Empty() error {
	t.tpLog.Logf("TOPIC empty")
	return t.backend.Empty()
}

func (t *Topic) ForceFlush() {
	if t.tpLog.Level() >= levellogger.LOG_DETAIL {
		e := t.backend.GetQueueReadEnd()
		curCommit := t.GetCommitted()
		t.tpLog.Logf("topic end to commit: %v, read end: %v", curCommit, e)
	}

	t.flushData()

	s := time.Now()
	e := t.getCommittedEnd()
	useFsync := t.option.UseFsync
	t.channelLock.RLock()
	for _, channel := range t.channelMap {
		updateChannelEnd(false, e, channel)
		channel.Flush(useFsync)
		cost := time.Since(s)
		if cost > slowCost*10 {
			t.tpLog.Logf("topic flush channel %v cost: %v", channel.GetName(), cost)
		}
	}
	t.channelLock.RUnlock()
	cost := time.Since(s)
	if cost > slowCost*10 {
		t.tpLog.Logf("topic flush channel cost: %v", cost)
	}
}

func (t *Topic) flushBuffer(notifyCh bool) error {
	hasData := t.backend.FlushBuffer()
	if notifyCh && hasData {
		t.notifyChEndChanged(true)
	}
	return nil
}

func (t *Topic) IsFsync() bool {
	syncEvery := atomic.LoadInt64(&t.dynamicConf.SyncEvery)
	useFsync := syncEvery == 1 || t.option.UseFsync
	return useFsync
}

func (t *Topic) flushData() (error, bool) {
	syncEvery := atomic.LoadInt64(&t.dynamicConf.SyncEvery)
	useFsync := syncEvery == 1 || t.option.UseFsync

	s := time.Now()
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().ForceFlush()
	}

	ok := atomic.CompareAndSwapInt32(&t.needFlush, 1, 0)
	if !ok {
		return nil, false
	}
	cost1 := time.Since(s)
	atomic.StoreInt64(&t.lastSyncCnt, t.backend.GetQueueWriteEnd().TotalMsgCnt())
	err := t.backend.Flush(useFsync)
	if err != nil {
		t.tpLog.LogErrorf("failed flush: %v", err)
		return err, false
	}
	cost2 := time.Since(s)
	if cost2 >= slowCost {
		t.tpLog.LogWarningf("topic flush cost: %v, %v", cost1, cost2)
	}
	return err, true
}

func (t *Topic) PrintCurrentStats() {
	t.tpLog.Logf("topic status: start: %v, write end %v", t.backend.GetQueueReadStart(), t.backend.GetQueueWriteEnd())
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		t.tpLog.Logf("channel(%s) depth: %v, confirmed: %v, debug: %v", ch.GetName(), ch.Depth(),
			ch.GetConfirmed(), ch.GetChannelDebugStats())
	}
	t.channelLock.RUnlock()
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.channelLock.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.channelLock.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.option.E2EProcessingLatencyWindowTime,
				t.option.E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

// maybe should return the cleaned offset to allow commit log clean
func (t *Topic) TryCleanOldData(retentionSize int64, noRealClean bool, maxCleanOffset BackendOffset) (BackendQueueEnd, error) {
	// clean the data that has been consumed and keep the retention policy
	var oldestPos BackendQueueEnd
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		pos := ch.GetConfirmed()
		if oldestPos == nil {
			oldestPos = pos
		} else if oldestPos.Offset() > pos.Offset() {
			oldestPos = pos
		}
	}
	t.channelLock.RUnlock()
	if oldestPos == nil {
		t.tpLog.Debugf("no consume position found for topic: %v", t.GetFullName())
		return nil, nil
	}
	cleanStart := t.backend.GetQueueReadStart()
	t.tpLog.Debugf("clean topic data current start: %v, oldest confirmed %v, max clean end: %v",
		cleanStart, oldestPos, maxCleanOffset)
	if cleanStart.Offset()+BackendOffset(retentionSize) >= oldestPos.Offset() {
		return nil, nil
	}

	if oldestPos.Offset() < maxCleanOffset || maxCleanOffset == BackendOffset(0) {
		maxCleanOffset = oldestPos.Offset()
	}
	snapReader := NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, oldestPos)
	snapReader.SetQueueStart(cleanStart)

	seekCnt := int64(0)
	if cleanStart.TotalMsgCnt() > 0 {
		seekCnt = cleanStart.TotalMsgCnt() - 1
	}
	err := snapReader.SeekTo(cleanStart.Offset(), seekCnt)
	if err != nil {
		t.tpLog.Errorf("topic failed to seek to %v: %v", cleanStart, err)
		return nil, err
	}

	var cleanEndInfo BackendQueueOffset
	t.Lock()
	retentionDay := atomic.LoadInt32(&t.dynamicConf.RetentionDay)
	if retentionDay == 0 {
		retentionDay = int32(DEFAULT_RETENTION_DAYS)
	}
	cleanTime := time.Now().Add(-1 * time.Hour * 24 * time.Duration(retentionDay))
	t.Unlock()
	for {
		readInfo := snapReader.GetCurrentReadQueueOffset()
		data := snapReader.ReadOne()
		if data.Err != nil {
			if data.Err == io.EOF {
				break
			}
			t.tpLog.Warningf("failed to read at %v - %s", readInfo, data.Err)
			err := snapReader.SkipToNext()
			if err != nil {
				t.tpLog.Logf("failed to skip - %s ", err)
				break
			}
			continue
		}
		if retentionSize > 0 {
			// clean data ignore the retention day
			// only keep the retention size (start from the last consumed)
			if data.Offset > maxCleanOffset-BackendOffset(retentionSize) {
				break
			}
			cleanEndInfo = readInfo
		} else {
			msg, decodeErr := decodeMessage(data.Data, t.IsExt())
			if decodeErr != nil {
				t.tpLog.LogErrorf("failed to decode message - %s - %v", decodeErr, data)
			} else {
				if msg.Timestamp >= cleanTime.UnixNano() {
					break
				}
				if data.Offset >= maxCleanOffset {
					break
				}
				cleanEndInfo = readInfo
			}
		}
		err = snapReader.SkipToNext()
		if err != nil {
			t.tpLog.Logf("failed to skip - %s ", err)
			break
		}
	}

	if cleanEndInfo == nil || cleanEndInfo.Offset()+BackendOffset(retentionSize) >= maxCleanOffset {
		if cleanEndInfo != nil {
			t.tpLog.Infof("clean topic data at position: %v could not exceed current oldest confirmed %v and max clean end: %v",
				cleanEndInfo, oldestPos, maxCleanOffset)
		}
		return nil, nil
	}
	t.tpLog.Infof("clean topic data from %v under retention %v, %v",
		cleanEndInfo, cleanTime, retentionSize)
	if !noRealClean && t.kvTopic != nil {
		t.kvTopic.TryCleanOldData(retentionSize, cleanEndInfo, maxCleanOffset)
	}
	return t.backend.CleanOldDataByRetention(cleanEndInfo, noRealClean, maxCleanOffset)
}

func (t *Topic) TryFixQueueEnd(vend BackendOffset, totalCnt int64) error {
	old := t.backend.GetQueueWriteEnd()
	if old.Offset() == vend && old.TotalMsgCnt() == totalCnt {
		return nil
	}
	t.tpLog.Logf("topic try fix the backend end from %v to : %v, %v", old, vend, totalCnt)
	dend, err := t.backend.TryFixWriteEnd(vend, totalCnt)
	if err != nil {
		t.tpLog.LogErrorf("fix backend to %v error: %v", vend, err)
		return err
	}
	// should check if there are kvtopic data missing
	t.UpdateCommittedOffset(&dend)
	t.updateChannelsEnd(true, true)
	_, err = t.tryFixKVTopic()
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) ResetBackendWithQueueStartNoLock(queueStartOffset int64, queueStartCnt int64) error {
	if !t.IsWriteDisabled() {
		t.tpLog.Warningf("reset the topic backend only allow while write disabled")
		return ErrOperationInvalidState
	}
	if queueStartOffset < 0 || queueStartCnt < 0 {
		return errors.New("queue start should not less than 0")
	}
	queueStart := t.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	queueStart.virtualEnd = BackendOffset(queueStartOffset)
	queueStart.totalMsgCnt = queueStartCnt
	t.tpLog.Warningf("reset the topic backend with queue start: %v", queueStart)
	err := t.backend.ResetWriteWithQueueStart(queueStart)
	if err != nil {
		return err
	}
	if t.kvTopic != nil {
		err = t.kvTopic.ResetBackendWithQueueStart(queueStartOffset, queueStartCnt)
		if err != nil {
			return err
		}
	}
	newEnd := t.backend.GetQueueReadEnd()
	t.UpdateCommittedOffset(newEnd)

	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		t.tpLog.Infof("channel stats: %v", ch.GetChannelDebugStats())
		ch.UpdateQueueEnd(newEnd, true)
		ch.ConfirmBackendQueueOnSlave(newEnd.Offset(), newEnd.TotalMsgCnt(), true)
	}
	t.channelLock.RUnlock()
	return nil
}

func (t *Topic) GetDelayedQueueUpdateTs() (int64, bool) {
	if t.IsOrdered() {
		return 0, false
	}
	dq := t.GetDelayedQueue()
	if dq == nil {
		return 0, false
	}
	ts := dq.GetChangedTs()
	return ts, true
}

func (t *Topic) GetDelayedQueueConsumedState() (int64, RecentKeyList, map[int]uint64, map[string]uint64) {
	if t.IsOrdered() {
		return 0, nil, nil, nil
	}
	dq := t.GetDelayedQueue()
	if dq == nil {
		return 0, nil, nil, nil
	}
	ts := time.Now().UnixNano()
	chList := make([]string, 0)
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		chList = append(chList, ch.GetName())
	}
	t.channelLock.RUnlock()
	kl, cntList, chDelayCntList := dq.GetOldestConsumedState(chList, true)
	return ts, kl, cntList, chDelayCntList
}

func (t *Topic) UpdateDelayedQueueConsumedState(ts int64, keyList RecentKeyList, cntList map[int]uint64, channelCntList map[string]uint64) error {
	if t.IsOrdered() {
		t.tpLog.Infof("should never delayed queue in ordered topic")
		return nil
	}
	dq := t.GetDelayedQueue()
	if dq == nil {
		t.tpLog.Infof("no delayed queue while update delayed state on topic")
		return nil
	}

	return dq.UpdateConsumedState(ts, keyList, cntList, channelCntList)
}

// after crash, some topic meta need to be fixed by manual
func (t *Topic) TryFixData(checkCorrupt bool) error {
	t.Lock()
	defer t.Unlock()
	t.backend.tryFixData()
	dq := t.GetDelayedQueue()
	if dq != nil {
		dq.backend.tryFixData()
	}
	if checkCorrupt {
		err := t.tryFixCorruptData()
		if err != nil {
			return err
		}
	}
	// TODO: fix channel meta
	_, err := t.tryFixKVTopic()
	return err
}

func (t *Topic) tryFixCorruptData() error {
	// try check the disk queue data by read from start to end, if corrupt is head or tail of queue,
	// we can just truncate header or tail.
	// if corrupt is in the middle (start, corrupt-pos, end), we should handle like below
	// 1.if all consumed is higher than corrupt-pos, we truncate the data between start and oldest consumed
	// 2.if some consumed less than corrupt-pos, we truncate the data between start and corrupt-pos, and move consumed pos
	// which less than corrupt-pos to the corrupt-pos.
	snap := t.GetDiskQueueSnapshot(false)
	defer snap.Close()
	start := t.backend.GetQueueReadStart()
	seekCnt := start.TotalMsgCnt()
	lastCorrupt, lastCnt, err := snap.CheckDiskQueueReadToEndOK(int64(start.Offset()), seekCnt, t.getCommittedEnd().Offset())
	if err == nil {
		return nil
	}
	t.tpLog.Warningf("check read failed at: %v, %v, err: %s", lastCorrupt, lastCnt, err)
	var oldestPos BackendQueueEnd
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		pos := ch.GetConfirmed()
		if oldestPos == nil {
			oldestPos = pos
		} else if oldestPos.Offset() > pos.Offset() {
			oldestPos = pos
		}
	}
	t.channelLock.RUnlock()
	if oldestPos != nil && int64(oldestPos.Offset()) > lastCorrupt {
		t.tpLog.Warningf("clean corrupt topic data to %v", oldestPos)
		t.backend.CleanOldDataByRetention(oldestPos, false, t.getCommittedEnd().Offset())
	} else {
		// check if all the tail is corrupt
		snap.ResetToStart()
		var fixerr error
		for {
			fixerr = snap.SkipToNext()
			if fixerr == ErrReadEndOfQueue {
				break
			}
			curRead := snap.GetCurrentReadQueueOffset()
			if curRead.Offset() > BackendOffset(lastCorrupt) {
				break
			}
		}
		if fixerr != nil && fixerr != ErrReadEndOfQueue {
			t.tpLog.Warningf("clean corrupt topic data failed, err %s", fixerr)
			return fixerr
		}
		curRead := snap.GetCurrentReadQueueOffset()
		if curRead.Offset() <= BackendOffset(lastCorrupt) {
			t.tpLog.Warningf("clean corrupt topic data since tail is corrupted, %v", curRead)
			// all tail is corrupt, we need truncate the tail
			t.ResetBackendEndNoLock(BackendOffset(lastCorrupt), lastCnt)
		} else {
			t.tpLog.Warningf("clean corrupt topic data since old is corrupted, %v", curRead)
			// some middle is corrupt, we just truncate old
			newStart, err := t.backend.CleanOldDataByRetention(curRead, false, t.getCommittedEnd().Offset())
			if err != nil {
				return err
			}
			t.tpLog.Warningf("clean corrupt topic data since old is corrupted, %v, %v", curRead, newStart)
			t.channelLock.RLock()
			for _, ch := range t.channelMap {
				pos := ch.GetConfirmed()
				if pos.Offset() < newStart.Offset() {
					t.tpLog.Infof("channel set new offset %v, old stats: %v", newStart, ch.GetChannelDebugStats())
					ch.SetConsumeOffset(newStart.Offset(), newStart.TotalMsgCnt(), true)
				}
			}
			t.channelLock.RUnlock()
		}
	}
	return nil
}

func (t *Topic) CheckDiskQueueReadToEndOK(offset int64, seekCnt int64, endOffset BackendOffset) error {
	snap := t.GetDiskQueueSnapshot(false)
	defer snap.Close()
	_, _, err := snap.CheckDiskQueueReadToEndOK(offset, seekCnt, endOffset)
	if err != nil {
		t.tpLog.Warningf("check read failed at: %v, err: %s", offset, err)
		return err
	}
	return nil
}

// should be locked outside
func (t *Topic) tryFixKVTopic() (int64, error) {
	// try replay with the disk queue if kv topic data missing some data in end
	// try handle some case :
	// 1. empty kv data
	// 2. queue start not matched
	// 3. queue end not matched
	if t.kvTopic == nil {
		return 0, nil
	}
	dqEnd := t.backend.GetQueueWriteEnd()
	dqStart := t.backend.GetQueueReadStart()

	offset, cnt, err := t.kvTopic.GetTopicMeta()
	if err != nil {
		return 0, err
	}
	if cnt > 0 {
		if id1, lastCntOffset1, err := t.kvTopic.GetMsgByCnt(cnt - 1); err != nil {
			// since the meta is not matched msg, we try fix all index data
			t.tpLog.Warningf("kv topic end need fix since last count message not found: %v-%v, %v", offset, cnt, dqEnd)
			offset = int64(dqStart.Offset())
			cnt = dqStart.TotalMsgCnt()
			// will be reset start below
		} else {
			id2, offsetCnt, _, lastCntOffset2, err := t.kvTopic.getMsgIDCntTsLessThanOffset(offset)
			if err != nil {
				t.tpLog.Warningf("kv topic end need fix since last offset message not found: %v-%v, %v", offset, cnt, dqEnd)
				offset = int64(dqStart.Offset())
				cnt = dqStart.TotalMsgCnt()
				// will be reset start below
			} else if id1.ID != id2 || offsetCnt != cnt-1 || lastCntOffset1 != lastCntOffset2 {
				t.tpLog.Warningf("kv topic end need fix since last offset message not match: %v-%v, %v vs %v, %v, %v vs %v", offset, cnt, id1, id2, offsetCnt,
					lastCntOffset1, lastCntOffset2)
				offset = int64(dqStart.Offset())
				cnt = dqStart.TotalMsgCnt()
			}
		}
	}
	if dqEnd.TotalMsgCnt() == cnt && dqEnd.Offset() == BackendOffset(offset) {
		return 0, nil
	}
	t.ForceFlushForChannels(true)
	snap := t.GetDiskQueueSnapshot(false)
	defer snap.Close()
	if offset <= int64(dqStart.Offset()) || cnt <= dqStart.TotalMsgCnt() {
		t.tpLog.Warningf("kv topic need empty since end is less than queue start: %v-%v, %v", offset, cnt, dqStart)
		err = t.kvTopic.ResetBackendWithQueueStart(int64(dqStart.Offset()), dqStart.TotalMsgCnt())
		if err != nil {
			return 0, err
		}
		offset = int64(dqStart.Offset())
		cnt = dqStart.TotalMsgCnt()
		if dqStart.Offset() == dqEnd.Offset() {
			return 0, nil
		}
		err = snap.SeekTo(dqStart.Offset(), dqStart.TotalMsgCnt())
		if err != nil {
			return 0, err
		}
	} else {
		err = snap.SeekTo(BackendOffset(offset), cnt)
		if err != nil {
			return 0, err
		}
	}
	t.tpLog.Warningf("kv topic end need fix since not matched: %v-%v, %v", offset, cnt, dqEnd)
	fixedCnt := int64(0)
	for {
		rr := snap.ReadOne()
		if rr.Err != nil {
			if rr.Err == io.EOF {
				break
			}
			t.tpLog.Warningf("kv topic end fix error: %s, cnt: %v", rr.Err, fixedCnt)
			return fixedCnt, rr.Err
		}
		_, _, _, err = t.kvTopic.putRaw(rr.Data, rr.Offset, int64(rr.MovedSize))
		if err != nil {
			t.tpLog.Warningf("kv topic end fix error: %s, %v", err, rr)
			return fixedCnt, err
		}
		fixedCnt++
	}
	offset, cnt, err = t.kvTopic.GetTopicMeta()
	if err != nil {
		t.tpLog.Warningf("kv topic end fix error: %s", err)
		return fixedCnt, err
	}
	t.tpLog.Warningf("kv topic end fixed to: %v-%v, %v", offset, cnt, fixedCnt)
	if BackendOffset(offset) != dqEnd.Offset() || cnt != dqEnd.TotalMsgCnt() {
		t.tpLog.Warningf("kv topic end not matched after fixed: %v, %v-%v", dqEnd, offset, cnt)
		return fixedCnt, errors.New("failed to fix kv topic")
	}
	return fixedCnt, nil
}
