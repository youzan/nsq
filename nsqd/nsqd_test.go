package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	jsoniter "github.com/json-iterator/go"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/test"
)

func init() {
	SetLogger(&levellogger.SimpleLogger{})
}

func assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line}, v...)...)
		t.FailNow()
	}
}

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
	level int32
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
func (tl *testLogger) OutputErr(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
func (tl *testLogger) OutputWarning(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) levellogger.Logger {
	return &testLogger{tbl, 0}
}

func adjustDefaultOptsForTest(opts *Options) *Options {
	opts.QueueScanRefreshInterval = time.Second / 10
	opts.QueueScanInterval = time.Second / 100
	opts.SyncEvery = 1
	opts.MsgTimeout = 100 * time.Millisecond
	opts.LogLevel = 3
	if testing.Verbose() {
		opts.LogLevel = 4
	}
	SetLogger(opts.Logger)
	return opts
}

func mustStartNSQD(opts *Options) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	opts.KVEnabled = true
	opts.KVEnabled = false
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd, err := New(opts)
	if err != nil {
		panic(err)
	}
	nsqd.Start()
	return nil, nil, nsqd
}

func getMetadata(n *NSQD) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	return js, nil
}

func API(endpoint string) (data *simplejson.Json, err error) {
	d := make(map[string]interface{})
	err = http_api.NewClient(nil).NegotiateV1(endpoint, &d)
	data = simplejson.New()
	data.SetPath(nil, d)
	return
}

func TestStartup(t *testing.T) {
	iterations := 300
	doneExitChan := make(chan int)

	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 2
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	origDataPath := opts.DataPath

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	// verify nsqd metadata shows no topics
	err := nsqd.persistMetadata(nsqd.GetTopicMapCopy())
	equal(t, err, nil)
	atomic.StoreInt32(&nsqd.isLoading, 1)
	nsqd.GetTopicIgnPart(topicName) // will not persist if `flagLoading`
	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 0)
	nsqd.DeleteExistingTopic(topicName, 0)
	atomic.StoreInt32(&nsqd.isLoading, 0)

	body := make([]byte, 256)
	tmpbuf := bytes.NewBuffer(make([]byte, 1024))
	tmpmsg := NewMessage(0, body)
	tmpbuf.Reset()
	msgRawSize, _ := tmpmsg.WriteTo(tmpbuf, false)
	msgRawSize += 4
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch1")
	for i := 0; i < iterations; i++ {
		msg := NewMessage(0, body)
		topic.PutMessage(msg)
	}

	topic.ForceFlush()
	t.Logf("topic: %v. %v", topic.GetCommitted(), topic.backend.GetQueueReadEnd())
	backEnd := topic.backend.GetQueueReadEnd()
	equal(t, backEnd.Offset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.TotalMsgCnt(), int64(iterations))
	channel2 := topic.GetChannel("ch2")

	err = nsqd.persistMetadata(nsqd.GetTopicMapCopy())
	equal(t, err, nil)
	t.Logf("msgs: depth: %v. %v", channel1.Depth(), channel1.DepthSize())
	equal(t, channel1.Depth(), int64(iterations))
	equal(t, channel1.DepthSize(), int64(iterations)*msgRawSize)
	equal(t, channel1.backend.(*diskQueueReader).queueEndInfo.Offset(),
		BackendOffset(channel1.DepthSize()))

	// new channel should consume from end
	equal(t, channel2.Depth(), int64(0))
	equal(t, channel2.DepthSize(), int64(0)*msgRawSize)
	equal(t, channel2.backend.(*diskQueueReader).queueEndInfo.Offset(),
		BackendOffset(channel1.DepthSize()))
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		//t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}
	channel1.backend.ConfirmRead(BackendOffset(int64(iterations/2)*
		msgRawSize), int64(iterations/2))
	equal(t, channel1.backend.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.Depth(), int64(iterations/2))
	equal(t, channel1.DepthSize(), int64(iterations/2)*msgRawSize)

	// make sure metadata shows the topic
	metaData, err = getMetadata(nsqd)
	equal(t, err, nil)
	t.Logf("meta: %v", metaData)
	topics, err = metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 1)
	observedTopicName, err := metaData.Get("topics").GetIndex(0).Get("name").String()
	equal(t, observedTopicName, topicName)
	equal(t, err, nil)

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	opts = NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 0
	opts.SyncEvery = 1
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	opts.DataPath = origDataPath
	_, _, nsqd = mustStartNSQD(opts)

	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqd.GetTopicIgnPart(topicName)
	backEnd = topic.backend.GetQueueReadEnd()
	equal(t, backEnd.Offset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.TotalMsgCnt(), int64(iterations))

	channel1 = topic.GetChannel("ch1")
	channel1.UpdateQueueEnd(backEnd, false)

	equal(t, channel1.backend.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.backend.(*diskQueueReader).queueEndInfo.Offset(),
		backEnd.Offset())

	equal(t, channel1.Depth(), int64(iterations/2))
	equal(t, channel1.DepthSize(), int64(iterations/2)*msgRawSize)

	time.Sleep(time.Second)
	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		equal(t, msg.Body, body)
	}

	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(t *testing.T, n *NSQD, topic *Topic, channelIndex int) *ChannelMetaInfo {
	fn := topic.getChannelMetaFileName()
	metas, err := topic.metaStorage.LoadChannelMeta(fn)
	equal(t, err, nil)
	equal(t, len(metas) > channelIndex, true)
	return metas[channelIndex]
}

func TestPauseMetadata(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async persistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "pause_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	topic.SaveChannelMeta()

	b := metadataForChannel(t, nsqd, topic, 0).Paused
	equal(t, b, false)

	channel.Pause()
	b = metadataForChannel(t, nsqd, topic, 0).Paused
	equal(t, b, false)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).Paused
	equal(t, b, true)

	channel.UnPause()
	b = metadataForChannel(t, nsqd, topic, 0).Paused
	equal(t, b, true)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).Paused
	equal(t, b, false)
}

func TestZantestSkipMetaData(t *testing.T) {
	opts := NewOptions()
	opts.AllowZanTestSkip = true
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async persistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "zantestskip_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicWithExt(topicName, 0, false)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())

	b := metadataForChannel(t, nsqd, topic, 0).ZanTestSkipped
	equal(t, b, true)

	channel.UnskipZanTest()
	b = metadataForChannel(t, nsqd, topic, 0).ZanTestSkipped
	equal(t, b, true)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).IsZanTestSkipepd()
	equal(t, b, false)

	channel.SkipZanTest()
	b = metadataForChannel(t, nsqd, topic, 0).IsZanTestSkipepd()
	equal(t, b, false)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).IsZanTestSkipepd()
	equal(t, b, true)
}

func TestDisableChannelAutoCreateMetaData(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async persistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "disabel_channel_auto_create_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicWithExt(topicName, 0, false)
	topic.DisableChannelAutoCreate()

	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())

	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	found := false
	for ti := range topics {
		topicJs := metaData.Get("topics").GetIndex(ti)

		topic, err := topicJs.Get("name").String()
		equal(t, err, nil)
		if topicName == topic {
			disableChannelAutoCreated, err := topicJs.Get("disbale_channel_auto_create").Bool()
			equal(t, err, nil)
			equal(t, disableChannelAutoCreated, true)
			found = true
			break
		}
	}
	equal(t, found, true)

	topic.EnableChannelAutoCreate()
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())

	metaData, err = getMetadata(nsqd)
	equal(t, err, nil)
	topics, err = metaData.Get("topics").Array()
	found = false
	for ti := range topics {
		topicJs := metaData.Get("topics").GetIndex(ti)

		topic, err := topicJs.Get("name").String()
		equal(t, err, nil)
		if topicName == topic {
			disableChannelAutoCreated, err := topicJs.Get("disbale_channel_auto_create").Bool()
			equal(t, err, nil)
			equal(t, disableChannelAutoCreated, false)
			found = true
			break
		}
	}
	equal(t, found, true)
}

func TestSkipMetaData(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async persistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "skip_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	topic.SaveChannelMeta()

	b := metadataForChannel(t, nsqd, topic, 0).Skipped
	equal(t, b, false)

	channel.Skip()
	b = metadataForChannel(t, nsqd, topic, 0).Skipped
	equal(t, b, false)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).Skipped
	equal(t, b, true)

	channel.UnSkip()
	b = metadataForChannel(t, nsqd, topic, 0).Skipped
	equal(t, b, true)

	topic.SaveChannelMeta()
	b = metadataForChannel(t, nsqd, topic, 0).Skipped
	equal(t, b, false)
}

func TestSetHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.KVEnabled = false
	nsqd, err := New(opts)
	equal(t, err, nil)

	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.IsHealthy(), true)

	nsqd.SetHealth(nil)
	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.IsHealthy(), true)

	nsqd.SetHealth(errors.New("health error"))
	nequal(t, nsqd.GetError(), nil)
	equal(t, nsqd.GetHealth(), "NOK - health error")
	equal(t, nsqd.IsHealthy(), false)

	nsqd.SetHealth(nil)
	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.GetHealth(), "OK")
	equal(t, nsqd.IsHealthy(), true)
}

func TestLoadTopicMetaExt(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	//defer nsqd.Exit()

	// avoid concurrency issue of async persistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "load_topic_meta" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	topicNameExt := "load_topic_meta_ext" + strconv.Itoa(int(time.Now().Unix()))
	topicExt := nsqd.GetTopicIgnPart(topicNameExt)
	topicDynConf := TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topicExt.SetDynamicInfo(topicDynConf, nil)
	topicExt.GetChannel("ch")

	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())
	nsqd.Exit()

	_, _, nsqd = mustStartNSQD(opts)
	defer nsqd.Exit()
	nsqd.LoadMetadata(1)

	topic, err := nsqd.GetExistingTopic(topicName, 0)
	if err != nil {
		t.FailNow()
	}
	if topic.IsExt() {
		t.FailNow()
	}

	topicExt, err = nsqd.GetExistingTopic(topicNameExt, 0)
	if err != nil {
		t.FailNow()
	}
	if !topicExt.IsExt() {
		t.FailNow()
	}
}

func TestLoadTopicChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "load_topic_meta" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")
	ch.Skip()
	ch_closed := topic.GetChannel("ch_closed")
	ch_closed.Pause()
	ch, err := topic.GetExistingChannel("ch_closed")
	if err != nil || ch == nil {
		t.FailNow()
	}

	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())

	topic.CloseExistingChannel("ch_closed", false)
	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.persistMetadata(nsqd.GetTopicMapCopy())
	_, err = topic.GetExistingChannel("ch_closed")
	if err == nil {
		t.Errorf("should closed this channel after reload")
	}

	nsqd.Exit()

	_, _, nsqd = mustStartNSQD(opts)
	defer nsqd.Exit()
	nsqd.LoadMetadata(1)

	// read meta file and check if load meta changed
	fn := topic.getChannelMetaFileName()
	channels, err := topic.metaStorage.LoadChannelMeta(fn)
	test.Nil(t, err)
	for _, chmeta := range channels {
		if chmeta.Name == ch.GetName() {
			test.Equal(t, true, chmeta.Skipped)
		}
	}

	topic, err = nsqd.GetExistingTopic(topicName, 0)
	if err != nil {
		t.FailNow()
	}

	ch, err = topic.GetExistingChannel("ch")
	if err != nil || ch == nil {
		t.FailNow()
	}
	_, err = topic.GetExistingChannel("ch_closed")
	if err == nil {
		t.Errorf("should closed this channel after reload")
	}
}

func BenchmarkJsonExtV1(b *testing.B) {
	jb := []byte("{\"key\":\"true\", \"##key\":\"v1\", \"keybool\":true}")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		extHeader := &JsonExtObj{
			jsonExt: jsoniter.Get(jb),
		}
		extHeader.GetBoolOrStringBool("key")
		extHeader.GetBoolOrStringBool("keybool")
		extHeader.GetBoolOrStringBool("keynoexsit")
		extHeader.GetString("key")
		extHeader.GetString("keynoexsit")
	}
}

func BenchmarkJsonExtV2(b *testing.B) {
	jb := []byte("{\"key\":\"true\", \"##key\":\"v1\", \"keybool\":true}")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		extHeader, _ := newJsonExtObjV2(jb)
		extHeader.GetBoolOrStringBool("key")
		extHeader.GetBoolOrStringBool("keybool")
		extHeader.GetBoolOrStringBool("keynoexsit")
		extHeader.GetString("key")
		extHeader.GetString("keynoexsit")
	}
}
