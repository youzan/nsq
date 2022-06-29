package nsqd

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	//"runtime"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/glog"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/test"
)

func TestGetTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic1 := nsqd.GetTopic("test", 0, false)
	test.NotNil(t, topic1)
	test.Equal(t, "test", topic1.GetTopicName())

	topic2 := nsqd.GetTopic("test", 0, false)
	test.Equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2", 1, false)
	test.Equal(t, "test2", topic3.GetTopicName())
	test.NotEqual(t, topic2, topic3)

	topic1_1 := nsqd.GetTopicIgnPart("test")
	test.Equal(t, "test", topic1_1.GetTopicName())
	test.Equal(t, 0, topic1_1.GetTopicPart())
	topic3_1 := nsqd.GetTopicIgnPart("test2")
	test.Equal(t, "test2", topic3_1.GetTopicName())
	test.Equal(t, 1, topic3_1.GetTopicPart())

}

func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	test.Equal(t, channel1, topic.channelMap["ch1"])
	test.Equal(t, channel2, topic.channelMap["ch2"])
}

func TestLoadChannelMeta(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)
	channel1.Skip()

	test.Equal(t, channel1, topic.channelMap["ch1"])
	topic.SaveChannelMeta()
	nsqd.CloseExistingTopic("test", 0)
	topic.Close()

	topic = nsqd.GetTopic("test", 0, false)
	channel1 = topic.GetChannel("ch1")

	test.Equal(t, true, channel1.IsSkipped())
	// read meta file and check if load meta changed
	fn := topic.getChannelMetaFileName()
	channels, err := topic.metaStorage.LoadChannelMeta(fn)
	test.Nil(t, err)
	for _, chmeta := range channels {
		if chmeta.Name == channel1.GetName() {
			test.Equal(t, chmeta.Skipped, channel1.IsSkipped())
		}
	}
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, errors.New("never gonna happen")
}
func (d *errorBackendQueue) ReadChan() chan []byte                     { return nil }
func (d *errorBackendQueue) Close() error                              { return nil }
func (d *errorBackendQueue) Delete() error                             { return nil }
func (d *errorBackendQueue) Depth() int64                              { return 0 }
func (d *errorBackendQueue) Empty() error                              { return nil }
func (d *errorBackendQueue) Flush() error                              { return nil }
func (d *errorBackendQueue) GetQueueReadEnd() BackendQueueEnd          { return &diskQueueEndInfo{} }
func (d *errorBackendQueue) GetQueueWriteEnd() BackendQueueEnd         { return &diskQueueEndInfo{} }
func (d *errorBackendQueue) ResetWriteEnd(BackendOffset, int64) error  { return nil }
func (d *errorBackendQueue) RollbackWrite(BackendOffset, uint64) error { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, nil
}

func TestTopicMarkRemoved(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	origPath := topic.dataPath

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	for i := 0; i <= 1000; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic1 := nsqd.GetTopic("test", 1, false)
	err := topic.SetMagicCode(time.Now().UnixNano())
	err = topic1.SetMagicCode(time.Now().UnixNano())
	test.Equal(t, nil, err)
	channel1 = topic1.GetChannel("ch1")
	test.NotNil(t, channel1)
	for i := 0; i <= 1000; i++ {
		msg.ID = 0
		topic1.PutMessage(msg)
	}
	topic.ForceFlush()
	topic.SaveHistoryStats()
	topic1.ForceFlush()
	topic1.SaveHistoryStats()
	oldName := topic.backend.fileName(0)
	oldName1 := topic1.backend.fileName(0)
	oldMagicFile := topic.getMagicCodeFileName()
	oldMagicFile1 := topic1.getMagicCodeFileName()
	oldHistoryFile := topic.getHistoryStatsFileName()
	oldHistoryFile1 := topic1.getHistoryStatsFileName()
	_, err = os.Stat(oldMagicFile)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldMagicFile1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldHistoryFile)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldHistoryFile1)
	test.Equal(t, nil, err)

	removedPath, err := topic.MarkAsRemoved()
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))
	// mark as removed should keep the topic base directory
	_, err = os.Stat(origPath)
	test.Equal(t, nil, err)
	// partition data should be removed
	newPath := removedPath
	_, err = os.Stat(newPath)
	defer os.RemoveAll(newPath)
	test.Equal(t, nil, err)
	newName := path.Join(newPath, filepath.Base(oldName))
	newMagicFile := path.Join(newPath, filepath.Base(oldMagicFile))
	// should keep other topic partition
	_, err = os.Stat(oldName1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldMagicFile1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldHistoryFile1)
	test.Equal(t, nil, err)
	_, err = os.Stat(oldName)
	test.NotNil(t, err)
	_, err = os.Stat(oldMagicFile)
	test.NotNil(t, err)
	_, err = os.Stat(oldHistoryFile)
	test.NotNil(t, err)
	_, err = os.Stat(newName)
	test.Equal(t, nil, err)
	_, err = os.Stat(newMagicFile)
	test.Equal(t, nil, err)
}

func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopicIgnPart("test")
	oldMagicFile := path.Join(topic.dataPath, "magic"+strconv.Itoa(topic.partition))

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.SetMagicCode(time.Now().UnixNano())
	_, err = os.Stat(oldMagicFile)
	test.Equal(t, nil, err)
	err = topic.DeleteExistingChannel("ch1")
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	test.NotNil(t, channel2)

	err = nsqd.DeleteExistingTopic("test", topic.GetTopicPart())
	test.Equal(t, nil, err)
	test.Equal(t, 0, len(topic.channelMap))
	test.Equal(t, 0, len(nsqd.topicMap))
	_, err = os.Stat(oldMagicFile)
	test.NotNil(t, err)
}

func TestDeleteLast(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	_, _, _, _, err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	test.Nil(t, err)
}

func TestTopicBackendMaxMsgSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_backend_maxmsgsize" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName, 0, false)

	test.Equal(t, topic.backend.maxMsgSize, int32(opts.MaxMsgSize+minValidMsgLength))
}

func changeDynamicConfAutCommit(dynamicConf *TopicDynamicConf) {
	atomic.StoreInt32(&dynamicConf.AutoCommit, 1)
	atomic.StoreInt64(&dynamicConf.SyncEvery, 10)
}

func TestTopicPutChannelWait(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	for i := 0; i <= 10; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	for i := 0; i <= 10; i++ {
		select {
		case outMsg := <-channel.clientMsgChan:
			test.Equal(t, msg.Body, outMsg.Body)
			channel.ConfirmBackendQueue(outMsg)
		case <-time.After(time.Second):
			t.Fatalf("should read message in channel")
		}
	}
	test.Equal(t, true, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	msg.ID = 0
	topic.PutMessage(msg)
	// wait channel end notify done
	time.Sleep(time.Millisecond * 2000)
	test.Equal(t, false, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	select {
	case outMsg := <-channel.clientMsgChan:
		test.Equal(t, msg.Body, outMsg.Body)
		channel.ConfirmBackendQueue(outMsg)
	case <-time.After(time.Second):
		t.Fatalf("should read the message in channel")
	}
	test.Equal(t, true, channel.IsWaitingMoreData())
	msg.ID = 0
	topic.PutMessage(msg)
	time.Sleep(time.Millisecond * 2000)
	test.Equal(t, false, channel.IsWaitingMoreData())
	test.Equal(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
	msg.ID = 0
	topic.PutMessage(msg)
	time.Sleep(time.Millisecond * 2000)
	test.NotEqual(t, topic.backend.GetQueueReadEnd(), topic.backend.GetQueueWriteEnd())
	test.Equal(t, topic.backend.GetQueueReadEnd(), channel.GetChannelEnd())
}

func TestTopicCleanOldDataByRetentionSize(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	atomic.StoreInt32(&topic.dynamicConf.RetentionDay, 1)

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msgSize := int32(0)
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, _, _ = topic.PutMessage(msg)
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.TryCleanOldData(1, false, 0)
	// should not clean not consumed data
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName := topic.backend.fileName(0)
	fStat, err := os.Stat(startFileName)
	test.Nil(t, err)
	fileSize := fStat.Size()
	fileCnt := fileSize / int64(msgSize)

	for i := 0; i < msgNum-100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}
	topic.TryCleanOldData(1024*1024*2, false, 0)
	test.Equal(t, int64(2), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName = topic.backend.fileName(0)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	startFileName = topic.backend.fileName(1)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	test.Equal(t, BackendOffset(2*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, 2*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())

	topic.TryCleanOldData(1, false, 0)

	// should keep at least 2 files
	test.Equal(t, fileNum-1, topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	test.Equal(t, BackendOffset((fileNum-1)*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, (fileNum-1)*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())
	for i := 0; i < int(fileNum)-1; i++ {
		startFileName = topic.backend.fileName(int64(i))
		_, err = os.Stat(startFileName)
		test.NotNil(t, err)
		test.Equal(t, true, os.IsNotExist(err))
	}
}

func TestTopicCleanOldDataByRetentionDay(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()
	msgSize := int32(0)
	var dend BackendQueueEnd
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.dynamicConf.RetentionDay = 1
	topic.TryCleanOldData(0, false, 0)
	// should not clean not consumed data
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName := topic.backend.fileName(0)
	fStat, err := os.Stat(startFileName)
	test.Nil(t, err)
	fileSize := fStat.Size()
	fileCnt := fileSize / int64(msgSize)

	for i := 0; i < msgNum-100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}
	topic.dynamicConf.RetentionDay = 2
	topic.TryCleanOldData(0, false, 0)
	test.Equal(t, int64(2), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	startFileName = topic.backend.fileName(0)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	startFileName = topic.backend.fileName(1)
	_, err = os.Stat(startFileName)
	test.NotNil(t, err)
	test.Equal(t, true, os.IsNotExist(err))
	test.Equal(t, BackendOffset(2*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, 2*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())

	topic.dynamicConf.RetentionDay = 1
	topic.TryCleanOldData(0, false, 0)

	// should keep at least 2 files
	test.Equal(t, fileNum-1, topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)
	test.Equal(t, BackendOffset((fileNum-1)*fileSize), topic.backend.GetQueueReadStart().Offset())
	test.Equal(t, (fileNum-1)*fileCnt, topic.backend.GetQueueReadStart().TotalMsgCnt())
	for i := 0; i < int(fileNum)-1; i++ {
		startFileName = topic.backend.fileName(int64(i))
		_, err = os.Stat(startFileName)
		test.NotNil(t, err)
		test.Equal(t, true, os.IsNotExist(err))
	}
}

func TestTopicCleanOldDataByRetentionDayWithResetStart(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	readStart := *(topic.backend.GetQueueReadStart().(*diskQueueEndInfo))
	test.Equal(t, int64(0), readStart.EndOffset.FileNum)
	topic.DisableForSlave(false)
	err := topic.ResetBackendWithQueueStartNoLock(0, 0)
	test.Nil(t, err)
	writeEnd := topic.backend.GetQueueWriteEnd()
	test.Equal(t, BackendOffset(0), writeEnd.Offset())
	test.Equal(t, int64(0), writeEnd.TotalMsgCnt())
	readStart = *(topic.backend.GetQueueReadStart().(*diskQueueEndInfo))
	test.Equal(t, BackendOffset(0), readStart.Offset())
	test.Equal(t, int64(0), readStart.TotalMsgCnt())
	err = topic.ResetBackendWithQueueStartNoLock(0, 0)
	test.Nil(t, err)
	writeEnd = topic.backend.GetQueueWriteEnd()
	readStart = *(topic.backend.GetQueueReadStart().(*diskQueueEndInfo))
	test.Equal(t, BackendOffset(0), readStart.Offset())
	test.Equal(t, int64(0), readStart.TotalMsgCnt())
	test.Equal(t, BackendOffset(0), writeEnd.Offset())
	test.Equal(t, int64(0), writeEnd.TotalMsgCnt())

	nsqd.CloseExistingTopic("test", 0)
	topic = nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	writeEnd2 := topic.backend.GetQueueWriteEnd()
	readStart2 := topic.backend.GetQueueReadStart().(*diskQueueEndInfo)
	test.Equal(t, true, writeEnd2.IsSame(writeEnd))
	test.Equal(t, true, readStart2.IsSame(&readStart))

	topic.EnableForMaster()
	msgNum := 1000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()
	msgSize := int32(0)
	var dend BackendQueueEnd
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, err = topic.PutMessage(msg)
		test.Nil(t, err)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(readStart.EndOffset.FileNum), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	for i := 0; i < msgNum; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.dynamicConf.RetentionDay = 1
	_, err = topic.TryCleanOldData(0, false, 0)
	test.Equal(t, readStart, *(topic.backend.GetQueueReadStart().(*diskQueueEndInfo)))
	test.Nil(t, err)
	startFileName := topic.backend.fileName(readStart.EndOffset.FileNum)
	fStat, err := os.Stat(startFileName)
	test.Nil(t, err)
	fileSize := fStat.Size()
	fileCnt := fileSize / int64(msgSize)
	test.Equal(t, int64(msgNum), fileCnt)

	_, err = topic.TryCleanOldData(0, false, 0)
	test.Nil(t, err)
	test.Equal(t, readStart, *(topic.backend.GetQueueReadStart().(*diskQueueEndInfo)))
	startFileName = topic.backend.fileName(int64(fileNum))
	_, err = os.Stat(startFileName)
	test.Nil(t, err)
}

func TestTopicResetWithQueueStart(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()
	msgSize := int32(0)
	var dend BackendQueueEnd
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	nsqLog.Warningf("reading the topic %v backend ", topic.GetFullName())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
	}

	topic.dynamicConf.RetentionDay = 2

	oldEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	// reset with new start
	resetStart := &diskQueueEndInfo{}
	resetStart.virtualEnd = topic.backend.GetQueueWriteEnd().Offset() + BackendOffset(msgSize*10)
	resetStart.totalMsgCnt = topic.backend.GetQueueWriteEnd().TotalMsgCnt() + 10
	err := topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.NotNil(t, err)
	topic.DisableForSlave(false)
	err = topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.Nil(t, err)
	topic.EnableForMaster()

	nsqLog.Warningf("reset the topic %v backend with queue start: %v", topic.GetFullName(), resetStart)
	test.Equal(t, resetStart.Offset(), BackendOffset(topic.GetQueueReadStart()))
	newEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newEnd.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newEnd.TotalMsgCnt())
	test.Equal(t, true, newEnd.EndOffset.GreatThan(&oldEnd.EndOffset))
	test.Equal(t, int64(0), newEnd.EndOffset.Pos)
	test.Equal(t, resetStart.Offset(), channel.GetConfirmed().Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), channel.GetChannelEnd().TotalMsgCnt())
	newReadStart := topic.backend.GetQueueReadStart().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newReadStart.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newReadStart.TotalMsgCnt())

	test.Equal(t, true, newEnd.IsSame(newReadStart))

	// test reopen
	nsqd.CloseExistingTopic("test", 0)
	topic = nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	channel = topic.GetChannel("ch")
	test.NotNil(t, channel)
	newEnd2 := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	newReadStart2 := topic.backend.GetQueueReadStart().(*diskQueueEndInfo)
	test.Equal(t, true, newEnd.IsSame(newEnd2))
	test.Equal(t, true, newReadStart.IsSame(newReadStart2))

	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.TotalMsgCnt()+int64(msgNum), newEnd.TotalMsgCnt())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
		test.Equal(t, msg.Offset+msg.RawMoveSize, channel.GetConfirmed().Offset())
	}

	// reset with old start
	topic.DisableForSlave(false)
	err = topic.ResetBackendWithQueueStartNoLock(int64(resetStart.Offset()), resetStart.TotalMsgCnt())
	test.Nil(t, err)

	topic.EnableForMaster()
	test.Equal(t, resetStart.Offset(), BackendOffset(topic.GetQueueReadStart()))
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newEnd.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newEnd.TotalMsgCnt())
	test.Equal(t, true, newEnd.EndOffset.GreatThan(&oldEnd.EndOffset))
	test.Equal(t, int64(0), newEnd.EndOffset.Pos)
	test.Equal(t, resetStart.Offset(), channel.GetConfirmed().Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), channel.GetChannelEnd().TotalMsgCnt())

	newReadStart = topic.backend.GetQueueReadStart().(*diskQueueEndInfo)
	test.Equal(t, resetStart.Offset(), newReadStart.Offset())
	test.Equal(t, resetStart.TotalMsgCnt(), newReadStart.TotalMsgCnt())
	test.Equal(t, true, newEnd.IsSame(newReadStart))

	// test reopen
	nsqd.CloseExistingTopic("test", 0)
	topic = nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	channel = topic.GetChannel("ch")
	test.NotNil(t, channel)
	newEnd2 = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	newReadStart2 = topic.backend.GetQueueReadStart().(*diskQueueEndInfo)
	test.Equal(t, true, newEnd.IsSame(newEnd2))
	test.Equal(t, true, newReadStart.IsSame(newReadStart2))

	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, _, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, resetStart.TotalMsgCnt()+int64(msgNum), newEnd.TotalMsgCnt())
	for i := 0; i < 100; i++ {
		msg := <-channel.clientMsgChan
		channel.ConfirmBackendQueue(msg)
		test.Equal(t, msg.Offset+msg.RawMoveSize, channel.GetConfirmed().Offset())
	}
}

func TestTopicWriteRollback(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 100
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().UnixNano()
	singleSize := int32(0)
	var qend BackendOffset
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, err := topic.PutMessage(msg)
		test.Nil(t, err)
		msg.Timestamp = time.Now().UnixNano()
		singleSize = msgSize
		qend = dend.Offset()
	}
	topic.ForceFlush()

	// rollback single
	nend := topic.backend.GetQueueWriteEnd()
	if topic.kvTopic != nil {
		_, _, err := topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
		test.Equal(t, nil, err)
	}
	err := topic.RollbackNoLock(qend-BackendOffset(singleSize), 1)
	test.Nil(t, err)
	if topic.kvTopic != nil {
		_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
		test.Equal(t, ErrMsgNotFoundInIndex, err)
	}
	nend = topic.backend.GetQueueWriteEnd()
	test.Equal(t, qend-BackendOffset(singleSize), nend.Offset())
	test.Equal(t, int64(msgNum-1), nend.TotalMsgCnt())
	if topic.kvTopic != nil {
		loffset, lcnt, err := topic.kvTopic.GetTopicMeta()
		test.Nil(t, err)
		test.Equal(t, int64(nend.Offset()), topic.kvTopic.lastOffset)
		test.Equal(t, nend.TotalMsgCnt(), topic.kvTopic.lastCnt)
		test.Equal(t, int64(nend.Offset()), loffset)
		test.Equal(t, nend.TotalMsgCnt(), lcnt)
		// rollback batch
		_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
		test.Nil(t, err)
	}
	err = topic.RollbackNoLock(qend-BackendOffset(singleSize)*10, 9)
	test.Nil(t, err)
	if topic.kvTopic != nil {
		_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
		test.Equal(t, ErrMsgNotFoundInIndex, err)
		_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 9)
		test.Equal(t, ErrMsgNotFoundInIndex, err)
	}
	nend = topic.backend.GetQueueWriteEnd()
	test.Equal(t, qend-BackendOffset(singleSize)*10, nend.Offset())
	test.Equal(t, int64(msgNum-10), nend.TotalMsgCnt())
	if topic.kvTopic != nil {
		loffset, lcnt, err := topic.kvTopic.GetTopicMeta()
		test.Nil(t, err)
		test.Equal(t, int64(nend.Offset()), topic.kvTopic.lastOffset)
		test.Equal(t, nend.TotalMsgCnt(), topic.kvTopic.lastCnt)
		test.Equal(t, int64(nend.Offset()), loffset)
		test.Equal(t, nend.TotalMsgCnt(), lcnt)
	}
}

func TestTopicCheckDiskQueueReadToEndOK(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 100
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 100))
	msg.Timestamp = time.Now().UnixNano()
	singleSize := BackendOffset(0)
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, _, err := topic.PutMessage(msg)
		test.Nil(t, err)
		msg.Timestamp = time.Now().UnixNano()
		singleSize = BackendOffset(msgSize)
	}
	topic.ForceFlush()

	for i := 0; i < msgNum; i++ {
		err := topic.CheckDiskQueueReadToEndOK(int64(i)*int64(singleSize), int64(i), BackendOffset(msgNum)*singleSize)
		test.Nil(t, err)
	}
}

func TestTopicFixCorruptData(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 32
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test-corrupt-header", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	topic2 := nsqd.GetTopic("test-corrupt-tail", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 100
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	channel2 := topic2.GetChannel("ch2")
	test.NotNil(t, channel2)
	channel3 := topic2.GetChannel("ch3")
	test.NotNil(t, channel3)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().UnixNano()
	singleSize := int32(0)
	var qend BackendQueueEnd
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, err := topic.PutMessage(msg)
		test.Nil(t, err)
		msg.Timestamp = time.Now().UnixNano()
		singleSize = msgSize
		qend = dend
		msg.ID = 0
		topic2.PutMessage(msg)
	}
	topic.ForceFlush()
	topic2.ForceFlush()
	channel2.skipChannelToEnd()
	time.Sleep(time.Second)
	test.Equal(t, qend, channel2.GetChannelEnd())
	test.Equal(t, qend, channel3.GetChannelEnd())

	fs, err := os.OpenFile(topic.backend.fileName(0), os.O_RDWR, 0755)
	test.Nil(t, err)
	_, err = fs.WriteAt(make([]byte, singleSize/2), 0)
	test.Nil(t, err)
	fs.Close()
	fs, err = os.OpenFile(topic2.backend.fileName(qend.(*diskQueueEndInfo).EndOffset.FileNum), os.O_RDWR, 0755)
	test.Nil(t, err)
	fs.WriteAt(make([]byte, singleSize), 0)
	fs.Close()

	err = topic.tryFixCorruptData()
	test.Nil(t, err)
	test.Equal(t, qend, topic.GetCommitted())
	time.Sleep(time.Second)
	consumed := channel.GetConfirmed()
	t.Logf("new consumed: %v", consumed)
	test.Equal(t, topic.GetQueueReadStart(), int64(consumed.Offset()))
	test.Equal(t, true, consumed.TotalMsgCnt() > 1)

	err = topic2.tryFixCorruptData()
	test.Nil(t, err)
	t.Logf("new end: %v", topic2.GetCommitted())
	diskEnd := qend.(*diskQueueEndInfo)
	test.Equal(t, diskEnd.EndOffset.FileNum, topic2.GetCommitted().(*diskQueueEndInfo).EndOffset.FileNum)
	test.Equal(t, int64(0), topic2.GetCommitted().(*diskQueueEndInfo).EndOffset.Pos)
	test.Assert(t, diskEnd.Offset() > topic2.GetCommitted().Offset(), "should truncate tail")
	test.Assert(t, diskEnd.TotalMsgCnt() > topic2.GetCommitted().TotalMsgCnt(), "should truncate tail")
	time.Sleep(time.Second)
	consumed = channel2.GetConfirmed()
	t.Logf("new consumed: %v", consumed)
	test.Equal(t, topic2.getCommittedEnd(), channel2.GetConfirmed())
	test.Equal(t, topic2.getCommittedEnd(), channel2.GetChannelEnd())
	consumed = channel3.GetConfirmed()
	t.Logf("new consumed: %v", consumed)
	test.Equal(t, topic2.GetQueueReadStart(), channel3.GetConfirmed().TotalMsgCnt())
	test.Equal(t, topic2.getCommittedEnd(), channel3.GetChannelEnd())
}

type testEndOffset struct {
	offset BackendOffset
}

func (t *testEndOffset) Offset() BackendOffset { return t.offset }

func TestTopicFixKV(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 2
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 2000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 100))
	msg.Timestamp = time.Now().UnixNano()
	singleSize := int32(0)
	var qend BackendOffset
	var resetStart testEndOffset
	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		_, _, msgSize, dend, err := topic.PutMessage(msg)
		test.Nil(t, err)
		msg.Timestamp = time.Now().UnixNano()
		singleSize = msgSize
		qend = dend.Offset()
		if i == msgNum/2 {
			resetStart.offset = (dend.Offset())
		}
	}
	topic.ForceFlush()

	if topic.kvTopic == nil {
		return
	}
	// rollback single
	nend := topic.backend.GetQueueWriteEnd()
	_, _, err := topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Equal(t, nil, err)
	err = topic.kvTopic.ResetBackendEnd(qend-BackendOffset(singleSize), nend.TotalMsgCnt()-1)
	test.Nil(t, err)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Equal(t, ErrMsgNotFoundInIndex, err)
	loffset, lcnt, err := topic.kvTopic.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(nend.Offset()-BackendOffset(singleSize)), topic.kvTopic.lastOffset)
	test.Equal(t, nend.TotalMsgCnt()-1, topic.kvTopic.lastCnt)
	test.Equal(t, int64(nend.Offset()-BackendOffset(singleSize)), loffset)
	test.Equal(t, nend.TotalMsgCnt()-1, lcnt)

	fixedCnt, err := topic.tryFixKVTopic()
	if err != nil {
		t.Log(err.Error())
	}
	test.Nil(t, err)
	test.Equal(t, int64(1), fixedCnt)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Equal(t, nil, err)
	loffset, lcnt, err = topic.kvTopic.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(nend.Offset()), topic.kvTopic.lastOffset)
	test.Equal(t, nend.TotalMsgCnt(), topic.kvTopic.lastCnt)
	test.Equal(t, int64(nend.Offset()), loffset)
	test.Equal(t, nend.TotalMsgCnt(), lcnt)

	// rollback batch
	err = topic.kvTopic.ResetBackendEnd(qend-BackendOffset(singleSize)*10, nend.TotalMsgCnt()-10)
	test.Nil(t, err)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Equal(t, ErrMsgNotFoundInIndex, err)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 9)
	test.Equal(t, ErrMsgNotFoundInIndex, err)

	fixedCnt, err = topic.tryFixKVTopic()
	test.Nil(t, err)
	test.Equal(t, int64(10), fixedCnt)
	loffset, lcnt, err = topic.kvTopic.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(nend.Offset()), topic.kvTopic.lastOffset)
	test.Equal(t, nend.TotalMsgCnt(), topic.kvTopic.lastCnt)
	test.Equal(t, int64(nend.Offset()), loffset)
	test.Equal(t, nend.TotalMsgCnt(), lcnt)

	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Nil(t, err)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 9)
	test.Nil(t, err)
	// fix empty kv
	topic.kvTopic.Empty()
	fixedCnt, err = topic.tryFixKVTopic()
	test.Nil(t, err)
	test.Equal(t, int64(msgNum), fixedCnt)
	// fix which queue start is not 0, and kv is less than start
	topic.backend.CleanOldDataByRetention(&resetStart, false, nend.Offset())
	topic.kvTopic.Empty()
	fixedCnt, err = topic.tryFixKVTopic()
	test.Nil(t, err)
	test.Equal(t, int64(msgNum/2+8), fixedCnt)

	loffset, lcnt, err = topic.kvTopic.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(nend.Offset()), topic.kvTopic.lastOffset)
	test.Equal(t, nend.TotalMsgCnt(), topic.kvTopic.lastCnt)
	test.Equal(t, int64(nend.Offset()), loffset)
	test.Equal(t, nend.TotalMsgCnt(), lcnt)

	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 1)
	test.Nil(t, err)
	_, _, err = topic.kvTopic.GetMsgByCnt(nend.TotalMsgCnt() - 9)
	test.Nil(t, err)
}

func TestTopicFixQueueEnd(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	opts.MaxBytesPerFile = 1024 * 1024
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)

	msgNum := 5000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	msg := NewMessage(0, make([]byte, 1000))
	msg.Timestamp = time.Now().Add(-1 * time.Hour * time.Duration(24*4)).UnixNano()

	topic.PutMessage(msg)
	topic.ForceFlush()
	// test fix end while only one message
	oldEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	// fix end should only work to expand current
	err := topic.TryFixQueueEnd(oldEnd.Offset()/2, oldEnd.TotalMsgCnt()/2)
	test.NotNil(t, err)

	topic.backend.diskWriteEnd = diskQueueEndInfo{}
	err = topic.TryFixQueueEnd(oldEnd.Offset(), oldEnd.TotalMsgCnt())
	test.Nil(t, err)
	newEnd := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)

	test.Equal(t, oldEnd.Offset(), newEnd.Offset())
	test.Equal(t, oldEnd, newEnd)

	var dend BackendQueueEnd
	for i := 0; i <= msgNum; i++ {
		msg.ID = 0
		_, _, _, dend, _ = topic.PutMessage(msg)
		msg.Timestamp = time.Now().Add(-1 * time.Hour * 24 * time.Duration(4-dend.(*diskQueueEndInfo).EndOffset.FileNum)).UnixNano()
	}
	topic.ForceFlush()

	fileNum := topic.backend.diskWriteEnd.EndOffset.FileNum
	test.Equal(t, int64(0), topic.backend.GetQueueReadStart().(*diskQueueEndInfo).EndOffset.FileNum)

	test.Equal(t, true, fileNum >= 4)
	topic.dynamicConf.RetentionDay = 2

	oldEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	// fix end should only work to expand current
	err = topic.TryFixQueueEnd(oldEnd.Offset()/2, oldEnd.TotalMsgCnt()/2)
	test.NotNil(t, err)

	topic.backend.diskWriteEnd = diskQueueEndInfo{}
	err = topic.TryFixQueueEnd(oldEnd.Offset(), oldEnd.TotalMsgCnt())
	test.Nil(t, err)
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)

	test.Equal(t, oldEnd.Offset(), newEnd.Offset())
	test.Equal(t, oldEnd, newEnd)

	// test reopen
	nsqd.CloseExistingTopic("test", 0)
	topic = nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	newEnd2 := topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, true, newEnd.IsSame(newEnd2))

	for i := 0; i < msgNum; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	newEnd = topic.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	test.Equal(t, oldEnd.TotalMsgCnt()+int64(msgNum), newEnd.TotalMsgCnt())
}

func TestTopicWriteConcurrentMulti(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MaxBytesPerFile = 1024 * 2
	opts.KVWriteBufferSize = 1024 * 1024
	opts.KVMaxWriteBufferNumber = 2
	if testing.Verbose() {
		opts.LogLevel = 3
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
		SetLogger(opts.Logger)
	}
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test", 0, false)
	changeDynamicConfAutCommit(topic.dynamicConf)
	topic2 := nsqd.GetTopic("test2", 0, false)
	changeDynamicConfAutCommit(topic2.dynamicConf)

	msgNum := 2000
	channel := topic.GetChannel("ch")
	test.NotNil(t, channel)
	channel2 := topic2.GetChannel("ch")
	test.NotNil(t, channel2)
	var wg sync.WaitGroup
	t.Logf("begin test: %s", time.Now())
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			if topic.kvTopic == nil {
				return
			}
			for time.Since(start) < time.Second*25 {
				topic.kvTopic.kvEng.CompactAllRange()
				time.Sleep(time.Second * 5)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := NewMessage(0, make([]byte, 100))
			msg.Timestamp = time.Now().UnixNano()
			for i := 0; i < msgNum; i++ {
				msg.ID = 0
				msg.Body = []byte("test" + strconv.Itoa(int(i)))
				_, _, _, dend, err := topic.PutMessage(msg)
				test.Nil(t, err)
				if err != nil {
					t.Errorf("failed to write : %s", err)
					return
				}
				msg.Timestamp = time.Now().UnixNano()
				if topic.kvTopic != nil {
					topic.kvTopic.GetMsgByCnt(int64(i))
				}
				if i%100 == 0 {
					topic.ForceFlush()
					if topic.kvTopic != nil {
						topic.Lock()
						topic.kvTopic.ResetBackendEnd(dend.Offset()/2, dend.TotalMsgCnt()/2)
						_, err := topic.tryFixKVTopic()
						topic.Unlock()
						if err != nil {
							t.Errorf("failed to fix kv topic: %s", err)
							return
						}
					}
				}
			}
			topic.ForceFlush()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := NewMessage(0, make([]byte, 100))
			msg.Timestamp = time.Now().UnixNano()
			for i := 0; i < msgNum; i++ {
				msg.ID = 0
				msg.Body = []byte("test2" + strconv.Itoa(int(i)))
				_, _, _, dend, err := topic2.PutMessage(msg)
				test.Nil(t, err)
				if err != nil {
					t.Errorf("failed to write : %s", err)
					return
				}
				msg.Timestamp = time.Now().UnixNano()
				if topic2.kvTopic != nil {
					topic2.kvTopic.GetMsgByCnt(int64(i))
				}
				if i%100 == 0 {
					topic2.ForceFlush()
					if topic2.kvTopic != nil {
						topic2.Lock()
						topic2.kvTopic.ResetBackendEnd(dend.Offset()/2, dend.TotalMsgCnt()/2)
						_, err := topic2.tryFixKVTopic()
						topic2.Unlock()
						if err != nil {
							t.Errorf("failed to fix kv topic: %s", err)
							return
						}
					}
				}
			}
			topic2.ForceFlush()
		}()
	}

	wg.Wait()
	t.Logf("end test: %s", time.Now())
}

func benchmarkTopicPut(b *testing.B, size int, useExt bool) {
	b.StopTimer()
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	msg := NewMessage(0, make([]byte, size))
	if useExt {
		msg = NewMessageWithExt(0, make([]byte, size), ext.JSON_HEADER_EXT_VER, []byte("test"))
	}
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	b.StartTimer()

	topic := nsqd.GetTopic(topicName, 0, false)
	topic.SetDynamicInfo(TopicDynamicConf{
		Ext: useExt,
	}, nil)
	for i := 0; i <= b.N; i++ {
		msg.ID = 0
		topic.PutMessage(msg)
	}
}

func BenchmarkTopicPut16(b *testing.B) {
	benchmarkTopicPut(b, 16, false)
}

func BenchmarkTopicPut128(b *testing.B) {
	benchmarkTopicPut(b, 128, false)
}

func BenchmarkTopicPut1024(b *testing.B) {
	benchmarkTopicPut(b, 1024, false)
}
func BenchmarkTopicExtPut16(b *testing.B) {
	benchmarkTopicPut(b, 16, true)
}

func BenchmarkTopicExtPut128(b *testing.B) {
	benchmarkTopicPut(b, 128, true)
}

func BenchmarkTopicExtPut1024(b *testing.B) {
	benchmarkTopicPut(b, 1024, true)
}
func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	opts := NewOptions()
	opts.Logger = newTestLogger(b)
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	nsqd.GetTopic(topicName, 0, false).GetChannel(channelName)
	b.StartTimer()
	msg := NewMessage(0, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName, 0, false)
		msg.ID = 0
		topic.PutMessage(msg)
	}
}
