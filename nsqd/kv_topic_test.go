package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/test"
)

func testKVTopicWriteRead(t *testing.T, replica bool) {
	opts := NewOptions()
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	opts.DataPath = tmpDir
	defer os.RemoveAll(tmpDir)
	kvt := NewKVTopicWithExt("test-kv-topic", 0, true, opts)
	defer kvt.kvEng.CloseAll()
	defer kvt.Close()

	test.NotNil(t, kvt)
	offset, totalCnt, err := kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(0), offset)
	test.Equal(t, int64(0), totalCnt)
	singleSize := int32(47)
	msgs := make([]*Message, 0)
	msgCnt := 6
	rawMsgData := bytes.NewBuffer(make([]byte, 0, int(singleSize)*msgCnt))
	for i := 0; i < msgCnt; i++ {
		m := NewMessageWithExt(MessageID(i+1), make([]byte, 10), ext.JSON_HEADER_EXT_VER, []byte("tes"+strconv.Itoa(i)))
		if i <= msgCnt-2 {
			m.TraceID = uint64(i + 1)
		}
		msgs = append(msgs, m)
		binary.Write(rawMsgData, binary.BigEndian, singleSize-4)
		m.WriteTo(rawMsgData, kvt.IsExt())
	}
	test.Equal(t, int(singleSize)*msgCnt, len(rawMsgData.Bytes()))
	var wsize int32
	var end BackendQueueEnd
	if replica {
		end, err = kvt.PutRawDataOnReplica(rawMsgData.Bytes()[:singleSize], BackendOffset(kvt.lastOffset), int64(singleSize), 1)
	} else {
		wsize, end, err = kvt.PutMessage(msgs[0])
	}
	t.Log(err)
	test.Nil(t, err)
	if !replica {
		test.Equal(t, singleSize, wsize)
	}
	test.Equal(t, end.Offset(), BackendOffset(singleSize))
	test.Equal(t, end.TotalMsgCnt(), int64(1))

	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(singleSize), offset)
	test.Equal(t, int64(1), totalCnt)

	var wfirst BackendOffset
	if replica {
		end, err = kvt.PutRawDataOnReplica(rawMsgData.Bytes()[singleSize:], BackendOffset(kvt.lastOffset), int64(singleSize*int32(msgCnt-1)), int32(msgCnt-1))
	} else {
		wfirst, wsize, end, err = kvt.PutMessages(msgs[1:])
	}
	test.Nil(t, err)
	if !replica {
		test.Equal(t, int32(singleSize*int32(msgCnt-1)), wsize)
		test.Equal(t, BackendOffset(singleSize), wfirst)
	}
	test.Equal(t, BackendOffset(int(singleSize)*msgCnt), end.Offset())
	test.Equal(t, int64(msgCnt), end.TotalMsgCnt())
	test.Equal(t, int64(end.Offset()), kvt.lastOffset)
	test.Equal(t, end.TotalMsgCnt(), kvt.lastCnt)

	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(msgCnt*int(singleSize)), offset)
	test.Equal(t, int64(msgCnt), totalCnt)

	// test reopen
	kvt.Close()
	kvt.kvEng.CloseAll()
	kvt = NewKVTopicWithExt("test-kv-topic", 0, true, opts)
	test.NotNil(t, kvt)
	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(msgCnt*int(singleSize)), offset)
	test.Equal(t, int64(msgCnt), totalCnt)

	for i := 0; i < msgCnt; i++ {
		dbMsg, err := kvt.GetMsgByID(MessageID(i + 1))
		test.Nil(t, err)
		test.Equal(t, msgs[i].ID, dbMsg.ID)
		test.Equal(t, msgs[i].Body, dbMsg.Body)
		test.Equal(t, msgs[i].TraceID, dbMsg.TraceID)
		test.Equal(t, msgs[i].ExtBytes, dbMsg.ExtBytes)
		dbMsg, err = kvt.GetMsgByOffset(int64(i * int(singleSize)))
		test.Nil(t, err)
		test.Equal(t, msgs[i].ID, dbMsg.ID)
		test.Equal(t, msgs[i].Body, dbMsg.Body)
		test.Equal(t, msgs[i].TraceID, dbMsg.TraceID)
		test.Equal(t, msgs[i].ExtBytes, dbMsg.ExtBytes)
		dbMsg, _, err = kvt.GetMsgByCnt(int64(i))
		test.Nil(t, err)
		test.Equal(t, msgs[i].ID, dbMsg.ID)
		test.Equal(t, msgs[i].Body, dbMsg.Body)
		test.Equal(t, msgs[i].TraceID, dbMsg.TraceID)
		test.Equal(t, msgs[i].ExtBytes, dbMsg.ExtBytes)
		dbmsgs, err := kvt.GetMsgByTraceID(uint64(i+1), 2)
		test.Nil(t, err)
		if i <= msgCnt-2 {
			test.Equal(t, 1, len(dbmsgs))
			test.Equal(t, msgs[i].ID, dbmsgs[0].ID)
			test.Equal(t, msgs[i].Body, dbmsgs[0].Body)
			test.Equal(t, msgs[i].TraceID, dbmsgs[0].TraceID)
			test.Equal(t, msgs[i].ExtBytes, dbmsgs[0].ExtBytes)
		} else {
			test.Equal(t, 0, len(dbmsgs))
		}
	}

	for i := 0; i < msgCnt+2; i++ {
		for j := 1; j < msgCnt+2; j++ {
			dbmsgs, err := kvt.PullMsgByCntFrom(int64(i), int64(j))
			test.Nil(t, err)
			expectCnt := 0
			if i < msgCnt {
				if i+j < msgCnt {
					expectCnt = j
				} else {
					expectCnt = msgCnt - i
				}
			}
			t.Logf("begin cnt: %v, limit: %v", i, j)
			test.Equal(t, expectCnt, len(dbmsgs))
			for mi, m := range dbmsgs {
				test.Equal(t, m.ID, msgs[i+mi].ID)
				test.Equal(t, m.Body, msgs[i+mi].Body)
				test.Equal(t, m.TraceID, msgs[i+mi].TraceID)
				test.Equal(t, m.ExtBytes, msgs[i+mi].ExtBytes)
			}
		}
	}
	kvt.Empty()
	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(0), offset)
	test.Equal(t, int64(0), totalCnt)
	dbmsgs, err := kvt.PullMsgByCntFrom(0, int64(msgCnt))
	test.Nil(t, err)
	test.Equal(t, 0, len(dbmsgs))
}

func TestKVTopicWriteRead(t *testing.T) {
	testKVTopicWriteRead(t, false)
}

func TestKVTopicWriteReadReplica(t *testing.T) {
	testKVTopicWriteRead(t, true)
}

func TestKVTopicWriteRawData(t *testing.T) {
	opts := NewOptions()
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	opts.DataPath = tmpDir
	defer os.RemoveAll(tmpDir)
	kvt := NewKVTopicWithExt("test-kv-topic-raw", 0, true, opts)
	defer kvt.kvEng.CloseAll()
	defer kvt.Close()
	test.NotNil(t, kvt)
	singleSize := int32(47)
	m := NewMessageWithExt(1, make([]byte, 10), ext.JSON_HEADER_EXT_VER, []byte("tes1"))
	m.TraceID = 1
	var end BackendQueueEnd
	b := bytes.NewBuffer(make([]byte, 0, singleSize))
	binary.Write(b, binary.BigEndian, singleSize-4)
	m.WriteTo(b, true)
	end, err = kvt.PutRawDataOnReplica(b.Bytes(), BackendOffset(kvt.lastOffset), int64(singleSize), 1)
	t.Log(err)
	test.Nil(t, err)
	test.Equal(t, end.Offset(), BackendOffset(singleSize))
	test.Equal(t, end.TotalMsgCnt(), int64(1))
	msgs := make([]*Message, 0)
	m2 := NewMessageWithExt(2, make([]byte, 10), ext.JSON_HEADER_EXT_VER, []byte("tes2"))
	m2.TraceID = 2
	m3 := NewMessageWithExt(3, make([]byte, 10), ext.JSON_HEADER_EXT_VER, []byte("tes3"))
	msgs = append(msgs, m2, m3)
	b.Reset()
	binary.Write(b, binary.BigEndian, singleSize-4)
	m2.WriteTo(b, true)
	binary.Write(b, binary.BigEndian, singleSize-4)
	m3.WriteTo(b, true)
	end, err = kvt.PutRawDataOnReplica(b.Bytes(), BackendOffset(kvt.lastOffset), int64(singleSize*2), 2)
	test.Nil(t, err)

	test.Equal(t, BackendOffset(singleSize*3), end.Offset())
	test.Equal(t, int64(3), end.TotalMsgCnt())
	test.Equal(t, int64(end.Offset()), kvt.lastOffset)
	test.Equal(t, end.TotalMsgCnt(), kvt.lastCnt)

	dbMsg, err := kvt.GetMsgByID(MessageID(1))
	test.Nil(t, err)
	test.Equal(t, m.ID, dbMsg.ID)
	test.Equal(t, m.Body, dbMsg.Body)
	test.Equal(t, m.TraceID, dbMsg.TraceID)
	test.Equal(t, m.ExtBytes, dbMsg.ExtBytes)
	dbMsg, err = kvt.GetMsgByID(MessageID(2))
	test.Nil(t, err)
	test.Equal(t, m2.ID, dbMsg.ID)
	test.Equal(t, m2.Body, dbMsg.Body)
	test.Equal(t, m2.TraceID, dbMsg.TraceID)
	test.Equal(t, m2.ExtBytes, dbMsg.ExtBytes)
	dbMsg, err = kvt.GetMsgByID(MessageID(3))
	test.Nil(t, err)
	test.Equal(t, m3.ID, dbMsg.ID)
	test.Equal(t, m3.Body, dbMsg.Body)
	test.Equal(t, m3.TraceID, dbMsg.TraceID)
	test.Equal(t, m3.ExtBytes, dbMsg.ExtBytes)

	offset, totalCnt, err := kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(3*singleSize), offset)
	test.Equal(t, int64(3), totalCnt)
	// test reopen
	kvt.Close()
	kvt.kvEng.CloseAll()
	kvt = NewKVTopic("test-kv-topic-raw", 0, opts)
	test.NotNil(t, kvt)
	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(3*singleSize), offset)
	test.Equal(t, int64(3), totalCnt)
}

func TestKVTopicResetStartEnd(t *testing.T) {
	opts := NewOptions()
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	opts.DataPath = tmpDir
	defer os.RemoveAll(tmpDir)
	kvt := NewKVTopicWithExt("test-kv-topic-reset", 0, true, opts)
	defer kvt.Close()
	test.NotNil(t, kvt)
	singleSize := int32(47)
	msgs := make([]*Message, 0)
	msgCnt := 6
	for i := 0; i < msgCnt; i++ {
		m := NewMessageWithExt(MessageID(i+1), make([]byte, 10), ext.JSON_HEADER_EXT_VER, []byte("tes"+strconv.Itoa(i)))
		if i <= msgCnt-2 {
			m.TraceID = uint64(i + 1)
		}
		msgs = append(msgs, m)
	}
	var wsize int32
	var end BackendQueueEnd
	wsize, end, err = kvt.PutMessage(msgs[0])
	test.Nil(t, err)
	test.Equal(t, singleSize, wsize)
	test.Equal(t, end.Offset(), BackendOffset(singleSize))
	test.Equal(t, end.TotalMsgCnt(), int64(1))

	_, wsize, end, err = kvt.PutMessages(msgs[1:])
	test.Nil(t, err)
	test.Equal(t, BackendOffset(int(singleSize)*msgCnt), end.Offset())
	test.Equal(t, int64(msgCnt), end.TotalMsgCnt())

	offset, totalCnt, err := kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64(msgCnt*int(singleSize)), offset)
	test.Equal(t, int64(msgCnt), totalCnt)

	err = kvt.CleanBackendWithQueueStart(int64(singleSize) * 2)
	test.Nil(t, err)
	err = kvt.ResetBackendEnd(BackendOffset(singleSize)*BackendOffset(msgCnt-2), int64(msgCnt-2))
	test.Nil(t, err)

	offset, totalCnt, err = kvt.GetTopicMeta()
	test.Nil(t, err)
	test.Equal(t, int64((msgCnt-2)*int(singleSize)), offset)
	test.Equal(t, int64(msgCnt-2), totalCnt)
	dbmsgs, err := kvt.PullMsgByCntFrom(2, int64(msgCnt))
	test.Nil(t, err)
	test.Equal(t, int(msgCnt)-4, len(dbmsgs))
	for i := 0; i < msgCnt; i++ {
		if i < msgCnt-4 {
			test.Equal(t, msgs[i+2].ID, dbmsgs[i].ID)
			test.Equal(t, msgs[i+2].TraceID, dbmsgs[i].TraceID)
		}
		dbmsg, err := kvt.GetMsgByOffset(int64(i * int(singleSize)))
		t.Logf("%v:%v", i, dbmsg)
		if i < 2 || i >= msgCnt-2 {
			test.Equal(t, ErrMsgNotFoundInIndex, err)
		} else {
			test.Nil(t, err)
			test.Equal(t, msgs[i].ID, dbmsg.ID)
			test.Equal(t, msgs[i].TraceID, dbmsg.TraceID)
		}
		dbmsg, err = kvt.GetMsgByID(msgs[i].ID)
		t.Logf("%v:%v", i, dbmsg)
		if i < 2 || i >= msgCnt-2 {
			test.Equal(t, ErrMsgNotFoundInIndex, err)
		} else {
			test.Nil(t, err)
			test.Equal(t, msgs[i].ID, dbmsg.ID)
			test.Equal(t, msgs[i].TraceID, dbmsg.TraceID)
		}
		dbmsg, _, err = kvt.GetMsgByCnt(int64(i))
		t.Logf("%v:%v", i, dbmsg)
		if i < 2 || i >= msgCnt-2 {
			test.Equal(t, ErrMsgNotFoundInIndex, err)
		} else {
			test.Nil(t, err)
			test.Equal(t, msgs[i].ID, dbmsg.ID)
			test.Equal(t, msgs[i].TraceID, dbmsg.TraceID)
		}
	}
}
