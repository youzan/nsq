package nsqd

import (
	"bytes"
	"encoding/binary"
	//"github.com/youzan/nsq/internal/levellogger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/test"
)

func TestDelayQueuePutChannelDelayed(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	var end BackendOffset
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, dend, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
		test.Equal(t, true, dq.IsChannelMessageDelayed(msg.DelayedOrigID, "test"))
		end = dend.Offset()
	}
	synced, err := dq.GetSyncedOffset()
	test.Nil(t, err)
	test.Equal(t, end, synced)
	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)
	dq.Delete()
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)
	_, err = os.Stat(path.Join(dq.dataPath, getDelayQueueDBName(dq.tname, dq.partition)))
	test.NotNil(t, err)
}

// put raw and message mixed, put ext and non-ext mixed.
func TestDelayQueuePutRawChannelDelayed(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dqRaw, err := NewDelayQueue("test_raw", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dqRaw.Close()
	cnt := 20
	rawOffset := BackendOffset(0)

	tag := createJsonHeaderExtWithTag(t, "tagname")
	extCnt := 0
	// put raw data message and normal message
	// and then switch to ext and do that again
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		msg.ExtVer = tag.ExtVersion()
		msg.ExtBytes = tag.GetBytes()
		if dqRaw.IsExt() {
			extCnt++
		}

		wsize := int32(0)
		if i%2 == 0 {
			_, _, wsize, _, err = dqRaw.PutDelayMessage(msg)
			test.Nil(t, err)
			test.Equal(t, true, dqRaw.IsChannelMessageDelayed(msg.DelayedOrigID, "test"))
			rawOffset += BackendOffset(wsize)
		} else {
			buf := bytes.Buffer{}
			_, err = msg.WriteDelayedTo(&buf, dqRaw.IsExt())
			test.Nil(t, err)
			rawData := make([]byte, 4+len(buf.Bytes()))
			binary.BigEndian.PutUint32(rawData[:4], uint32(len(buf.Bytes())))
			copy(rawData[4:], buf.Bytes())
			if i > cnt/2 {
				// notice we set ext after build raw message, so we write a non-ext message to extend topic
				dqRaw.setExt()
			}
			wsize = int32(len(rawData))
			_, err = dqRaw.PutRawDataOnReplica(rawData, rawOffset, int64(wsize), 1)
			test.Nil(t, err)
			test.Equal(t, true, dqRaw.IsChannelMessageDelayed(msg.DelayedOrigID, "test"))
			rawOffset += BackendOffset(wsize)
		}
	}
	synced, err := dqRaw.GetSyncedOffset()
	test.Nil(t, err)
	test.Equal(t, rawOffset, synced)
	newCnt, _ := dqRaw.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	_, err = os.Stat(dqRaw.dataPath)
	test.Nil(t, err)
	ret := make([]Message, cnt)
	time.Sleep(time.Second)
	test.Equal(t, cnt/2-2, extCnt)
	for {
		n, err := dqRaw.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
			if m.ExtVer == tag.ExtVersion() {
				extCnt--
				test.Equal(t, m.ExtBytes, tag.GetBytes())
			}
		}
		if n == 0 {
			test.Assert(t, false, "should have recent timeout messages")
			break
		}
		if n >= cnt {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	test.Equal(t, 0, extCnt)
}

func createJsonHeaderExtWithTag(t *testing.T, tag string) *ext.JsonHeaderExt {
	jsonHeader := make(map[string]interface{})
	jsonHeader[ext.CLIENT_DISPATCH_TAG_KEY] = tag
	jsonHeaderBytes, err := json.Marshal(&jsonHeader)
	test.Nil(t, err)
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes(jsonHeaderBytes)
	return jhe
}

func TestDelayQueueWithExtPutChannelDelayed(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dq, err := NewDelayQueue("test-ext", 0, tmpDir, opts, nil, true)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	var end BackendOffset
	tagName := "exttagdata"
	tag := createJsonHeaderExtWithTag(t, tagName)
	test.Nil(t, err)
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.ExtVer = tag.ExtVersion()
		msg.ExtBytes = tag.GetBytes()
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Millisecond).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, dend, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
		test.Equal(t, true, dq.IsChannelMessageDelayed(msg.DelayedOrigID, "test"))
		end = dend.Offset()
	}
	synced, err := dq.GetSyncedOffset()
	test.Nil(t, err)
	test.Equal(t, end, synced)
	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)

	time.Sleep(time.Second)
	ret := make([]Message, cnt)
	n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
	test.Nil(t, err)
	test.Equal(t, cnt, n)
	for _, m := range ret {
		test.Equal(t, tag.ExtVersion(), m.ExtVer)
		test.Equal(t, tag.GetBytes(), m.ExtBytes)
	}

	dq.Delete()
	_, err = os.Stat(dq.dataPath)
	test.Nil(t, err)
	_, err = os.Stat(path.Join(dq.dataPath, getDelayQueueDBName(dq.tname, dq.partition)))
	test.NotNil(t, err)
}

func TestDelayQueueEmptyAll(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	oldMaxBatch := txMaxBatch
	txMaxBatch = 100
	defer func() {
		txMaxBatch = oldMaxBatch
	}()
	cnt := txMaxBatch + 2
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		test.Nil(t, err)
		time.Sleep(time.Millisecond * 100)
	}

	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))
	dq.EmptyDelayedChannel("test2")

	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, 0, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	dq.EmptyDelayedChannel("test")
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, 0, int(newCnt))
}

func TestDelayQueueEmptyUntil(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	var middle *Message
	middleIndex := 0
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
		if i == cnt/2 {
			middle = msg
			middleIndex = i
		}

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		test.Nil(t, err)
		time.Sleep(time.Millisecond * 100)
	}

	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))
	dq.emptyDelayedUntil(ChannelDelayed, middle.DelayedTs, middle.ID, "test")
	// test empty until should keep the until cursor
	recent, _, _ := dq.GetOldestConsumedState([]string{"test"}, true)
	test.Equal(t, 1, len(recent))
	_, ts, id, ch, err := decodeDelayedMsgDBKey(recent[0])
	test.Nil(t, err)
	test.Equal(t, middle.DelayedChannel, ch)
	test.Equal(t, middle.ID, id)
	test.Equal(t, middle.DelayedTs, ts)

	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt-middleIndex, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))
	dq.EmptyDelayedChannel("test")
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, 0, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))
}

func TestDelayQueuePeekRecent(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		test.Nil(t, err)
		time.Sleep(time.Millisecond * 100)
	}

	ret := make([]Message, cnt)
	for {
		n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
		}

		n, err = dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test2")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test2", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
		}

		if n >= cnt {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func TestDelayQueueConfirmMsg(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Second).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		test.Nil(t, err)
		time.Sleep(time.Millisecond * 100)
	}

	ret := make([]Message, cnt)
	for {
		n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
			oldCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")

			origID := m.DelayedOrigID
			test.Equal(t, true, dq.IsChannelMessageDelayed(origID, "test"))
			m.DelayedOrigID = m.ID
			dq.ConfirmedMessage(&m)
			test.Equal(t, false, dq.IsChannelMessageDelayed(origID, "test"))
			newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
			test.Equal(t, oldCnt-1, newCnt)
			cursorList, cntList, channelCntList := dq.GetOldestConsumedState([]string{"test"}, true)
			for _, v := range cntList {
				test.Equal(t, uint64(0), v)
			}
			test.Equal(t, 1, len(channelCntList))
			test.Equal(t, uint64(newCnt), channelCntList["test"])
			for _, c := range cursorList {
				dt, ts, id, ch, err := decodeDelayedMsgDBKey(c)
				test.Nil(t, err)
				if dt == ChannelDelayed {
					test.Equal(t, "test", ch)
					test.Equal(t, true, ts > m.DelayedTs)
					t.Logf("confirmed: %v, oldest ts: %v\n", m.DelayedTs, ts)
					//test.Equal(t, true, ts < m.DelayedTs+int64(time.Millisecond*210))
					test.Equal(t, true, id > m.ID)
				}
			}
		}

		n, err = dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test2")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test2", m.DelayedChannel)
			test.Equal(t, true, m.DelayedTs <= time.Now().UnixNano())
			oldCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
			origID := m.DelayedOrigID
			test.Equal(t, true, dq.IsChannelMessageDelayed(origID, "test2"))
			m.DelayedOrigID = m.ID
			dq.ConfirmedMessage(&m)
			test.Equal(t, false, dq.IsChannelMessageDelayed(origID, "test2"))
			newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
			test.Equal(t, oldCnt-1, newCnt)

			cursorList, cntList, channelCntList := dq.GetOldestConsumedState([]string{"test2"}, true)
			for _, v := range cntList {
				test.Equal(t, uint64(0), v)
			}
			test.Equal(t, 1, len(channelCntList))
			test.Equal(t, uint64(newCnt), channelCntList["test2"])
			for _, c := range cursorList {
				dt, ts, id, ch, err := decodeDelayedMsgDBKey(c)
				test.Nil(t, err)
				if dt == ChannelDelayed {
					test.Equal(t, "test2", ch)
					test.Equal(t, true, ts > m.DelayedTs)
					//test.Equal(t, true, ts < m.DelayedTs+int64(time.Millisecond*210))
					test.Equal(t, true, id > m.ID)
				}
			}
		}

		if n, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test2"); n <= 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

}

func TestDelayQueueBackupRestore(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test-backup", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := 10
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Millisecond).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)

		msg = NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Millisecond).UnixNano()
		msg.DelayedChannel = "test2"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err = dq.PutDelayMessage(msg)
		test.Nil(t, err)
		time.Sleep(time.Millisecond * 100)
	}

	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))
	dq.getStore().Sync()

	oldDBStat, err := os.Stat(dq.getStore().Path())
	test.Nil(t, err)

	f, err := os.Create(path.Join(tmpDir, "backuped.file"))
	test.Nil(t, err)
	fsize, err := dq.BackupKVStoreTo(f)
	test.Nil(t, err)
	f.Sync()
	f.Close()
	stat, err := os.Stat(path.Join(tmpDir, "backuped.file"))
	test.Equal(t, fsize, stat.Size())
	f, err = os.OpenFile(path.Join(tmpDir, "backuped.file"), os.O_RDWR, 0666)
	test.Nil(t, err)
	err = dq.RestoreKVStoreFrom(f)
	test.Nil(t, err)

	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))
	newCnt, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test2")
	test.Equal(t, cnt, int(newCnt))

	dbSize, _ := dq.GetDBSize()
	test.Equal(t, stat.Size()-8, dbSize)

	ret := make([]Message, cnt)
	for {
		n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, "body", string(m.Body))
		}
		n2, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test2")
		test.Nil(t, err)
		for _, m := range ret[:n2] {
			test.Equal(t, "test2", m.DelayedChannel)
			test.Equal(t, "body", string(m.Body))
		}
		if n+n2 >= cnt*2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	dbStat, err := os.Stat(dq.getStore().Path())
	test.Nil(t, err)
	t.Logf("old %v, new %v\n", oldDBStat, dbStat)
	test.Equal(t, true, oldDBStat.Size() >= dbStat.Size())
}

func TestDelayQueueCompactStore(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-delay-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
	SetLogger(opts.Logger)

	dq, err := NewDelayQueue("test-compact", 0, tmpDir, opts, nil, false)
	test.Nil(t, err)
	defer dq.Close()
	cnt := CompactCntThreshold + 1
	for i := 0; i < cnt; i++ {
		msg := NewMessage(0, []byte("body"))
		msg.DelayedType = ChannelDelayed
		msg.DelayedTs = time.Now().Add(time.Millisecond).UnixNano()
		msg.DelayedChannel = "test"
		msg.DelayedOrigID = MessageID(i + 1)
		_, _, _, _, err := dq.PutDelayMessage(msg)
		test.Nil(t, err)
	}
	newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, cnt, int(newCnt))

	beforeCompact, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	fi, err := os.Stat(dq.getStore().Path())
	test.Nil(t, err)
	CompactThreshold = 1024 * 8
	// first compact is ignored
	err = dq.compactStore(false)
	test.Nil(t, err)
	fi2, err := os.Stat(dq.getStore().Path())
	t.Log(fi)
	t.Log(fi2)
	afterCompact, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, beforeCompact, afterCompact)
	test.Equal(t, true, fi2.Size() == fi.Size())

	ret := make([]Message, 100)
	done := false
	for !done {
		n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			origID := m.DelayedOrigID
			test.Equal(t, true, dq.IsChannelMessageDelayed(origID, "test"))
			m.DelayedOrigID = m.ID
			dq.ConfirmedMessage(&m)
			test.Equal(t, false, dq.IsChannelMessageDelayed(origID, "test"))
			newCnt, _ := dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
			if int(newCnt) < cnt/10 {
				done = true
				break
			}
		}
	}
	dq.getStore().Sync()
	beforeCompact, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, true, int(beforeCompact) <= cnt/10)

	fi, err = os.Stat(dq.getStore().Path())
	test.Nil(t, err)
	err = dq.compactStore(false)
	test.Nil(t, err)
	fi2, err = os.Stat(dq.getStore().Path())
	test.Nil(t, err)
	t.Log(fi)
	t.Log(fi2)
	afterCompact, _ = dq.GetCurrentDelayedCnt(ChannelDelayed, "test")
	test.Equal(t, beforeCompact, afterCompact)
	test.Equal(t, true, fi2.Size() < fi.Size())

	ret = make([]Message, beforeCompact)
	for {
		n, err := dq.PeekRecentChannelTimeout(time.Now().UnixNano(), ret, "test")
		test.Nil(t, err)
		for _, m := range ret[:n] {
			test.Equal(t, "test", m.DelayedChannel)
			test.Equal(t, "body", string(m.Body))
		}
		if uint64(n) >= beforeCompact {
			break
		}
		time.Sleep(time.Millisecond)
	}
}
