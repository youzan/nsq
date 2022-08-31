package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/bolt"
	"github.com/youzan/nsq/internal/levellogger"
)

var (
	syncedOffsetKey       = []byte("synced_offset")
	bucketDelayedMsg      = []byte("delayed_message")
	bucketDelayedMsgIndex = []byte("delayed_message_index")
	bucketMeta            = []byte("meta")
	CompactThreshold      = 1024 * 1024 * 32
	// should equal or larger than the max msg size
	compactSingleAvgSize         = 1024 * 1024
	errBucketKeyNotFound         = errors.New("bucket key not found")
	txMaxBatch                   = 5000
	largeDBSize            int64 = 1024 * 1024 * 1024 * 4
	errDBSizeTooLarge            = errors.New("db size too large")
	errTooMuchRunningEmpty       = errors.New("too much running empty")
	errOnlyPartialEmpty          = errors.New("only partial empty")
	testEmptyDelay               = false
)

const (
	MinDelayedType      = 1
	ChannelDelayed      = 1
	PubDelayed          = 2
	TransactionDelayed  = 3
	MaxDelayedType      = 4
	TxMaxSize           = 65536
	CompactCntThreshold = 20000
	maxEmptyRunning     = 5
)

type searchedItem struct {
	delayedTs int64
	delayedID MessageID
	ch        string
}

type RecentKeyList [][]byte

func writeDelayedMessageToBackendWithCheck(buf *bytes.Buffer, msg *Message,
	checkSize int64, bq *diskQueueWriter,
	isExt bool) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	wsize, err := msg.WriteDelayedTo(buf, isExt)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	if checkSize > 0 && checkSize != wsize+4 {
		return 0, 0, diskQueueEndInfo{}, fmt.Errorf("write message size mismatch: %v vs %v", checkSize, wsize+4)
	}
	return bq.PutV2(buf.Bytes())
}

func IsValidDelayedMessage(m *Message) bool {
	if m.DelayedType == ChannelDelayed {
		return m.DelayedOrigID > 0 && len(m.DelayedChannel) > 0 && m.DelayedTs > 0
	} else if m.DelayedType == PubDelayed {
		return m.DelayedTs > 0
	} else if m.DelayedType == TransactionDelayed {
		return true
	}
	return false
}

func decodeDelayedMsgDBIndexValue(b []byte) (int64, MessageID, error) {
	if len(b) != 1+8+8 {
		return 0, 0, errors.New("invalid data")
	}
	if b[0] != byte(1) {
		return 0, 0, errors.New("invalid data")
	}
	ts := binary.BigEndian.Uint64(b[1 : 1+8])
	id := binary.BigEndian.Uint64(b[1+8 : 1+8+8])
	return int64(ts), MessageID(id), nil
}

func getDelayedMsgDBIndexValue(ts int64, id MessageID) []byte {
	d := make([]byte, 1+8+8)
	pos := 0
	d[0] = byte(1)
	pos++
	binary.BigEndian.PutUint64(d[pos:pos+8], uint64(ts))
	pos += 8
	binary.BigEndian.PutUint64(d[pos:pos+8], uint64(id))
	return d
}

func getDelayedMsgDBKey(dt int, ch string, ts int64, id MessageID) []byte {
	msgKey := make([]byte, len(ch)+2+1+2+8+8)
	binary.BigEndian.PutUint16(msgKey[:2], uint16(dt))
	pos := 2
	msgKey[pos] = '-'
	pos++
	binary.BigEndian.PutUint16(msgKey[pos:pos+2], uint16(len(ch)))
	pos += 2
	copy(msgKey[pos:pos+len(ch)], []byte(ch))
	pos += len(ch)
	binary.BigEndian.PutUint64(msgKey[pos:pos+8], uint64(ts))
	pos += 8
	binary.BigEndian.PutUint64(msgKey[pos:pos+8], uint64(id))
	return msgKey
}

func decodeDelayedMsgDBKey(b []byte) (uint16, int64, MessageID, string, error) {
	if len(b) < 2+1+2+8+8 {
		return 0, 0, 0, "", errors.New("invalid buffer length")
	}
	dt := binary.BigEndian.Uint16(b[:2])
	pos := 2
	pos++
	chLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if len(b) < pos+chLen {
		return 0, 0, 0, "", errors.New("invalid buffer length")
	}
	ch := b[pos : pos+chLen]
	pos += chLen
	ts := int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	id := int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	return dt, ts, MessageID(id), string(ch), nil
}

func getDelayedMsgDBIndexKey(dt int, ch string, id MessageID) []byte {
	msgKey := make([]byte, len(ch)+2+1+2+8)
	binary.BigEndian.PutUint16(msgKey[:2], uint16(dt))
	pos := 2
	msgKey[pos] = '-'
	pos++
	binary.BigEndian.PutUint16(msgKey[pos:pos+2], uint16(len(ch)))
	pos += 2
	copy(msgKey[pos:pos+len(ch)], []byte(ch))
	pos += len(ch)
	binary.BigEndian.PutUint64(msgKey[pos:pos+8], uint64(id))
	return msgKey
}

func decodeDelayedMsgDBIndexKey(b []byte) (uint16, MessageID, string, error) {
	if len(b) < 2+1+2+8 {
		return 0, 0, "", errors.New("invalid buffer length")
	}
	dt := binary.BigEndian.Uint16(b[:2])
	pos := 2
	pos++
	chLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if len(b) < pos+chLen {
		return 0, 0, "", errors.New("invalid buffer length")
	}
	ch := b[pos : pos+chLen]
	pos += chLen
	id := int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	return dt, MessageID(id), string(ch), nil
}

func getDelayedMsgDBPrefixKey(dt int, ch string) []byte {
	msgKey := make([]byte, len(ch)+2+1+2)
	binary.BigEndian.PutUint16(msgKey[:2], uint16(dt))
	pos := 2
	msgKey[pos] = '-'
	pos++
	binary.BigEndian.PutUint16(msgKey[pos:pos+2], uint16(len(ch)))
	pos += 2
	copy(msgKey[pos:pos+len(ch)], []byte(ch))
	return msgKey
}

func getDelayedMsgDBPrefixKeyWithTs(dt int, ch string, ts int64) []byte {
	msgKey := make([]byte, len(ch)+2+1+2+8)
	binary.BigEndian.PutUint16(msgKey[:2], uint16(dt))
	pos := 2
	msgKey[pos] = '-'
	pos++
	binary.BigEndian.PutUint16(msgKey[pos:pos+2], uint16(len(ch)))
	pos += 2
	copy(msgKey[pos:pos+len(ch)], []byte(ch))
	pos += len(ch)
	binary.BigEndian.PutUint64(msgKey[pos:pos+8], uint64(ts))
	return msgKey
}

func deleteMsgIndex(msgData []byte, tx *bolt.Tx, isExt bool) error {
	m, err := DecodeDelayedMessage(msgData, isExt)
	if err != nil {
		nsqLog.LogErrorf("failed to decode delayed message: %v, %v", msgData, err)
		return err
	}
	msgIndexKey := getDelayedMsgDBIndexKey(int(m.DelayedType), m.DelayedChannel, m.DelayedOrigID)
	b := tx.Bucket(bucketDelayedMsgIndex)
	v := b.Get(msgIndexKey)

	if v != nil {
		ts, _, err := decodeDelayedMsgDBIndexValue(v)
		if err != nil {
			return err
		}
		if m.DelayedTs == ts {
			err = b.Delete(msgIndexKey)
			if err != nil {
				nsqLog.Infof("failed to delete delayed index : %v", msgIndexKey)
				return err
			}
		}
	}

	return nil
}

func deleteBucketKey(dt int, ch string, ts int64, id MessageID, tx *bolt.Tx, isExt bool) error {
	b := tx.Bucket(bucketDelayedMsg)
	msgKey := getDelayedMsgDBKey(dt, ch, ts, id)
	oldV := b.Get(msgKey)
	err := b.Delete(msgKey)
	if err != nil {
		nsqLog.Infof("failed to delete delayed message: %v", msgKey)
		return err
	}
	if oldV != nil {
		err = deleteMsgIndex(oldV, tx, isExt)
		if err != nil {
			return err
		}

		b = tx.Bucket(bucketMeta)
		cntKey := append([]byte("counter_"), getDelayedMsgDBPrefixKey(dt, ch)...)
		cnt := uint64(0)
		cntBytes := b.Get(cntKey)
		if cntBytes != nil && len(cntBytes) == 8 {
			cnt = binary.BigEndian.Uint64(cntBytes)
		}
		if cnt > 0 {
			cnt--
			cntBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(cntBytes[:8], cnt)
			err = b.Put(cntKey, cntBytes)
			if err != nil {
				nsqLog.Infof("failed to update the meta count: %v, %v", cntKey, err)
				return err
			}
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("decr delayed cnt: %v, msg: %v", cnt, msgKey)
			}
		}
	} else {
		nsqLog.Infof("failed to get the deleting delayed message: %v", msgKey)
		return errBucketKeyNotFound
	}
	return nil
}

type DelayQueue struct {
	tname     string
	fullName  string
	partition int
	backend   *diskQueueWriter
	dataPath  string
	exitFlag  int32

	msgIDCursor  MsgIDGenerator
	defaultIDSeq uint64

	needFlush   int32
	putBuffer   bytes.Buffer
	kvStore     *bolt.DB
	EnableTrace int32
	SyncEvery   int64
	lastSyncCnt int64
	needFixData int32
	isExt       int32
	dbLock      sync.Mutex
	// prevent write while compact db
	compactMutex           sync.Mutex
	oldestChannelDelayedTs map[string]int64
	oldestMutex            sync.Mutex
	changedTs              int64
	updateConsumeCnt       int64
}

func getDefaultBoltDbOptions(readOnly bool) *bolt.Options {
	return &bolt.Options{
		Timeout:        time.Second,
		ReadOnly:       readOnly,
		FreelistType:   bolt.FreelistArrayType,
		NoFreelistSync: true,
	}
}

func NewDelayQueueForRead(topicName string, part int, dataPath string, opt *Options,
	idGen MsgIDGenerator, isExt bool) (*DelayQueue, error) {
	return newDelayQueue(topicName, part, dataPath, opt, idGen, isExt, getDefaultBoltDbOptions(true))
}

func NewDelayQueue(topicName string, part int, dataPath string, opt *Options,
	idGen MsgIDGenerator, isExt bool) (*DelayQueue, error) {

	return newDelayQueue(topicName, part, dataPath, opt, idGen, isExt, nil)
}
func newDelayQueue(topicName string, part int, dataPath string, opt *Options,
	idGen MsgIDGenerator, isExt bool, ro *bolt.Options) (*DelayQueue, error) {
	dataPath = path.Join(dataPath, "delayed_queue")
	readOnly := false
	if ro != nil && ro.ReadOnly {
		readOnly = true
	}
	if !readOnly {
		os.MkdirAll(dataPath, 0755)
	}
	q := &DelayQueue{
		tname:                  topicName,
		partition:              part,
		putBuffer:              bytes.Buffer{},
		dataPath:               dataPath,
		msgIDCursor:            idGen,
		oldestChannelDelayedTs: make(map[string]int64),
		SyncEvery:              opt.SyncEvery,
	}
	if isExt {
		q.isExt = 1
	}
	q.fullName = GetTopicFullName(q.tname, q.partition)
	backendName := getDelayQueueBackendName(q.tname, q.partition)
	// max delay message size need add the delay ts and channel name
	queue, err := newDiskQueueWriter(backendName,
		q.dataPath,
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength+8+255, 0, readOnly, nil)

	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed disk queue: %v , %v ", q.fullName, err, backendName)
		return nil, err
	}
	q.backend = queue.(*diskQueueWriter)
	if ro == nil {
		ro = getDefaultBoltDbOptions(false)
	}
	// since the bolt will fail to open in read mode if no db file, but leave the db file created.
	// So we check here before open
	dbFile := path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition))
	if readOnly {
		_, err := os.Stat(dbFile)
		if os.IsNotExist(err) {
			return nil, err
		}
	}
	q.kvStore, err = bolt.Open(dbFile, 0644, ro)
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed db: %v , %v ", q.fullName, err, backendName)
		return nil, err
	}
	q.kvStore.NoSync = true
	atomic.StoreInt64(&q.changedTs, time.Now().UnixNano())
	if readOnly {
		return q, nil
	}
	err = q.kvStore.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketDelayedMsg)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketDelayedMsgIndex)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketMeta)
		return err
	})
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed db: %v , %v ", q.fullName, err, backendName)
		if ro == nil || !ro.ReadOnly {
			return nil, err
		}
	}

	return q, nil
}

func (q *DelayQueue) CheckConsistence() error {
	// Perform consistency check.
	return q.getStore().View(func(tx *bolt.Tx) error {
		var count int
		ch := tx.Check()
		done := false
		for !done {
			select {
			case err, ok := <-ch:
				if !ok {
					done = true
					break
				}
				nsqLog.LogErrorf("topic(%v) failed to check delayed db: %v ", q.fullName, err)
				if err != nil && strings.Contains(err.Error(), "unreachable unfreed") {
					continue
				}
				count++
			}
		}

		if count > 0 {
			nsqLog.LogErrorf("topic(%v) failed to check delayed db, %d errors found ", q.fullName, count)
			return errors.New("boltdb file corrupt")
		}
		return nil
	})
}

func (q *DelayQueue) Stats() string {
	s := q.getStore().Stats()
	d, _ := json.MarshalIndent(s, "", " ")
	return string(d)
}

func (q *DelayQueue) reOpenStore() error {
	var err error
	ro := getDefaultBoltDbOptions(false)
	q.kvStore, err = bolt.Open(path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition)), 0644, ro)
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to open delayed db: %v ", q.fullName, err)
		return err
	}

	q.oldestMutex.Lock()
	q.oldestChannelDelayedTs = make(map[string]int64)
	q.oldestMutex.Unlock()

	q.kvStore.NoSync = true
	atomic.StoreInt64(&q.changedTs, time.Now().UnixNano())
	err = q.kvStore.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketDelayedMsg)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketDelayedMsgIndex)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(bucketMeta)
		return err
	})
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed db: %v", q.fullName, err)
		return err
	}

	return nil
}

func (q *DelayQueue) getStore() *bolt.DB {
	q.dbLock.Lock()
	d := q.kvStore
	q.dbLock.Unlock()
	return d
}

func (q *DelayQueue) GetFullName() string {
	return q.fullName
}

func (q *DelayQueue) GetTopicName() string {
	return q.tname
}

func (q *DelayQueue) GetTopicPart() int {
	return q.partition
}

func (q *DelayQueue) SetTrace(enable bool) {
	if enable {
		atomic.StoreInt32(&q.EnableTrace, 1)
	} else {
		atomic.StoreInt32(&q.EnableTrace, 0)
	}
}

func (q *DelayQueue) setExt() {
	atomic.StoreInt32(&q.isExt, 1)
}

func (q *DelayQueue) IsExt() bool {
	return atomic.LoadInt32(&q.isExt) == 1
}

func (q *DelayQueue) nextMsgID() MessageID {
	id := uint64(0)
	if q.msgIDCursor != nil {
		id = q.msgIDCursor.NextID()
	} else {
		id = atomic.AddUint64(&q.defaultIDSeq, 1)
	}
	return MessageID(id)
}

func (q *DelayQueue) RollbackNoLock(vend BackendOffset, diffCnt uint64) error {
	old := q.backend.GetQueueWriteEnd()
	nsqLog.Logf("reset the backend from %v to : %v, %v", old, vend, diffCnt)
	_, err := q.backend.RollbackWriteV2(vend, diffCnt)
	atomic.StoreInt32(&q.needFlush, 1)
	return err
}

func (q *DelayQueue) ResetBackendEndNoLock(vend BackendOffset, totalCnt int64) error {
	old := q.backend.GetQueueWriteEnd()
	if old.Offset() == vend && old.TotalMsgCnt() == totalCnt {
		return nil
	}
	nsqLog.Logf("topic %v reset the backend from %v to : %v, %v", q.GetFullName(), old, vend, totalCnt)
	_, err := q.backend.ResetWriteEndV2(vend, totalCnt)
	if err != nil {
		nsqLog.LogErrorf("topic %v reset backend to %v error: %v", q.fullName, vend, err)
	}
	atomic.StoreInt32(&q.needFlush, 1)
	return err
}

func (q *DelayQueue) TryFixQueueEnd(vend BackendOffset, totalCnt int64) error {
	old := q.backend.GetQueueWriteEnd()
	if old.Offset() == vend && old.TotalMsgCnt() == totalCnt {
		return nil
	}
	nsqLog.Logf("topic %v try fix the backend end from %v to : %v, %v", q.GetFullName(), old, vend, totalCnt)
	_, err := q.backend.TryFixWriteEnd(vend, totalCnt)
	if err != nil {
		nsqLog.LogErrorf("fix backend to %v error: %v", vend, err)
	}
	atomic.StoreInt32(&q.needFlush, 1)
	return err
}

func (q *DelayQueue) CheckDiskQueueReadToEndOK(offset int64, seekCnt int64, endOffset BackendOffset) error {
	snap := q.GetDiskQueueSnapshot(false)
	defer snap.Close()
	_, _, err := snap.CheckDiskQueueReadToEndOK(offset, seekCnt, endOffset)
	if err != nil {
		nsqLog.Warningf("check read failed at: %v, err: %s", offset, err)
		return err
	}
	return nil
}

func (q *DelayQueue) ResetBackendWithQueueStartNoLock(queueStartOffset int64, queueStartCnt int64) error {
	if queueStartOffset < 0 || queueStartCnt < 0 {
		return errors.New("queue start should not less than 0")
	}
	queueStart := q.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	queueStart.virtualEnd = BackendOffset(queueStartOffset)
	queueStart.totalMsgCnt = queueStartCnt
	nsqLog.Warningf("reset the topic %v backend with queue start: %v", q.GetFullName(), queueStart)
	err := q.backend.ResetWriteWithQueueStart(queueStart)
	if err != nil {
		return err
	}
	atomic.StoreInt32(&q.needFlush, 1)
	return nil
}

func (q *DelayQueue) GetDiskQueueSnapshot(checkCommit bool) *DiskQueueSnapshot {
	e := q.backend.GetQueueReadEnd()
	start := q.backend.GetQueueReadStart()
	d := NewDiskQueueSnapshot(getDelayQueueBackendName(q.tname, q.partition), q.dataPath, e)
	d.SetQueueStart(start)
	return d
}

func (q *DelayQueue) IsDataNeedFix() bool {
	return atomic.LoadInt32(&q.needFixData) == 1
}

func (q *DelayQueue) SetDataFixState(needFix bool) {
	if needFix {
		atomic.StoreInt32(&q.needFixData, 1)
	} else {
		atomic.StoreInt32(&q.needFixData, 0)
	}
}

func (q *DelayQueue) TotalMessageCnt() uint64 {
	return uint64(q.backend.GetQueueWriteEnd().TotalMsgCnt())
}

func (q *DelayQueue) TotalDataSize() int64 {
	e := q.backend.GetQueueWriteEnd()
	if e == nil {
		return 0
	}
	return int64(e.Offset())
}

func (q *DelayQueue) GetDBSize() (int64, error) {
	totalSize := int64(0)
	err := q.getStore().View(func(tx *bolt.Tx) error {
		totalSize = tx.Size()
		return nil
	})
	return totalSize, err
}

func (q *DelayQueue) BackupKVStoreTo(w io.Writer) (int64, error) {
	totalSize := int64(0)
	err := q.getStore().View(func(tx *bolt.Tx) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(tx.Size()))
		_, err := w.Write(buf)
		if err != nil {
			return err
		}
		totalSize = tx.Size() + 8
		_, err = tx.WriteTo(w)
		return err
	})
	return totalSize, err
}

func (q *DelayQueue) ReopenWithEmpty() error {
	q.compactMutex.Lock()
	defer q.compactMutex.Unlock()
	kvPath := path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition))
	nsqLog.Logf("topic(%v) reopen empty delayed db: %v", q.fullName, kvPath)
	q.dbLock.Lock()
	defer q.dbLock.Unlock()
	q.kvStore.Close()
	os.Remove(kvPath)
	err := q.reOpenStore()
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to reopen empty delayed db: %v , %v ", q.fullName, err, kvPath)
		return err
	}
	return nil
}

func preloadDBAndOptimizeOpen(dbPath string) error {
	ro := getDefaultBoltDbOptions(false)
	// use this to scan freelist and sync to disk
	ro.NoFreelistSync = false
	tmpDB, err := bolt.Open(dbPath, 0644, ro)
	if err != nil {
		return err
	}
	tmpDB.Sync()
	tmpDB.Close()
	return nil
}

func (q *DelayQueue) RestoreKVStoreFrom(body io.Reader) error {
	buf := make([]byte, 8)
	n, err := body.Read(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("unexpected length for body length")
	}
	bodyLen := int64(binary.BigEndian.Uint64(buf))
	tmpPath := fmt.Sprintf("%s-tmp.restore.%d", q.getStore().Path(), time.Now().UnixNano())
	err = os.Remove(tmpPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	_, err = io.CopyN(f, body, bodyLen)
	if err != nil {
		f.Close()
		return err
	}
	err = f.Sync()
	if err != nil {
		f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}

	q.compactMutex.Lock()
	defer q.compactMutex.Unlock()
	kvPath := path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition))
	q.dbLock.Lock()
	defer q.dbLock.Unlock()
	q.kvStore.Close()
	err = os.Rename(tmpPath, kvPath)
	if err != nil {
		return err
	}
	err = q.reOpenStore()
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to restore delayed db: %v , %v ", q.fullName, err, kvPath)
		return err
	}
	return nil
}

func (q *DelayQueue) PutDelayMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return 0, 0, 0, nil, errors.New("exiting")
	}
	if m.ID > 0 {
		nsqLog.Logf("should not pass id in message ")
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	if !IsValidDelayedMessage(m) {
		return 0, 0, 0, nil, errors.New("invalid delayed message")
	}

	id, offset, writeBytes, dend, err := q.put(m, nil, true, 0)
	return id, offset, writeBytes, &dend, err
}

func (q *DelayQueue) PutRawDataOnReplica(rawData []byte, offset BackendOffset, checkSize int64, msgNum int32) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := q.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		nsqLog.LogErrorf("topic %v: write offset mismatch: %v, %v", q.GetFullName(), offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	if msgNum != 1 {
		return nil, errors.New("delayed raw message number must be 1.")
	}
	var m Message
	_, _, _, dend, err := q.put(&m, rawData, false, checkSize)
	if err != nil {
		q.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
		return nil, err
	}
	return &dend, nil
}

func (q *DelayQueue) PutMessageOnReplica(m *Message, offset BackendOffset, checkSize int64) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := q.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		nsqLog.LogErrorf("topic %v: write offset mismatch: %v, %v", q.GetFullName(), offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	if !IsValidDelayedMessage(m) {
		return nil, errors.New("invalid delayed message")
	}
	_, _, _, dend, err := q.put(m, nil, false, checkSize)
	if err != nil {
		q.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
		return nil, err
	}
	return &dend, nil
}

func (q *DelayQueue) put(m *Message, rawData []byte, trace bool, checkSize int64) (MessageID, BackendOffset, int32, diskQueueEndInfo, error) {
	var err error
	var dend diskQueueEndInfo
	// it may happened while the topic is upgraded to extend topic, so the message from leader will be raw.
	if rawData != nil {
		if len(rawData) < 4 {
			return 0, 0, 0, dend, fmt.Errorf("invalid raw message data: %v", rawData)
		}
		m, err = DecodeDelayedMessage(rawData[4:], q.IsExt())
		if err != nil {
			return 0, 0, 0, dend, err
		}
	}
	if m.ID <= 0 {
		m.ID = q.nextMsgID()
	}

	var offset BackendOffset
	var writeBytes int32
	if rawData != nil {
		q.putBuffer.Reset()
		_, err := m.WriteDelayedTo(&q.putBuffer, q.IsExt())
		if err != nil {
			return 0, 0, 0, dend, err
		}
		offset, writeBytes, dend, err = q.backend.PutRawV2(rawData, 1)
		if checkSize > 0 && checkSize != int64(writeBytes) {
			return 0, 0, 0, dend, err
		}
	} else {
		offset, writeBytes, dend, err = writeDelayedMessageToBackendWithCheck(&q.putBuffer,
			m, checkSize, q.backend, q.IsExt())
	}
	atomic.StoreInt32(&q.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write delayed message to backend - %s",
			q.GetFullName(), err)
		return m.ID, offset, writeBytes, dend, err
	}
	msgKey := getDelayedMsgDBKey(int(m.DelayedType), m.DelayedChannel, m.DelayedTs, m.ID)

	wstart := time.Now()
	q.compactMutex.Lock()
	err = q.getStore().Update(func(tx *bolt.Tx) error {
		newIndexKey := getDelayedMsgDBIndexKey(int(m.DelayedType), m.DelayedChannel, m.DelayedOrigID)
		ib := tx.Bucket(bucketDelayedMsgIndex)
		iv := ib.Get(newIndexKey)
		b := tx.Bucket(bucketDelayedMsg)
		var oldV []byte
		var oldMsgKey []byte
		if iv != nil {
			ts, oldID, err := decodeDelayedMsgDBIndexValue(iv)
			if err != nil {
				return err
			}
			oldMsgKey = getDelayedMsgDBKey(int(m.DelayedType), m.DelayedChannel, ts, oldID)
			oldV = b.Get(oldMsgKey)
			// it may have some old data which only have index but no msg value
			if len(oldV) > 0 {
				oldMsg, err := DecodeDelayedMessage(oldV, q.IsExt())
				if err != nil {
					nsqLog.Warningf("found old delayed index key %v (%v, %v) msg value data wrong: %v, %v", newIndexKey, iv, oldMsgKey, oldV, m)
					// we can just delete this safely
				} else if oldMsg.DelayedOrigID != m.DelayedOrigID || oldMsg.DelayedChannel != m.DelayedChannel || oldMsg.DelayedType != m.DelayedType {
					// the value from old index is not the same message we are inserting, this may happend when old code use the wrong id in index
					nsqLog.Infof("found old delayed index key %v (%v, %v) msg value not matched : %v, %v", newIndexKey, iv, oldMsgKey, oldMsg, m)
					oldV = nil
				}
			}
		}
		exists := oldV != nil
		if exists && bytes.Equal(oldV, q.putBuffer.Bytes()) {
		} else {
			err := b.Put(msgKey, q.putBuffer.Bytes())
			if err != nil {
				return err
			}
			if oldV != nil && (!bytes.Equal(oldMsgKey, msgKey)) {
				err := b.Delete(oldMsgKey)
				if err != nil {
					nsqLog.Infof("failed to delete old delayed message: %v", msgKey)
					return err
				}
				err = ib.Delete(newIndexKey)
				if err != nil {
					nsqLog.Infof("failed to delete old delayed index : %v, %v", oldV, err)
					return err
				}
			}
			// note here we only support one index for the same message(if dup inserted, only one has the index)
			ib = tx.Bucket(bucketDelayedMsgIndex)
			d := getDelayedMsgDBIndexValue(m.DelayedTs, m.ID)
			err = ib.Put(newIndexKey, d)
			if err != nil {
				return err
			}
		}
		b = tx.Bucket(bucketMeta)
		if !exists {
			cntKey := append([]byte("counter_"), getDelayedMsgDBPrefixKey(int(m.DelayedType), m.DelayedChannel)...)
			cnt := uint64(0)
			cntBytes := b.Get(cntKey)
			if cntBytes != nil && len(cntBytes) == 8 {
				cnt = binary.BigEndian.Uint64(cntBytes)
			}
			cnt++
			cntBytes = make([]byte, 8)
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.LogDebugf("incr delayed cnt: %v, msg: %v", cnt, m)
			}

			binary.BigEndian.PutUint64(cntBytes[:8], cnt)
			err = b.Put(cntKey, cntBytes)
			if err != nil {
				return err
			}
		}
		return b.Put(syncedOffsetKey, []byte(strconv.Itoa(int(dend.Offset()))))
	})
	atomic.StoreInt64(&q.changedTs, time.Now().UnixNano())
	q.compactMutex.Unlock()
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write delayed message %v to kv store- %s",
			q.GetFullName(), m, err)
		return m.ID, offset, writeBytes, dend, err
	}
	if m.DelayedType == ChannelDelayed {
		q.oldestMutex.Lock()
		oldest, ok := q.oldestChannelDelayedTs[m.DelayedChannel]
		if !ok || oldest == 0 || m.DelayedTs < oldest {
			q.oldestChannelDelayedTs[m.DelayedChannel] = m.DelayedTs
		}
		q.oldestMutex.Unlock()
	}
	if nsqLog.Level() >= levellogger.LOG_DEBUG {
		cost := time.Since(wstart)
		if cost > time.Millisecond*2 {
			nsqLog.Logf("write local delayed queue db cost :%v", cost)
		}
	}
	if trace {
		if m.TraceID != 0 || atomic.LoadInt32(&q.EnableTrace) == 1 || nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqMsgTracer.TracePub(q.GetTopicName(), q.GetTopicPart(), "DELAY_QUEUE_PUB", m.TraceID, m, offset, dend.TotalMsgCnt())
		}
	}
	syncEvery := atomic.LoadInt64(&q.SyncEvery)
	if syncEvery == 1 ||
		dend.TotalMsgCnt()-atomic.LoadInt64(&q.lastSyncCnt) >= syncEvery {
		q.flush(false)
	}

	return m.ID, offset, writeBytes, dend, nil
}

func (q *DelayQueue) Delete() error {
	return q.exit(true)
}

func (q *DelayQueue) Close() error {
	return q.exit(false)
}

func (q *DelayQueue) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&q.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		q.getStore().Close()
		os.RemoveAll(path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition)))
		return q.backend.Delete()
	}

	// write anything leftover to disk
	q.flush(true)
	q.getStore().Close()
	return q.backend.Close()
}

func (q *DelayQueue) ForceFlush() {
	q.flush(false)
}

func (q *DelayQueue) flush(fsync bool) error {
	ok := atomic.CompareAndSwapInt32(&q.needFlush, 1, 0)
	if !ok {
		return nil
	}
	s := time.Now()
	atomic.StoreInt64(&q.lastSyncCnt, q.backend.GetQueueWriteEnd().TotalMsgCnt())
	err := q.backend.Flush(fsync)
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
		return err
	}
	q.getStore().Sync()

	cost := time.Now().Sub(s)
	if cost > slowCost {
		nsqLog.Logf("topic(%s): flush cost: %v", q.GetFullName(), cost)
	}

	if nsqLog.Level() >= levellogger.LOG_DEBUG {
		if cost > time.Millisecond*5 {
			nsqLog.Logf("flush local delayed queue db cost :%v", cost)
		}
	}

	return err
}

func (q *DelayQueue) GetChangedTs() int64 {
	return atomic.LoadInt64(&q.changedTs)
}

func (q *DelayQueue) emptyDelayedUntil(dt int, peekTs int64, id MessageID, ch string, emptyAll bool) (int64, error) {
	cleanedTs := int64(0)
	totalCnt, err := q.GetCurrentDelayedCnt(dt, ch)
	if err != nil {
		nsqLog.Infof("get delayed counter error while empty %v, %v", ch, err.Error())
		return cleanedTs, err
	}
	db := q.getStore()
	prefix := getDelayedMsgDBPrefixKey(dt, ch)
	if totalCnt < uint64(txMaxBatch) {
		if ds, _ := q.GetDBSize(); ds > largeDBSize {
			nsqLog.Infof("topic %v empty return early since exceed max size %v, %v, %v", q.GetFullName(), string(prefix), ds, totalCnt)
			// we just ignore large db error
			return cleanedTs, nil
		}
	}
	// 0. to reduce the lock, we first search the possible messages without write lock
	// 1. to avoid too much in batch, we should empty at most 10000 at each tx
	// 2. some large db size with less data may need long time to scan the batch size, so
	// we need check the scan time also (however, we can not handle the slow if the first seek is slow)
	scanStart := time.Now()
	batched := 0
	exceedMaxBatch := false
	bufLen := txMaxBatch
	if totalCnt < uint64(bufLen) {
		bufLen = int(totalCnt)
	}
	searchedKeys := make([]searchedItem, 0, bufLen)

	err = db.View(func(tx *bolt.Tx) error {
		dbSize := tx.Size()
		b := tx.Bucket(bucketDelayedMsg)
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			if batched > txMaxBatch {
				exceedMaxBatch = true
				nsqLog.Infof("topic %v empty return early since exceed max batch : %v, %v", q.GetFullName(), string(prefix), batched)
				break
			}
			if dbSize > largeDBSize/4 && time.Since(scanStart) >= time.Second {
				exceedMaxBatch = true
				nsqLog.Infof("topic %v empty return early since exceed max time: %v, %v, %v", q.GetFullName(), string(prefix), batched, dbSize)
				break
			}
			delayedType, delayedTs, delayedID, delayedCh, err := decodeDelayedMsgDBKey(k)
			if err != nil {
				nsqLog.Infof("decode key failed : %v, %v", k, err)
				continue
			}
			if delayedType != uint16(dt) {
				continue
			}
			if !emptyAll {
				if delayedTs > peekTs {
					break
				}
				// only check equal for timestamp since all ts older should be cleaned on all message ids
				if delayedTs == peekTs && delayedID >= id {
					break
				}
				if delayedCh != ch {
					continue
				}
			} else {
				if ch != "" && delayedCh != ch {
					continue
				}
			}
			searchedKeys = append(searchedKeys, searchedItem{
				delayedTs: delayedTs,
				delayedID: delayedID,
				ch:        delayedCh,
			})
			cleanedTs = delayedTs
			batched++
		}
		return nil
	})
	if err != nil {
		if err == errDBSizeTooLarge {
			return cleanedTs, nil
		}
		return cleanedTs, err
	}
	if testEmptyDelay {
		time.Sleep(time.Second)
	}
	q.compactMutex.Lock()
	defer q.compactMutex.Unlock()
	err = q.getStore().Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsg)
		for _, k := range searchedKeys {
			err = deleteBucketKey(dt, k.ch, k.delayedTs, k.delayedID, tx, q.IsExt())
			if err != nil {
				if err != errBucketKeyNotFound {
					nsqLog.Warningf("failed to delete : %v, %v", k, err.Error())
					continue
				}
			}
		}
		if batched == 0 && !exceedMaxBatch && emptyAll && ch != "" {
			bm := tx.Bucket(bucketMeta)
			cntKey := append([]byte("counter_"), getDelayedMsgDBPrefixKey(dt, ch)...)
			cnt := uint64(0)
			cntBytes := bm.Get(cntKey)
			if cntBytes != nil && len(cntBytes) == 8 {
				cnt = binary.BigEndian.Uint64(cntBytes)
			}
			if cnt <= 0 {
				return nil
			}
			nsqLog.Warningf("topic %v empty delayed counter need fix: %v, %v", q.GetFullName(), string(prefix), cnt)
			// we need scan again to make sure no other changed during last read scan
			c := b.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				// not empty , we do not reset the cnt
				return nil
			}
			cnt = 0
			cntBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(cntBytes[:8], cnt)
			err = bm.Put(cntKey, cntBytes)
			if err != nil {
				nsqLog.Infof("failed to update the meta count: %v, %v", cntKey, err)
				// we can ignore error for this, since reset count should not affect the channel empty
			}
		}
		return nil
	})
	if err != nil {
		return cleanedTs, err
	}

	if exceedMaxBatch {
		err = errOnlyPartialEmpty
	}
	if batched == 0 {
		return cleanedTs, err
	}
	if emptyAll || exceedMaxBatch {
		nsqLog.Infof("topic %v empty delayed: %v, %v, %v", q.GetFullName(), string(prefix), batched, peekTs)
	}
	atomic.StoreInt64(&q.changedTs, time.Now().UnixNano())
	if dt == ChannelDelayed && ch != "" {
		q.oldestMutex.Lock()
		q.oldestChannelDelayedTs[ch] = cleanedTs

		if emptyAll && !exceedMaxBatch {
			// no message anymore, oldest as some future
			q.oldestChannelDelayedTs[ch] = time.Now().Add(time.Minute * 10).UnixNano()
		}
		if nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("channel %v update oldest to %v at time %v",
				ch, cleanedTs, time.Now().UnixNano())
		}
		q.oldestMutex.Unlock()
	}
	return cleanedTs, err
}

func (q *DelayQueue) emptyAllDelayedType(dt int, ch string) (int64, error) {
	return q.emptyDelayedUntil(dt, 0, 0, ch, true)
}

func (q *DelayQueue) EmptyDelayedChannel(ch string) error {
	if ch == "" {
		// to avoid empty all channels by accident
		// we do not allow empty channel with empty channel name
		return errors.New("empty delayed channel name should be given")
	}
	_, err := q.emptyAllDelayedType(ChannelDelayed, ch)
	if err == errOnlyPartialEmpty {
		return nil
	}
	return err
}

func (q *DelayQueue) PeekRecentTimeoutWithFilter(results []Message, peekTs int64, filterType int,
	filterChannel string) (int, error) {

	oldest := int64(0)
	if filterType == ChannelDelayed && filterChannel != "" {
		q.oldestMutex.Lock()
		ok := false
		oldest, ok = q.oldestChannelDelayedTs[filterChannel]
		q.oldestMutex.Unlock()
		if ok && oldest > peekTs {
			if nsqLog.Level() > levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("channel %v peek until %v ignored since oldest is %v",
					filterChannel, peekTs, oldest)
			}
			return 0, nil
		}
	}

	oldChangeTs := q.GetChangedTs()
	db := q.getStore()
	idx := 0
	var prefix []byte
	if filterType > 0 {
		prefix = getDelayedMsgDBPrefixKey(filterType, filterChannel)
		if nsqLog.Level() > levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("peek prefix %v: channel %v", prefix, filterChannel)
		}
	}
	oldest = int64(0)
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsg)
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			_, delayedTs, _, delayedCh, err := decodeDelayedMsgDBKey(k)
			if err != nil {
				nsqLog.Infof("decode key failed : %v, %v", k, err)
				continue
			}
			if oldest == 0 && filterType == ChannelDelayed && filterChannel != "" {
				oldest = delayedTs
			}
			if nsqLog.Level() > levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("peek delayed message %v: %v at %v", k, delayedTs, time.Now().UnixNano())
			}

			if delayedTs > peekTs || idx >= len(results) {
				break
			}

			if filterChannel != "" && delayedCh != filterChannel {
				continue
			}

			if v == nil {
				// k is not nil, v is nil, sub bucket?
				nsqLog.LogErrorf("topic %v iterater nil value: %v",
					q.fullName, k)
				continue
			}
			buf := make([]byte, len(v))
			copy(buf, v)
			m, err := DecodeDelayedMessage(buf, q.IsExt())
			if err != nil {
				nsqLog.LogErrorf("topic %v failed to decode delayed message: %v, %v, %v",
					q.fullName, v, k, err)
				continue
			}
			if nsqLog.Level() > levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("peek delayed message %v: %v, %v", k, delayedTs, m)
			}

			if filterType >= 0 && filterType != int(m.DelayedType) {
				continue
			}
			results[idx] = *m
			idx++
		}
		return nil
	})
	// if the delayed queue changed during peeking, we should not update oldest ts since it may changed by write
	if err == nil && oldest > 0 && oldChangeTs == q.GetChangedTs() {
		q.oldestMutex.Lock()
		q.oldestChannelDelayedTs[filterChannel] = oldest
		if nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("channel %v update oldest to %v at time %v",
				filterChannel, oldest, time.Now().UnixNano())
		}
		q.oldestMutex.Unlock()
	}
	return idx, err
}

func (q *DelayQueue) PeekRecentChannelTimeout(now int64, results []Message, ch string) (int, error) {
	return q.PeekRecentTimeoutWithFilter(results, now, ChannelDelayed, ch)
}

func (q *DelayQueue) PeekRecentDelayedPub(now int64, results []Message) (int, error) {
	return q.PeekRecentTimeoutWithFilter(results, now, PubDelayed, "")
}

func (q *DelayQueue) PeekAll(results []Message) (int, error) {
	return q.PeekRecentTimeoutWithFilter(results, time.Now().Add(time.Hour*24*365).UnixNano(), -1, "")
}

func (q *DelayQueue) GetSyncedOffset() (BackendOffset, error) {
	var synced BackendOffset
	err := q.getStore().View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		v := b.Get(syncedOffsetKey)
		offset, err := strconv.Atoi(string(v))
		if err != nil {
			return err
		}
		synced = BackendOffset(offset)
		return nil
	})
	if err != nil {
		nsqLog.LogErrorf("topic %v failed to get synced offset: %v", q.fullName, err)
	}
	return synced, err
}

func (q *DelayQueue) GetCurrentDelayedCnt(dt int, channel string) (uint64, error) {
	cnt := uint64(0)
	err := q.getStore().View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		cntKey := []byte("counter_" + string(getDelayedMsgDBPrefixKey(dt, channel)))
		cntBytes := b.Get(cntKey)
		if cntBytes != nil {
			cnt = binary.BigEndian.Uint64(cntBytes)
		}
		return nil
	})

	return cnt, err
}

func (q *DelayQueue) ConfirmedMessage(msg *Message) error {
	// confirmed message is finished by channel, this message has swap the
	// delayed id and original id to make sure the map key of inflight is original id
	q.compactMutex.Lock()
	err := q.getStore().Update(func(tx *bolt.Tx) error {
		return deleteBucketKey(int(msg.DelayedType), msg.DelayedChannel,
			msg.DelayedTs, msg.DelayedOrigID, tx, q.IsExt())
	})
	atomic.StoreInt64(&q.changedTs, time.Now().UnixNano())
	q.compactMutex.Unlock()
	if err != nil {
		if err != errBucketKeyNotFound {
			nsqLog.LogErrorf(
				"%s : failed to delete delayed message %v-%v, %v",
				q.GetFullName(), msg.DelayedOrigID, msg, err)
		} else {
			nsqLog.Logf(
				"%s : failed to delete delayed message %v-%v, %v",
				q.GetFullName(), msg.DelayedOrigID, msg, err)
			return nil
		}
	}
	return err
}

// note: since the msg index can only index one message, so if there are more than one duplicate delayed messages,
// after deleting one, we will have no index for others. We should scan with channel prefix to find them.
func (q *DelayQueue) FindChannelMessageDelayed(msgID MessageID, ch string, tryScan bool) (*Message, error) {
	var msg *Message
	msgKey := getDelayedMsgDBIndexKey(ChannelDelayed, ch, msgID)
	err := q.getStore().View(func(tx *bolt.Tx) error {
		ib := tx.Bucket(bucketDelayedMsgIndex)
		v := ib.Get(msgKey)
		var ts int64
		var err error
		hasIndex := true
		if v != nil {
			ts, _, err = decodeDelayedMsgDBIndexValue(v)
			if err != nil {
				return err
			}
		} else {
			// no index means no data for this msgid
			if !tryScan {
				return nil
			}
			hasIndex = false
		}
		prefix := getDelayedMsgDBPrefixKeyWithTs(ChannelDelayed, ch, ts)
		if !hasIndex {
			prefix = getDelayedMsgDBPrefixKey(ChannelDelayed, ch)
		}
		b := tx.Bucket(bucketDelayedMsg)
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			_, _, _, delayedCh, err := decodeDelayedMsgDBKey(k)
			if err != nil {
				nsqLog.Infof("decode key failed : %v, %v", k, err)
				continue
			}
			if delayedCh != ch {
				continue
			}
			if v == nil {
				continue
			}
			buf := make([]byte, len(v))
			copy(buf, v)
			m, err := DecodeDelayedMessage(buf, q.IsExt())
			if err != nil {
				nsqLog.LogErrorf("topic %v failed to decode delayed message: %v, %v, %v",
					q.fullName, v, k, err)
				continue
			}
			if m.DelayedOrigID == msgID {
				msg = m
				break
			}
		}
		return nil
	})
	return msg, err
}

func (q *DelayQueue) getChannelDelayedMsgDelayedTs(msgID MessageID, ch string) (int64, error) {
	msgKey := getDelayedMsgDBIndexKey(ChannelDelayed, ch, msgID)
	var ts int64
	err := q.getStore().View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsgIndex)
		v := b.Get(msgKey)
		if v != nil {
			var errDecode error
			ts, _, errDecode = decodeDelayedMsgDBIndexValue(v)
			return errDecode
		}
		return errBucketKeyNotFound
	})
	return ts, err
}

// IsChannelMessageDelayed check if this message may already exist. Note if dup delayed message with same id exist,
// after one of them deleted, the check exist may return false. We can sure whether it is exist, however we can not
// total sure whether it is not exist.
func (q *DelayQueue) IsChannelMessageDelayed(msgID MessageID, ch string) bool {
	found := false
	msgKey := getDelayedMsgDBIndexKey(ChannelDelayed, ch, msgID)
	q.getStore().View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsgIndex)
		v := b.Get(msgKey)
		if v != nil {
			found = true
		}
		return nil
	})
	return found
}

// TODO: benchmark this
func (q *DelayQueue) GetOldestConsumedState(chList []string, includeOthers bool) (RecentKeyList, map[int]uint64, map[string]uint64) {
	db := q.getStore()
	prefixList := make([][]byte, 0, len(chList)+2)
	var cntList map[int]uint64
	var err error
	if includeOthers {
		for filterType := MinDelayedType; filterType < MaxDelayedType; filterType++ {
			if filterType == ChannelDelayed {
				continue
			}
			if cntList == nil {
				cntList = make(map[int]uint64)
			}
			prefixList = append(prefixList, getDelayedMsgDBPrefixKey(filterType, ""))
			cntList[filterType], err = q.GetCurrentDelayedCnt(filterType, "")
			if err != nil {
				return nil, nil, nil
			}
		}
	}
	chIndex := len(prefixList)
	var channelCntList map[string]uint64
	if len(chList) > 0 {
		channelCntList = make(map[string]uint64)
	}
	for _, ch := range chList {
		prefixList = append(prefixList, getDelayedMsgDBPrefixKey(ChannelDelayed, ch))
		channelCntList[ch], err = q.GetCurrentDelayedCnt(ChannelDelayed, ch)

		if err != nil {
			return nil, nil, nil
		}
	}
	keyList := make(RecentKeyList, 0, len(prefixList))
	for i, prefix := range prefixList {
		var origCh string
		if i >= chIndex {
			origCh = chList[i-chIndex]
		}

		if nsqLog.Level() > levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("peek prefix %v: channel %v", prefix, origCh)
		}

		err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketDelayedMsg)
			c := b.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				_, delayedTs, _, delayedCh, err := decodeDelayedMsgDBKey(k)
				if err != nil {
					nsqLog.Infof("decode key failed : %v, %v", k, err)
					continue
				}

				if nsqLog.Level() > levellogger.LOG_DETAIL {
					nsqLog.LogDebugf("peek delayed message %v: %v, %v", k, delayedTs, origCh)
				}

				// prefix seek may across to other channel with the same prefix
				if delayedCh != origCh {
					continue
				}
				ck := make([]byte, len(k))
				copy(ck, k)
				keyList = append(keyList, ck)
				break
			}
			return nil
		})
		if err != nil {
			return nil, nil, nil
		}
	}
	return keyList, cntList, channelCntList
}

func (q *DelayQueue) UpdateConsumedState(ts int64, keyList RecentKeyList, cntList map[int]uint64, channelCntList map[string]uint64) error {
	// avoid too much running
	n := atomic.AddInt64(&q.updateConsumeCnt, 1)
	defer atomic.AddInt64(&q.updateConsumeCnt, -1)
	if n > maxEmptyRunning {
		nsqLog.Infof("topic %v empty too much %v", q.GetFullName(), n)
		return errTooMuchRunningEmpty
	}
	var anyErr error
	for _, k := range keyList {
		dt, dts, id, delayedCh, err := decodeDelayedMsgDBKey(k)
		if err != nil {
			nsqLog.Infof("decode key failed : %v, %v", k, err)
			continue
		}
		_, err = q.emptyDelayedUntil(int(dt), dts, id, delayedCh, false)
		if err != nil {
			anyErr = err
		}
	}
	for dt, cnt := range cntList {
		if cnt == 0 && dt != ChannelDelayed {
			_, err := q.emptyDelayedUntil(dt, ts, 0, "", false)
			if err != nil {
				anyErr = err
			}
		}
	}
	for ch, cnt := range channelCntList {
		if cnt == 0 {
			_, err := q.emptyDelayedUntil(ChannelDelayed, ts, 0, ch, false)
			if err != nil {
				anyErr = err
			}
		}
	}
	return anyErr
}

func (q *DelayQueue) TryCleanOldData(retentionSize int64, noRealClean bool, maxCleanOffset BackendOffset) (BackendQueueEnd, error) {
	// clean the data that has been consumed and keep the retention policy
	var oldestPos BackendQueueEnd
	oldestPos = q.backend.GetQueueReadEnd()
	if oldestPos == nil {
		nsqLog.Logf("no end position found")
		return nil, nil
	}
	cleanStart := q.backend.GetQueueReadStart()
	if cleanStart.Offset()+BackendOffset(retentionSize) >= oldestPos.Offset() {
		return nil, nil
	}
	nsqLog.Logf("clean topic %v data current start: %v, oldest end %v, max clean end: %v",
		q.GetFullName(), cleanStart, oldestPos, maxCleanOffset)

	if oldestPos.Offset() < maxCleanOffset || maxCleanOffset == BackendOffset(0) {
		maxCleanOffset = oldestPos.Offset()
	}
	snapReader := NewDiskQueueSnapshot(getDelayQueueBackendName(q.tname, q.partition), q.dataPath, oldestPos)
	snapReader.SetQueueStart(cleanStart)
	seekCnt := int64(0)
	if cleanStart.TotalMsgCnt() > 0 {
		seekCnt = cleanStart.TotalMsgCnt() - 1
	}
	err := snapReader.SeekTo(cleanStart.Offset(), seekCnt)
	if err != nil {
		nsqLog.Errorf("topic: %v failed to seek to %v: %v", q.GetFullName(), cleanStart, err)
		return nil, err
	}
	readInfo := snapReader.GetCurrentReadQueueOffset()
	data := snapReader.ReadOne()
	if data.Err != nil {
		return nil, data.Err
	}
	var cleanEndInfo BackendQueueOffset
	retentionDay := int32(DEFAULT_RETENTION_DAYS)
	cleanTime := time.Now().Add(-1 * time.Hour * 24 * time.Duration(retentionDay))
	for {
		if retentionSize > 0 {
			// clean data ignore the retention day
			// only keep the retention size (start from the last consumed)
			if data.Offset > maxCleanOffset-BackendOffset(retentionSize) {
				break
			}
			cleanEndInfo = readInfo
		} else {
			msg, decodeErr := DecodeDelayedMessage(data.Data, q.IsExt())
			if decodeErr != nil {
				nsqLog.LogErrorf("topic %v failed to decode message - %s - %v", q.fullName, decodeErr, data)
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
			nsqLog.Logf("failed to skip - %s ", err)
			break
		}
		readInfo = snapReader.GetCurrentReadQueueOffset()
		data = snapReader.ReadOne()
		if data.Err != nil {
			nsqLog.LogErrorf("topic %v failed to read - %s ", q.fullName, data.Err)
			break
		}
	}

	nsqLog.Infof("clean topic %v delayed queue from %v under retention %v, %v",
		q.GetFullName(), cleanEndInfo, cleanTime, retentionSize)

	// always try compact first since it may skip to clean under retention size.
	if !noRealClean {
		err := q.compactStore(false)
		if err != nil {
			nsqLog.Errorf("topic %v failed to compact the bolt db: %v", q.fullName, err)
			return nil, err
		}
	}

	if cleanEndInfo == nil || cleanEndInfo.Offset()+BackendOffset(retentionSize) >= maxCleanOffset {
		if cleanEndInfo != nil {
			nsqLog.Warningf("clean topic %v data at position: %v could not exceed current oldest confirmed %v and max clean end: %v",
				q.GetFullName(), cleanEndInfo, oldestPos, maxCleanOffset)
		}
		return nil, nil
	}
	return q.backend.CleanOldDataByRetention(cleanEndInfo, noRealClean, maxCleanOffset)
}

func (q *DelayQueue) compactStore(force bool) error {
	src := q.getStore()
	origPath := src.Path()
	if !force {
		fi, err := os.Stat(origPath)
		if err != nil {
			nsqLog.Infof("db %v compact stat err: %s", origPath, err.Error())
			return err
		}
		if fi.Size() < int64(CompactThreshold) {
			return nil
		}
		cnt := uint64(0)
		err = src.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketMeta)
			prefix := []byte("counter_")
			c := b.Cursor()
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				if v != nil && len(v) == 8 {
					cnt += binary.BigEndian.Uint64(v)
				}
			}
			return nil
		})

		if err != nil {
			nsqLog.Infof("db %v compact get counter err: %s", origPath, err.Error())
			return err
		}
		if cnt > CompactCntThreshold {
			nsqLog.Infof("db %v no need compact %v, %v", origPath, fi.Size(), cnt)
			return nil
		}
		// 10000 msgs with no delete is about 80MB, so we should check if it can become smaller after compact
		// to avoid too much compact.
		if cnt > 0 {
			singleAvgSize := fi.Size() / int64(cnt)
			if singleAvgSize < int64(compactSingleAvgSize) {
				nsqLog.Infof("db %v no need compact %v, %v, %v", origPath, singleAvgSize, fi.Size(), cnt)
				return nil
			}
		}
	}
	tmpPath := fmt.Sprintf("%s-tmp.compact.%d", src.Path(), time.Now().UnixNano())
	// Open destination database.
	ro := getDefaultBoltDbOptions(false)
	// we need sync free list to speed up the reopen which will hold write lock
	ro.NoFreelistSync = false
	dst, err := bolt.Open(tmpPath, 0644, ro)
	if err != nil {
		return err
	}
	dst.NoSync = true
	q.compactMutex.Lock()
	oldChangedTs := q.GetChangedTs()
	q.compactMutex.Unlock()

	nsqLog.Infof("db %v begin compact", origPath)
	defer nsqLog.Infof("db %v end compact", origPath)
	err = compactBolt(dst, src, time.Second*30)
	if err != nil {
		nsqLog.Infof("db %v compact failed: %v", origPath, err)
		os.Remove(tmpPath)
		return err
	}
	nsqLog.Infof("db %v compact scan finished", origPath)

	q.compactMutex.Lock()
	defer q.compactMutex.Unlock()
	if oldChangedTs != q.GetChangedTs() {
		nsqLog.Infof("db %v changed during compact scaning: %v", origPath, oldChangedTs)
		os.Remove(tmpPath)
		// just return nil since no any actual error
		return nil
	}
	q.dbLock.Lock()
	defer q.dbLock.Unlock()
	q.kvStore.Close()
	nsqLog.Infof("old db %v closed", origPath)
	// TODO: speed up , first rename old to tmp, if failed rename back, if success , delete in background
	err = os.Rename(tmpPath, origPath)
	openErr := q.reOpenStore()
	if openErr != nil {
		nsqLog.Errorf("db %v failed to reopen while compacted : %v", origPath, openErr)
	}
	if err != nil {
		nsqLog.Infof("db %v failed to rename compacted db: %v", origPath, err)
		return err
	}
	if openErr != nil {
		return openErr
	}
	return nil
}

func (q *DelayQueue) Dump() error {
	src := q.getStore()
	var size int64
	if err := walkBolt(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			nsqLog.Infof("bucket: %v(%s), seq: %v", k, k, seq)
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		nsqLog.Infof("bucket: %v(%s), seq: %v", keys[0], keys[0], seq)
		if nk > 1 {
			for _, k := range keys[1:] {
				nsqLog.Infof("bucket: %v(%s), seq: %v", k, k, seq)
			}
		}

		// If there is no value then this is a bucket call.
		if v == nil {
			nsqLog.Infof("bucket: %v(%s), seq: %v", k, k, seq)
			return nil
		}

		// Otherwise treat it as a key/value pair.
		nsqLog.Infof("key: %v(%s), value: %v(%s)", k, k, v, v)
		return nil
	}); err != nil {
		return err
	}

	nsqLog.Infof("total: %v", size)
	return nil
}

func compactBolt(dst, src *bolt.DB, maxCompactTime time.Duration) error {
	startT := time.Now()
	defer dst.Close()
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walkBolt(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > TxMaxSize && TxMaxSize != 0 {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// TODO: timeout here is not enough, since scan large empty db may cost long time
			if time.Since(startT) >= maxCompactTime {
				return errors.New("compact timeout")
			}
			// Start new transaction.
			var innererr error
			tx, innererr = dst.Begin(true)
			if innererr != nil {
				return innererr
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		vv := make([]byte, len(v))
		copy(vv, v)
		return b.Put(k, vv)
	}); err != nil {
		return err
	}

	err = tx.Commit()
	if err == nil {
		err = dst.Sync()
	}
	return err
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walkBolt(db *bolt.DB, walkFn walkFunc) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn)
		})
	})
}

func walkBucket(b *bolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc) error {
	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn)
	})
}
