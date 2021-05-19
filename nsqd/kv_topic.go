package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/nsqd/codec"
	"github.com/youzan/nsq/nsqd/engine"
)

const (
	SepStart     = ":"
	SepStop      = ";"
	maxBatchSize = 1000
)

const (
	TopicPrefix = "topic"
	// TopicPrefix + topicFullName + SepStart + SomeOtherPrefix
	TopicMetaPrefix      = "metav1"
	TopicMsgIDPrefix     = "msgid"
	TopicTagPrefix       = "tag"
	TopicTraceKeyPrefix  = "tracekey"
	TopicMsgOffsetPrefix = "msgoffset"
	TopicMsgCntPrefix    = "msgcnt"
	TopicMsgTsPrefix     = "msgts"
)

var (
	ErrMsgNotFoundInIndex = errors.New("message not found in index")
	errInvalidEncodedData = errors.New("invalid encoded data")
	errBatchSizeLimit     = errors.New("batch size limit exceeded")
	errInvalidLimit       = errors.New("invalid limit, should be >0")
)

func getKVKeyForTopicBegin(fullName string) []byte {
	startKey := make([]byte, 0, len(TopicPrefix)+len(fullName)+2+4)
	startKey, _ = codec.EncodeMemCmpKey(startKey[:0], TopicPrefix, SepStart, fullName, SepStart)
	return startKey
}

func getKVKeyForTopicEnd(fullName string) []byte {
	endKey := make([]byte, 0, len(TopicPrefix)+len(fullName)+2+4)
	endKey, _ = codec.EncodeMemCmpKey(endKey[:0], TopicPrefix, SepStart, fullName, SepStop)
	return endKey
}

func getKVKeyForTopicMeta(fullName string) []byte {
	key := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicMetaPrefix)+2+5)
	key, _ = codec.EncodeMemCmpKey(key[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMetaPrefix)
	return key
}

func encodeTopicMetaValue(offset int64, cnt int64) []byte {
	buf := make([]byte, 0, 8+8+2)
	buf, _ = codec.EncodeMemCmpKey(buf, offset, cnt)
	return buf
}

func decodeTopicMetaValue(buf []byte) (int64, int64, error) {
	vals, err := codec.Decode(buf, 2)
	if err != nil {
		return 0, 0, err
	}
	if len(vals) < 2 {
		return 0, 0, errInvalidEncodedData
	}
	offset := vals[0].(int64)
	cnt := vals[1].(int64)
	return offset, cnt, nil
}

func getKVKeyForMsgID(fullName string, msgid MessageID) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicMsgIDPrefix)+8+3+7)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgIDPrefix, SepStart, uint64(msgid))
	return keyBuf
}

func getKVKeyForMsgIDEnd(fullName string) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicMsgIDPrefix)+3+6)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgIDPrefix, SepStop)
	return keyBuf
}

func encodeMsgIDOffsetCntValue(msgid MessageID, offset, cnt int64) []byte {
	idBuf := make([]byte, 0, 3*(8+1))
	idBuf, _ = codec.EncodeMemCmpKey(idBuf, uint64(msgid), offset, cnt)
	return idBuf
}

func decodeMsgIDOffsetCntValue(buf []byte) (MessageID, int64, int64, error) {
	vals, err := codec.Decode(buf, 3)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(vals) < 3 {
		return 0, 0, 0, errInvalidEncodedData
	}
	id := vals[0].(uint64)
	offset := vals[1].(int64)
	cnt := vals[2].(int64)
	return MessageID(id), offset, cnt, nil
}

func getKVKeyForMsgTrace(fullName string, msgid MessageID, traceID uint64) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+8+8+4+9)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicTraceKeyPrefix, SepStart, traceID, SepStart, uint64(msgid))
	return keyBuf
}

func getKVKeyForMsgTraceIDEnd(fullName string, traceID uint64) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+4+len(TopicTraceKeyPrefix)+8+4+8)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicTraceKeyPrefix, SepStart, traceID, SepStop)
	return keyBuf
}

func getKVKeyForMsgTraceEnd(fullName string) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+3+6)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicTraceKeyPrefix, SepStop)
	return keyBuf
}

func decodeMsgTraceKey(buf []byte) (string, uint64, MessageID, error) {
	var tname string
	var tid uint64
	var msgid MessageID
	vals, err := codec.Decode(buf, 9)
	if err != nil {
		return tname, tid, msgid, err
	}
	if len(vals) != 9 {
		return tname, tid, msgid, errInvalidEncodedData
	}
	tname = string(vals[2].([]byte))
	tid = vals[6].(uint64)
	msgid = MessageID(vals[8].(uint64))
	return tname, tid, msgid, nil
}

func getKVKeyForMsgOffset(fullName string, msgStartOffset int64, msgEndOffset int64) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+4+len(TopicTraceKeyPrefix)+8+8+4+9)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgOffsetPrefix, SepStart, msgStartOffset, SepStart, msgEndOffset)
	return keyBuf
}

func getKVKeyForMsgOffsetEnd(fullName string) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+3+6)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgOffsetPrefix, SepStop)
	return keyBuf
}

func decodeMsgOffsetKey(b []byte) (int64, int64, error) {
	vals, err := codec.Decode(b, 9)
	if err != nil {
		return 0, 0, err
	}
	if len(vals) < 9 {
		return 0, 0, errInvalidEncodedData
	}
	start := vals[6].(int64)
	end := vals[8].(int64)
	return start, end, nil
}

func encodeMsgOffsetValue(msgid MessageID, cnt int64, ts int64) []byte {
	valueBuf := make([]byte, 0, 8*3+3)
	valueBuf, _ = codec.EncodeMemCmpKey(valueBuf[:0], uint64(msgid), cnt, ts)
	return valueBuf
}

func decodeMsgOffsetValue(buf []byte) (MessageID, int64, int64, error) {
	var msgid MessageID
	var cnt int64
	var ts int64
	vals, err := codec.Decode(buf, 3)
	if err != nil {
		return msgid, cnt, ts, err
	}
	if len(vals) != 3 {
		return msgid, cnt, ts, errInvalidEncodedData
	}
	msgid = MessageID(vals[0].(uint64))
	cnt = vals[1].(int64)
	ts = vals[2].(int64)
	return msgid, cnt, ts, nil
}

func getKVKeyForMsgCnt(fullName string, msgCnt int64) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+8+3+7)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgCntPrefix, SepStart, msgCnt)
	return keyBuf
}
func getKVKeyForMsgCntEnd(fullName string) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+3+6)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgCntPrefix, SepStop)
	return keyBuf
}

func getKVKeyForMsgTs(fullName string, msgid MessageID, ts int64) []byte {
	keyBuf := make([]byte, 0, len(TopicPrefix)+len(fullName)+len(TopicTraceKeyPrefix)+8+8+4+9)
	keyBuf, _ = codec.EncodeMemCmpKey(keyBuf[:0], TopicPrefix, SepStart, fullName, SepStart, TopicMsgTsPrefix, SepStart, ts, SepStart, uint64(msgid))
	return keyBuf
}

type KVTopic struct {
	kvEng      engine.KVEngine
	tname      string
	fullName   string
	partition  int
	putBuffer  bytes.Buffer
	bp         sync.Pool
	isExt      int32
	tpLog      *levellogger.LevelLogger
	lastOffset int64
	lastCnt    int64
	magicCode  int64
	defaultWB  engine.WriteBatch
}

func NewKVTopic(topicName string, part int, opt *Options) *KVTopic {
	return NewKVTopicWithExt(topicName, part, false, opt)
}

func NewKVTopicWithExt(topicName string, part int, ext bool, opt *Options) *KVTopic {
	dataPath := path.Join(opt.DataPath, topicName)
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		nsqLog.LogErrorf("failed to create directory %s: %s ", dataPath, err)
		return nil
	}
	backendName := getBackendName(topicName, part)
	cfg := engine.NewRockConfig()
	cfg.DisableWAL = true
	cfg.MaxWriteBufferNumber = 4
	cfg.DataDir = path.Join(dataPath, backendName)
	eng, err := engine.NewKVEng(cfg)
	if err != nil {
		nsqLog.LogErrorf("failed to create engine: %s ", err)
		return nil
	}
	err = eng.OpenEng()
	if err != nil {
		nsqLog.LogErrorf("failed to open engine: %s ", err)
		return nil
	}
	return NewKVTopicWithEngine(topicName, part, ext, eng)
}

func NewKVTopicWithEngine(topicName string, part int, ext bool, eng engine.KVEngine) *KVTopic {
	if eng == nil {
		return nil
	}
	if part > MAX_TOPIC_PARTITION {
		return nil
	}
	t := &KVTopic{
		tname:     topicName,
		partition: part,
		putBuffer: bytes.Buffer{},
	}
	t.fullName = GetTopicFullName(t.tname, t.partition)
	t.tpLog = nsqLog.WrappedWithPrefix("["+t.fullName+"]", 0)
	if ext {
		t.setExt()
	}

	t.bp.New = func() interface{} {
		return &bytes.Buffer{}
	}

	t.kvEng = eng
	t.defaultWB = eng.NewWriteBatch()
	offset, cnt, err := t.GetTopicMeta()
	if err != nil {
		t.tpLog.LogErrorf("failed to init topic meta: %s ", err)
		return nil
	}
	t.lastOffset = offset
	t.lastCnt = cnt
	return t
}

func (t *KVTopic) setExt() {
	atomic.StoreInt32(&t.isExt, 1)
}

func (t *KVTopic) IsExt() bool {
	return atomic.LoadInt32(&t.isExt) == 1
}

func (t *KVTopic) BufferPoolGet(capacity int) *bytes.Buffer {
	b := t.bp.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(capacity)
	return b
}

func (t *KVTopic) BufferPoolPut(b *bytes.Buffer) {
	t.bp.Put(b)
}

func (t *KVTopic) GetFullName() string {
	return t.fullName
}

func (t *KVTopic) GetTopicName() string {
	return t.tname
}

func (t *KVTopic) GetTopicPart() int {
	return t.partition
}

func (t *KVTopic) GetTopicMeta() (int64, int64, error) {
	key := getKVKeyForTopicMeta(t.fullName)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return 0, 0, err
	}
	if v == nil {
		return 0, 0, nil
	}
	return decodeTopicMetaValue(v)
}

func (t *KVTopic) saveTopicMetaInBatch(wb engine.WriteBatch, offset int64, cnt int64) {
	key := getKVKeyForTopicMeta(t.fullName)
	buf := encodeTopicMetaValue(offset, cnt)
	wb.Put(key, buf)
}

// search the first msg [offset, end]
func (t *KVTopic) getMsgIDCntTsStartFromOffset(vend int64) (MessageID, int64, int64, int64, error) {
	// tag info or trace key info currently not cleaned
	minKey := getKVKeyForMsgOffset(t.fullName, vend, 0)
	maxKey := getKVKeyForMsgOffsetEnd(t.fullName)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeClose
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer it.Close()
	it.SeekToFirst()
	if !it.Valid() {
		// maybe no data before vend while sync data from leader on replica
		return 0, 0, 0, 0, ErrMsgNotFoundInIndex
	}
	v := it.RefValue()
	if v == nil {
		return 0, 0, 0, 0, ErrMsgNotFoundInIndex
	}
	start, _, err := decodeMsgOffsetKey(it.RefKey())
	if err != nil {
		return 0, 0, 0, 0, err
	}
	id, cnt, ts, err := decodeMsgOffsetValue(v)
	return id, cnt, ts, start, err
}

// search the last msg at [begin, offset)
// return id, cnt, ts, realoffset
func (t *KVTopic) getMsgIDCntTsLessThanOffset(vend int64) (MessageID, int64, int64, int64, error) {
	// tag info or trace key info currently not cleaned
	minKey := getKVKeyForMsgOffset(t.fullName, 0, 0)
	maxKey := getKVKeyForMsgOffset(t.fullName, vend, 0)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeOpen
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer it.Close()
	it.SeekToLast()
	if !it.Valid() {
		// maybe no data before vend while sync data from leader on replica
		return 0, 0, 0, 0, nil
	}
	v := it.RefValue()
	if v == nil {
		return 0, 0, 0, 0, ErrMsgNotFoundInIndex
	}
	start, _, err := decodeMsgOffsetKey(it.RefKey())
	if err != nil {
		return 0, 0, 0, 0, err
	}
	id, cnt, ts, err := decodeMsgOffsetValue(v)
	return id, cnt, ts, start, err
}

func (t *KVTopic) getMsgIDCntTsAtOffset(vend int64) (MessageID, int64, int64, error) {
	// tag info or trace key info currently not cleaned
	minKey := getKVKeyForMsgOffset(t.fullName, vend, 0)
	maxKey := getKVKeyForMsgOffset(t.fullName, vend+1, 0)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeOpen
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return 0, 0, 0, err
	}
	defer it.Close()
	it.SeekToFirst()
	if !it.Valid() {
		// maybe no data before vend while sync data from leader on replica
		return 0, 0, 0, nil
	}
	v := it.RefValue()
	if v == nil {
		return 0, 0, 0, ErrMsgNotFoundInIndex
	}
	return decodeMsgOffsetValue(v)
}

func (t *KVTopic) ResetBackendEnd(vend BackendOffset, totalCnt int64) error {
	msgid, msgCnt, _, foundOffset, err := t.getMsgIDCntTsStartFromOffset(int64(vend))
	if err != nil {
		return err
	}
	if foundOffset != int64(vend) {
		t.tpLog.Warningf("reset offset %v not matched with db at offset: %v, id: %v", vend, foundOffset, msgid)
	}
	// the message at offset and greater than offset should be removed
	// so the left count is the count index at offset
	if msgCnt != totalCnt {
		t.tpLog.Warningf("total count %v not matched with db %v at offset: %v, id: %v", totalCnt, msgCnt, vend, msgid)
	}
	minKey := getKVKeyForMsgOffset(t.fullName, foundOffset, 0)
	maxKey := getKVKeyForMsgOffsetEnd(t.fullName)
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgID(t.fullName, MessageID(msgid))
	maxKey = getKVKeyForMsgIDEnd(t.fullName)
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgCnt(t.fullName, msgCnt)
	maxKey = getKVKeyForMsgCntEnd(t.fullName)
	wb.DeleteRange(minKey, maxKey)

	t.saveTopicMetaInBatch(wb, foundOffset, msgCnt)
	//minKey = getKVKeyForMsgTs(t.fullName, MessageID(msgid), msgTs)
	//maxKey = getKVKeyForMsgTsEnd(t.fullName)
	//wb.DeleteRange(minKey, maxKey)
	err = wb.Commit()
	if err != nil {
		return err
	}
	t.lastOffset = foundOffset
	t.lastCnt = msgCnt
	return nil
}

// PutMessage writes a Message to the queue
func (t *KVTopic) PutMessage(m *Message) (int32, BackendQueueEnd, error) {
	endOffset, writeBytes, dendCnt, err := t.put(m, 0)
	if err != nil {
		return writeBytes, nil, err
	}
	dn := &diskQueueEndInfo{totalMsgCnt: dendCnt}
	dn.virtualEnd = endOffset
	return writeBytes, dn, err
}

// this raw data include size header
func (t *KVTopic) PutRawDataOnReplica(rawData []byte, offset BackendOffset, checkSize int64, msgNum int32) (BackendQueueEnd, error) {
	wend := t.lastOffset
	if wend != int64(offset) {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	if len(rawData) < 4 {
		return nil, fmt.Errorf("invalid raw message data: %v", rawData)
	}
	wb := t.defaultWB
	defer wb.Clear()
	// batched
	leftBuf := rawData
	wsizeTotal := int32(0)
	var diskEnd diskQueueEndInfo
	diskEnd.totalMsgCnt = t.lastCnt
	diskEnd.virtualEnd = BackendOffset(t.lastOffset)
	for {
		if len(leftBuf) < 4 {
			return nil, fmt.Errorf("invalid raw message data: %v", rawData)
		}
		sz := int32(binary.BigEndian.Uint32(leftBuf[:4]))
		if sz <= 0 || sz > MAX_POSSIBLE_MSG_SIZE {
			// this file is corrupt and we have no reasonable guarantee on
			// where a new message should begin
			return nil, fmt.Errorf("invalid message read size (%d)", sz)
		}
		wend, wsize, err := t.putBatchedRaw(wb, int64(diskEnd.Offset()), diskEnd.TotalMsgCnt(), leftBuf[4:sz+4], 0)
		if err != nil {
			return nil, err
		}
		msgNum--
		wsizeTotal += wsize
		diskEnd.virtualEnd = wend
		diskEnd.totalMsgCnt = diskEnd.totalMsgCnt + 1
		leftBuf = leftBuf[sz+4:]
		if len(leftBuf) == 0 {
			break
		}
	}
	if checkSize > 0 && int64(wsizeTotal) != checkSize {
		return nil, fmt.Errorf("batch message size mismatch: %v vs %v", checkSize, wsizeTotal)
	}
	if msgNum != 0 {
		return nil, fmt.Errorf("should have the same message number in raw: %v", msgNum)
	}
	t.saveTopicMetaInBatch(wb, int64(diskEnd.Offset()), diskEnd.TotalMsgCnt())
	err := wb.Commit()
	if err != nil {
		return nil, err
	}
	t.lastOffset = int64(diskEnd.Offset())
	t.lastCnt = diskEnd.TotalMsgCnt()
	return &diskEnd, nil
}

func (t *KVTopic) PutMessages(msgs []*Message) (BackendOffset, int32, BackendQueueEnd, error) {
	wb := t.defaultWB
	defer wb.Clear()
	firstOffset := BackendOffset(t.lastOffset)
	var diskEnd diskQueueEndInfo
	diskEnd.totalMsgCnt = t.lastCnt
	diskEnd.virtualEnd = BackendOffset(t.lastOffset)
	batchBytes := int32(0)
	for _, m := range msgs {
		offset, bytes, err := t.putBatched(wb, int64(diskEnd.Offset()), diskEnd.TotalMsgCnt(), m, 0)
		if err != nil {
			return firstOffset, batchBytes, &diskEnd, err
		}
		diskEnd.totalMsgCnt = diskEnd.totalMsgCnt + 1
		diskEnd.virtualEnd = offset
		batchBytes += bytes
	}
	t.saveTopicMetaInBatch(wb, int64(diskEnd.Offset()), diskEnd.TotalMsgCnt())
	err := wb.Commit()
	if err != nil {
		return firstOffset, batchBytes, &diskEnd, err
	}
	t.lastOffset = int64(diskEnd.Offset())
	t.lastCnt = diskEnd.TotalMsgCnt()
	return firstOffset, batchBytes, &diskEnd, nil
}

func (t *KVTopic) putBatched(wb engine.WriteBatch, lastOffset int64, lastCnt int64, m *Message, checkSize int64) (BackendOffset, int32, error) {
	var writeEnd BackendOffset
	t.putBuffer.Reset()
	wsize, err := m.WriteTo(&t.putBuffer, t.IsExt())
	if err != nil {
		return writeEnd, 0, err
	}
	// there are 4bytes data length on disk.
	if checkSize > 0 && wsize+4 != checkSize {
		return writeEnd, 0, fmt.Errorf("message write size mismatch %v vs %v", checkSize, wsize+4)
	}
	writeEnd = BackendOffset(lastOffset) + BackendOffset(wsize+4)
	keyBuf := getKVKeyForMsgID(t.fullName, m.ID)
	wb.Put(keyBuf, t.putBuffer.Bytes())
	valBuf := encodeMsgIDOffsetCntValue(m.ID, lastOffset, lastCnt)
	if m.TraceID > 0 {
		keyBuf = getKVKeyForMsgTrace(t.fullName, m.ID, m.TraceID)
		wb.Put(keyBuf, valBuf)
	}
	keyBuf = getKVKeyForMsgOffset(t.fullName, lastOffset, int64(writeEnd))
	valueBuf := encodeMsgOffsetValue(m.ID, lastCnt, m.Timestamp)
	wb.Put(keyBuf, valueBuf)
	keyBuf = getKVKeyForMsgCnt(t.fullName, lastCnt)
	wb.Put(keyBuf, valBuf)
	keyBuf = getKVKeyForMsgTs(t.fullName, m.ID, m.Timestamp)
	wb.Put(keyBuf, valBuf)
	return writeEnd, int32(wsize + 4), nil
}

// note: the rawdata should not include the size header
func (t *KVTopic) putBatchedRaw(wb engine.WriteBatch, lastOffset int64, lastCnt int64, rawData []byte, checkSize int64) (BackendOffset, int32, error) {
	var writeEnd BackendOffset
	wsize := len(rawData)
	// there are 4bytes data length on disk.
	if checkSize > 0 && int64(wsize+4) != checkSize {
		return writeEnd, 0, fmt.Errorf("message write size mismatch %v vs %v", checkSize, wsize+4)
	}
	m, err := DecodeMessage(rawData, t.IsExt())
	// note, if the origin message is not ext, we should not write the ext to the data on replica
	if err != nil {
		return writeEnd, 0, err
	}
	if m.ID <= 0 {
		return writeEnd, 0, fmt.Errorf("message data invalid")
	}
	writeEnd = BackendOffset(lastOffset) + BackendOffset(wsize+4)
	keyBuf := getKVKeyForMsgID(t.fullName, m.ID)
	wb.Put(keyBuf, rawData)
	valBuf := encodeMsgIDOffsetCntValue(m.ID, lastOffset, lastCnt)
	if m.TraceID > 0 {
		keyBuf = getKVKeyForMsgTrace(t.fullName, m.ID, m.TraceID)
		wb.Put(keyBuf, valBuf)
	}
	keyBuf = getKVKeyForMsgOffset(t.fullName, lastOffset, int64(writeEnd))
	valueBuf := encodeMsgOffsetValue(m.ID, lastCnt, m.Timestamp)
	wb.Put(keyBuf, valueBuf)
	keyBuf = getKVKeyForMsgCnt(t.fullName, lastCnt)
	wb.Put(keyBuf, valBuf)
	keyBuf = getKVKeyForMsgTs(t.fullName, m.ID, m.Timestamp)
	wb.Put(keyBuf, valBuf)
	return writeEnd, int32(wsize + 4), nil
}

// this raw has no size header
func (t *KVTopic) putRaw(rawData []byte, offset BackendOffset, checkSize int64) (BackendOffset, int32, int64, error) {
	wend := t.lastOffset
	if wend != int64(offset) {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return 0, 0, 0, ErrWriteOffsetMismatch
	}
	wb := t.defaultWB
	defer wb.Clear()

	var writeCnt int64
	writeEnd, wsize, err := t.putBatchedRaw(wb, t.lastOffset, t.lastCnt, rawData, checkSize)
	if err != nil {
		return writeEnd, wsize, writeCnt, err
	}
	writeCnt = t.lastCnt + 1
	t.saveTopicMetaInBatch(wb, int64(writeEnd), writeCnt)
	// add tag if ext has tag info
	err = wb.Commit()
	if err != nil {
		return writeEnd, 0, writeCnt, err
	}
	t.lastOffset = int64(writeEnd)
	t.lastCnt = writeCnt
	return writeEnd, int32(wsize), writeCnt, nil
}

func (t *KVTopic) put(m *Message, checkSize int64) (BackendOffset, int32, int64, error) {
	wb := t.defaultWB
	defer wb.Clear()

	var writeCnt int64
	writeEnd, wsize, err := t.putBatched(wb, t.lastOffset, t.lastCnt, m, checkSize)
	if err != nil {
		return writeEnd, wsize, writeCnt, err
	}
	writeCnt = t.lastCnt + 1
	t.saveTopicMetaInBatch(wb, int64(writeEnd), writeCnt)
	// add tag if ext has tag info
	err = wb.Commit()
	if err != nil {
		return writeEnd, 0, writeCnt, err
	}
	t.lastOffset = int64(writeEnd)
	t.lastCnt = writeCnt
	return writeEnd, int32(wsize), writeCnt, nil
}

// Delete empties the topic and all its channels and closes
func (t *KVTopic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *KVTopic) Close() error {
	return t.exit(false)
}

func (t *KVTopic) exit(deleted bool) error {
	if deleted {
		// empty the queue (deletes the backend files, too)
		t.Empty()
	}

	t.defaultWB.Destroy()
	return nil
}

func (t *KVTopic) Empty() error {
	t.tpLog.Logf("TOPIC empty")
	startKey := getKVKeyForTopicBegin(t.fullName)
	endKey := getKVKeyForTopicEnd(t.fullName)
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	// note the meta will be deleted also
	// maybe we can padding magic code in the topic name, so
	// we can auto lazy delete (recreate with magic code changed)
	wb.DeleteRange(startKey, endKey)
	err := wb.Commit()
	if err != nil {
		return err
	}
	t.lastOffset = 0
	t.lastCnt = 0
	return nil
}

// maybe should return the cleaned offset to allow commit log clean
func (t *KVTopic) TryCleanOldData(retentionSize int64, cleanEndInfo BackendQueueOffset, maxCleanOffset BackendOffset) error {
	// clean the data that has been consumed and keep the retention policy
	if cleanEndInfo == nil || cleanEndInfo.Offset()+BackendOffset(retentionSize) >= maxCleanOffset {
		if cleanEndInfo != nil {
			t.tpLog.Infof("clean topic data at position: %v could not exceed max clean end: %v",
				cleanEndInfo, maxCleanOffset)
		}
		return nil
	}
	// clean data old then cleanEndInfo

	return t.CleanBackendWithQueueStart(int64(cleanEndInfo.Offset()))
}

func (t *KVTopic) ResetBackendWithQueueStart(queueStartOffset int64, totalCnt int64) error {
	t.tpLog.Warningf("reset the topic backend with queue start: %v, %v", queueStartOffset, totalCnt)
	err := t.Empty()
	if err != nil {
		return err
	}
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	t.saveTopicMetaInBatch(wb, queueStartOffset, totalCnt)
	err = wb.Commit()
	if err != nil {
		return err
	}
	t.lastOffset = queueStartOffset
	t.lastCnt = totalCnt
	return nil
}

func (t *KVTopic) CleanBackendWithQueueStart(queueStartOffset int64) error {
	t.tpLog.Infof("clean with queue start: %v", queueStartOffset)
	// delete the data old than queueStartOffset
	// tag info or trace key info currently not cleaned
	msgid, msgCnt, msgTs, _, err := t.getMsgIDCntTsLessThanOffset(queueStartOffset)
	if err != nil {
		return err
	}
	minKey := getKVKeyForMsgOffset(t.fullName, int64(0), 0)
	maxKey := getKVKeyForMsgOffset(t.fullName, int64(queueStartOffset), 0)
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgID(t.fullName, MessageID(0))
	maxKey = getKVKeyForMsgID(t.fullName, MessageID(msgid))
	wb.DeleteRange(minKey, maxKey)
	wb.Delete(maxKey)

	minKey = getKVKeyForMsgCnt(t.fullName, 0)
	maxKey = getKVKeyForMsgCnt(t.fullName, msgCnt)
	wb.DeleteRange(minKey, maxKey)
	wb.Delete(maxKey)

	minKey = getKVKeyForMsgTs(t.fullName, 0, 0)
	maxKey = getKVKeyForMsgTs(t.fullName, MessageID(msgid), msgTs)
	wb.DeleteRange(minKey, maxKey)
	wb.Delete(maxKey)
	err = wb.Commit()
	return err
}

func (t *KVTopic) GetMsgRawByID(id MessageID) ([]byte, error) {
	key := getKVKeyForMsgID(t.fullName, id)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrMsgNotFoundInIndex
	}
	return v, nil
}

func (t *KVTopic) GetMsgByID(id MessageID) (*Message, error) {
	v, err := t.GetMsgRawByID(id)
	if err != nil {
		return nil, err
	}
	return DecodeMessage(v, t.IsExt())
}

func (t *KVTopic) GetMsgByTraceID(tid uint64, limit int) ([]*Message, error) {
	if limit > maxBatchSize {
		return nil, errBatchSizeLimit
	}
	ids := make([]MessageID, 0, 3)
	err := func() error {
		minKey := getKVKeyForMsgTrace(t.fullName, 0, tid)
		maxKey := getKVKeyForMsgTraceIDEnd(t.fullName, tid)
		itopts := engine.IteratorOpts{}
		itopts.Min = minKey
		itopts.Max = maxKey
		itopts.Type = engine.RangeOpen
		it, err := t.kvEng.GetIterator(itopts)
		if err != nil {
			return err
		}
		defer it.Close()
		it.SeekToFirst()
		for ; it.Valid(); it.Next() {
			k := it.RefKey()
			_, ftid, msgid, err := decodeMsgTraceKey(k)
			if err != nil {
				continue
			}
			if ftid > tid {
				break
			}
			if ftid != tid {
				continue
			}
			ids = append(ids, msgid)
			if len(ids) >= limit {
				break
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	msgs := make([]*Message, 0, len(ids))
	for _, msgid := range ids {
		m, err := t.GetMsgByID(msgid)
		if err != nil {
			continue
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (t *KVTopic) GetMsgByOffset(offset int64) (*Message, error) {
	msgid, _, _, err := t.getMsgIDCntTsAtOffset(offset)
	if err != nil {
		return nil, err
	}
	return t.GetMsgByID(MessageID(msgid))
}

func (t *KVTopic) GetMsgIDOffsetByCnt(cnt int64) (MessageID, int64, error) {
	key := getKVKeyForMsgCnt(t.fullName, cnt)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return 0, 0, err
	}
	if v == nil {
		return 0, 0, ErrMsgNotFoundInIndex
	}
	id, offset, _, err := decodeMsgIDOffsetCntValue(v)
	if err != nil {
		return 0, 0, err
	}
	return id, offset, nil
}

func (t *KVTopic) GetMsgRawByCnt(cnt int64) ([]byte, error) {
	id, _, err := t.GetMsgIDOffsetByCnt(cnt)
	if err != nil {
		return nil, err
	}
	return t.GetMsgRawByID(id)
}

func (t *KVTopic) GetMsgByCnt(cnt int64) (*Message, int64, error) {
	id, offset, err := t.GetMsgIDOffsetByCnt(cnt)
	if err != nil {
		return nil, offset, err
	}
	msg, err := t.GetMsgByID(id)
	return msg, offset, err
}

func (t *KVTopic) GetMsgByTime(ts int64, limit int) ([]*Message, error) {
	if limit > maxBatchSize {
		return nil, errBatchSizeLimit
	}
	return nil, nil
}

func (t *KVTopic) PullMsgByCntFrom(cnt int64, limit int64) ([]*Message, error) {
	if limit > maxBatchSize {
		return nil, errBatchSizeLimit
	}
	if limit <= 0 {
		return nil, errInvalidLimit
	}
	id, _, err := t.GetMsgIDOffsetByCnt(cnt)
	if err != nil {
		if err == ErrMsgNotFoundInIndex {
			return nil, nil
		}
		return nil, err
	}
	_, lastCnt, err := t.GetTopicMeta()
	if err != nil {
		return nil, err
	}
	if lastCnt <= cnt {
		t.tpLog.Warningf("total count %v less than count %v, but have cnt key: %v", t.lastCnt, cnt, id)
		return nil, nil
	}
	minKey := getKVKeyForMsgID(t.fullName, id)
	maxKey := getKVKeyForMsgIDEnd(t.fullName)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeClose
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	it.SeekToFirst()
	sz := limit
	if lastCnt-cnt < sz {
		sz = t.lastCnt - cnt
	}
	msgs := make([]*Message, 0, sz)
	for ; it.Valid(); it.Next() {
		m, err := DecodeMessage(it.Value(), t.IsExt())
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
		if int64(len(msgs)) >= limit {
			break
		}
	}
	return msgs, nil
}
