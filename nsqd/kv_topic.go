package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

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

func encodeMsgIDValue(msgid MessageID) []byte {
	idBuf := make([]byte, 0, 8+1)
	idBuf, _ = codec.EncodeMemCmpKey(idBuf, uint64(msgid))
	return idBuf
}

func decodeMsgIDValue(buf []byte) (MessageID, error) {
	vals, err := codec.Decode(buf, 1)
	if err != nil {
		return 0, err
	}
	if len(vals) < 1 {
		return 0, errInvalidEncodedData
	}
	id := vals[0].(uint64)
	return MessageID(id), nil
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
	kvEng     engine.KVEngine
	tname     string
	fullName  string
	partition int
	dataPath  string

	option     *Options
	putBuffer  bytes.Buffer
	bp         sync.Pool
	isExt      int32
	tpLog      *levellogger.LevelLogger
	lastOffset int64
	lastCnt    int64
}

func NewKVTopic(topicName string, part int, opt *Options) *KVTopic {
	return NewKVTopicWithExt(topicName, part, false, opt)
}

func NewKVTopicWithExt(topicName string, part int, ext bool, opt *Options) *KVTopic {
	if part > MAX_TOPIC_PARTITION {
		return nil
	}
	t := &KVTopic{
		tname:     topicName,
		partition: part,
		option:    opt,
		putBuffer: bytes.Buffer{},
	}
	t.fullName = GetTopicFullName(t.tname, t.partition)
	t.tpLog = nsqLog.WrappedWithPrefix("["+t.fullName+"]", 0)

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
	cfg := engine.NewRockConfig()
	cfg.MaxWriteBufferNumber = 4
	cfg.DataDir = path.Join(t.dataPath, backendName)
	eng, err := engine.NewKVEng(cfg)
	if err != nil {
		t.tpLog.LogErrorf("failed to create engine: %v ", err)
		return nil
	}
	err = eng.OpenEng()
	if err != nil {
		t.tpLog.LogErrorf("failed to create engine: %v ", err)
		return nil
	}
	t.kvEng = eng
	offset, cnt, err := t.GetTopicMeta()
	if err != nil {
		t.tpLog.LogErrorf("failed to init topic meta: %s ", err)
		return nil
	}
	t.lastOffset = offset
	t.lastCnt = cnt
	return t
}

func (t *KVTopic) IsExt() bool {
	return true
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

func (t *KVTopic) SaveTopicMeta(offset int64, cnt int64) error {
	wb := t.kvEng.DefaultWriteBatch()
	defer wb.Clear()
	t.saveTopicMetaInBatch(wb, offset, cnt)
	return wb.Commit()
}

func (t *KVTopic) saveTopicMetaInBatch(wb engine.WriteBatch, offset int64, cnt int64) {
	key := getKVKeyForTopicMeta(t.fullName)
	buf := encodeTopicMetaValue(offset, cnt)
	wb.Put(key, buf)
}

func (t *KVTopic) ResetBackendEnd(vend BackendOffset, totalCnt int64) error {
	// tag info or trace key info currently not cleaned
	minKey := getKVKeyForMsgOffset(t.fullName, int64(vend), 0)
	maxKey := getKVKeyForMsgOffsetEnd(t.fullName)
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
	if !it.Valid() {
		return nil
	}
	v := it.RefValue()
	msgid, msgCnt, _, err := decodeMsgOffsetValue(v)
	if err != nil {
		return err
	}
	if msgCnt != totalCnt {
		t.tpLog.Warningf("total count %v not matched with db %v", totalCnt, msgCnt)
	}
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgID(t.fullName, MessageID(msgid))
	maxKey = getKVKeyForMsgIDEnd(t.fullName)
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgCnt(t.fullName, msgCnt)
	maxKey = getKVKeyForMsgCntEnd(t.fullName)
	wb.DeleteRange(minKey, maxKey)

	t.saveTopicMetaInBatch(wb, int64(vend), totalCnt)
	//minKey = getKVKeyForMsgTs(t.fullName, MessageID(msgid), msgTs)
	//maxKey = getKVKeyForMsgTsEnd(t.fullName)
	//wb.DeleteRange(minKey, maxKey)
	err = wb.Commit()
	if err != nil {
		return err
	}
	t.lastOffset = int64(vend)
	t.lastCnt = totalCnt
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

func (t *KVTopic) PutRawDataOnReplica(rawData []byte, offset BackendOffset, checkSize int64, msgNum int32) (BackendQueueEnd, error) {
	wend := t.lastOffset
	if wend != int64(offset) {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	if len(rawData) < 4 {
		return nil, fmt.Errorf("invalid raw message data: %v", rawData)
	}
	if msgNum == 1 {
		m, err := DecodeMessage(rawData[4:], t.IsExt())
		if err != nil {
			return nil, err
		}
		writeBytes, dend, err := t.PutMessage(m)
		if err != nil {
			t.tpLog.LogErrorf("topic write to disk error: %v, %v", offset, err.Error())
			return dend, err
		}
		if checkSize > 0 && int64(writeBytes) != checkSize {
			return dend, fmt.Errorf("message write size mismatch %v vs %v", checkSize, writeBytes)
		}
		return dend, nil
	} else {
		// batched
		msgs := make([]*Message, 0, msgNum)
		leftBuf := rawData
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

			m, err := DecodeMessage(leftBuf[4:sz+4], t.IsExt())
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, m)
			leftBuf = leftBuf[sz+4:]
			if len(leftBuf) == 0 {
				break
			}
		}
		return t.PutMessagesOnReplica(msgs, offset, checkSize)
	}
}

func (t *KVTopic) PutMessageOnReplica(m *Message, offset BackendOffset, checkSize int64) (BackendQueueEnd, error) {
	wend := t.lastOffset
	if wend != int64(offset) {
		t.tpLog.LogErrorf("topic write offset mismatch: %v, %v", offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	endOffset, _, dendCnt, err := t.put(m, checkSize)
	if err != nil {
		return nil, err
	}
	dn := &diskQueueEndInfo{totalMsgCnt: dendCnt}
	dn.virtualEnd = endOffset
	return dn, nil
}

func (t *KVTopic) PutMessagesOnReplica(msgs []*Message, offset BackendOffset, checkSize int64) (BackendQueueEnd, error) {
	wend := t.lastOffset
	if wend != int64(offset) {
		t.tpLog.LogErrorf(
			"TOPIC write message offset mismatch %v, %v",
			offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	_, wsizeTotal, dend, err := t.PutMessages(msgs)
	if err != nil {
		return dend, err
	}
	if checkSize > 0 && int64(wsizeTotal) != checkSize {
		return nil, fmt.Errorf("batch message size mismatch: %v vs %v", checkSize, wsizeTotal)
	}
	return dend, nil
}

func (t *KVTopic) PutMessages(msgs []*Message) (BackendOffset, int32, BackendQueueEnd, error) {
	wb := t.kvEng.DefaultWriteBatch()
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
	idBuf := encodeMsgIDValue(m.ID)
	if m.TraceID > 0 {
		keyBuf = getKVKeyForMsgTrace(t.fullName, m.ID, m.TraceID)
		wb.Put(keyBuf, idBuf)
	}
	keyBuf = getKVKeyForMsgOffset(t.fullName, lastOffset, int64(writeEnd))
	valueBuf := encodeMsgOffsetValue(m.ID, lastCnt, m.Timestamp)
	wb.Put(keyBuf, valueBuf)
	keyBuf = getKVKeyForMsgCnt(t.fullName, lastCnt)
	wb.Put(keyBuf, idBuf)
	keyBuf = getKVKeyForMsgTs(t.fullName, m.ID, m.Timestamp)
	wb.Put(keyBuf, idBuf)
	return writeEnd, int32(wsize + 4), nil
}

func (t *KVTopic) put(m *Message, checkSize int64) (BackendOffset, int32, int64, error) {
	wb := t.kvEng.DefaultWriteBatch()
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

	t.kvEng.CloseAll()
	return nil
}

func (t *KVTopic) Empty() error {
	t.tpLog.Logf("TOPIC empty")
	startKey := getKVKeyForTopicBegin(t.fullName)
	endKey := getKVKeyForTopicEnd(t.fullName)
	wb := t.kvEng.NewWriteBatch()
	wb.DeleteRange(startKey, endKey)
	t.saveTopicMetaInBatch(wb, int64(0), 0)
	defer wb.Clear()
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

	return t.ResetBackendWithQueueStart(int64(cleanEndInfo.Offset()))
}

func (t *KVTopic) ResetBackendWithQueueStart(queueStartOffset int64) error {
	t.tpLog.Warningf("reset the topic backend with queue start: %v", queueStartOffset)
	// delete the data old than queueStartOffset
	// tag info or trace key info currently not cleaned
	minKey := getKVKeyForMsgOffset(t.fullName, int64(0), 0)
	maxKey := getKVKeyForMsgOffset(t.fullName, int64(queueStartOffset), 0)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeOpen
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return err
	}
	defer it.Close()
	it.SeekToLast()
	if !it.Valid() {
		return nil
	}
	v := it.RefValue()
	msgid, msgCnt, msgTs, err := decodeMsgOffsetValue(v)
	if err != nil {
		return err
	}
	wb := t.kvEng.NewWriteBatch()
	defer wb.Destroy()
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgID(t.fullName, MessageID(0))
	maxKey = getKVKeyForMsgID(t.fullName, MessageID(msgid))
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgCnt(t.fullName, 0)
	maxKey = getKVKeyForMsgCnt(t.fullName, msgCnt)
	wb.DeleteRange(minKey, maxKey)

	minKey = getKVKeyForMsgTs(t.fullName, 0, 0)
	maxKey = getKVKeyForMsgTs(t.fullName, MessageID(msgid), msgTs)
	wb.DeleteRange(minKey, maxKey)
	err = wb.Commit()
	return err
}

func (t *KVTopic) GetMsgByID(id MessageID) (*Message, error) {
	key := getKVKeyForMsgID(t.fullName, id)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrMsgNotFoundInIndex
	}
	return DecodeMessage(v, t.IsExt())
}

func (t *KVTopic) GetMsgByTraceID(tid uint64, limit int) ([]*Message, error) {
	if limit > maxBatchSize {
		return nil, errBatchSizeLimit
	}
	minKey := getKVKeyForMsgTrace(t.fullName, 0, tid)
	maxKey := getKVKeyForMsgTraceIDEnd(t.fullName, tid)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeOpen
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	it.SeekToFirst()
	msgs := make([]*Message, 0, 3)
	for ; it.Valid(); it.Next() {
		k := it.RefKey()
		_, ftid, msgid, err := decodeMsgTraceKey(k)
		if err != nil {
			return nil, err
		}
		if ftid != tid {
			continue
		}
		m, err := t.GetMsgByID(msgid)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
		if len(msgs) >= limit {
			break
		}
	}
	return msgs, nil
}

func (t *KVTopic) GetMsgByOffset(offset int64) (*Message, error) {
	minKey := getKVKeyForMsgOffset(t.fullName, int64(offset), 0)
	maxKey := getKVKeyForMsgOffsetEnd(t.fullName)
	itopts := engine.IteratorOpts{}
	itopts.Min = minKey
	itopts.Max = maxKey
	itopts.Type = engine.RangeOpen
	it, err := t.kvEng.GetIterator(itopts)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	it.SeekToFirst()
	if !it.Valid() {
		return nil, ErrMsgNotFoundInIndex
	}
	v := it.RefValue()
	if v == nil {
		return nil, ErrMsgNotFoundInIndex
	}
	msgid, _, _, err := decodeMsgOffsetValue(v)
	if err != nil {
		return nil, err
	}
	return t.GetMsgByID(MessageID(msgid))
}

func (t *KVTopic) GetMsgByCnt(cnt int64) (*Message, error) {
	key := getKVKeyForMsgCnt(t.fullName, cnt)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrMsgNotFoundInIndex
	}
	id, err := decodeMsgIDValue(v)
	if err != nil {
		return nil, err
	}
	return t.GetMsgByID(id)
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
	key := getKVKeyForMsgCnt(t.fullName, cnt)
	v, err := t.kvEng.GetBytes(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	id, err := decodeMsgIDValue(v)
	if err != nil {
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
