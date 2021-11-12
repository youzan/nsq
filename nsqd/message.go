package nsqd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fastjson"
	"github.com/youzan/nsq/internal/ext"
)

const (
	MsgIDLength         = 16
	MsgTraceIDLength    = 8
	MsgJsonHeaderLength = 2
	minValidMsgLength   = MsgIDLength + 8 + 2 // Timestamp + Attempts
	MaxAttempts         = 4000
	extMsgHighBits      = 0xa000
)

// the new message total id will be ID+TraceID, the length is same with old id
// slice, the traceid only used for trace for business, the ID is used for internal.
// In order to be compatible with old format, we keep the attempts field.
type FullMessageID [MsgIDLength]byte
type MessageID uint64

func GetMessageIDFromFullMsgID(id FullMessageID) MessageID {
	return MessageID(binary.BigEndian.Uint64(id[:8]))
}

func GetTraceIDFromFullMsgID(id FullMessageID) uint64 {
	return binary.BigEndian.Uint64(id[8 : 8+MsgTraceIDLength])
}

func PrintMessage(m *Message) string {
	if m == nil {
		return ""
	}
	return fmt.Sprintf("%v %v %s %v %v %v %v %v %v %v %v %v, %v %v %v",
		m.ID, m.TraceID, string(m.Body), m.Timestamp, m.Attempts(), m.deliveryTS,
		m.pri, m.index, m.deferredCnt, m.Offset, m.RawMoveSize, m.queueCntIndex,
		m.DelayedType, m.DelayedTs, m.DelayedChannel,
	)
}

func PrintMessageNoBody(m *Message) string {
	if m == nil {
		return ""
	}
	return fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v, %v %v %v",
		m.ID, m.TraceID, m.Timestamp, m.Attempts(), m.deliveryTS,
		m.pri, m.index, m.deferredCnt, m.Offset, m.RawMoveSize, m.queueCntIndex,
		m.DelayedType, m.DelayedTs, m.DelayedChannel,
	)
}

type Message struct {
	ID        MessageID
	TraceID   uint64
	Body      []byte
	Timestamp int64
	attempts  uint32
	ExtBytes  []byte
	ExtVer    ext.ExtVer

	// for in-flight handling
	deliveryTS time.Time
	//clientID    int64
	belongedConsumer Consumer
	pri              int64
	index            int
	deferredCnt      int32
	timedoutCnt      int32
	//for backend queue
	Offset        BackendOffset
	RawMoveSize   BackendOffset
	queueCntIndex int64
	// for delayed queue message
	// 1 - delayed message by channel
	// 2 - delayed pub
	//
	DelayedType    int32
	DelayedTs      int64
	DelayedOrigID  MessageID
	DelayedChannel string
	// will be used for delayed pub. (json data to tell different type of delay)
	DelayedData []byte
}

func MessageHeaderBytes() int {
	return MsgIDLength + 8 + 2
}

func NewMessageWithExt(id MessageID, body []byte, extVer ext.ExtVer, extBytes []byte) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		ExtVer:    extVer,
		ExtBytes:  extBytes,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func NewMessageWithTs(id MessageID, body []byte, ts int64) *Message {
	return &Message{
		ID:        id,
		TraceID:   0,
		Body:      body,
		Timestamp: ts,
	}
}

func (m *Message) GetFullMsgID() FullMessageID {
	var buf FullMessageID
	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	return buf
}

func (m *Message) IsDeferred() bool {
	return atomic.LoadInt32(&m.deferredCnt) > 0
}

func (m *Message) GetCopy() *Message {
	newMsg := *m
	newMsg.Body = make([]byte, len(m.Body))
	copy(newMsg.Body, m.Body)
	if m.ExtBytes != nil {
		newMsg.ExtBytes = make([]byte, len(m.ExtBytes))
		copy(newMsg.ExtBytes, m.ExtBytes)
	}
	if m.DelayedData != nil {
		newMsg.DelayedData = make([]byte, len(m.DelayedData))
		copy(newMsg.DelayedData, m.DelayedData)
	}
	return &newMsg
}

func (m *Message) GetClientID() int64 {
	if m.belongedConsumer != nil {
		return m.belongedConsumer.GetID()
	}
	return 0
}

func (m *Message) WriteToClient(w io.Writer, writeExt bool, writeDetail bool) (int64, error) {
	// for client, we no need write the compatible version info to message
	return m.internalWriteTo(w, writeExt, false, writeDetail)
}

func (m *Message) WriteTo(w io.Writer, writeExt bool) (int64, error) {
	return m.internalWriteTo(w, writeExt, true, false)
}

func (m *Message) Attempts() uint16 {
	at := atomic.LoadUint32(&m.attempts)
	if at > MaxAttempts {
		return MaxAttempts
	}
	return uint16(at)
}

func (m *Message) IncrAttempts(delta int) {
	if m.Attempts() < MaxAttempts || delta < 0 {
		atomic.AddUint32(&m.attempts, uint32(delta))
	}
}

func (m *Message) SetAttempts(at uint16) {
	if at > MaxAttempts {
		at = MaxAttempts
	}
	atomic.StoreUint32(&m.attempts, uint32(at))
}

func (m *Message) internalWriteTo(w io.Writer, writeExt bool, writeCompatible bool, writeDetail bool) (int64, error) {
	var buf [16]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	combined := m.Attempts()
	if writeExt && writeCompatible {
		combined += uint16(extMsgHighBits)
	}
	binary.BigEndian.PutUint16(buf[8:10], combined)

	n, err := w.Write(buf[:10])
	total += int64(n)
	if err != nil {
		return total, err
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	n, err = w.Write(buf[:MsgIDLength])
	total += int64(n)
	if err != nil {
		return total, err
	}

	//write ext content
	if writeExt {
		//write ext version
		buf[0] = byte(m.ExtVer)
		n, err = w.Write(buf[:1])
		total += int64(n)
		if err != nil {
			return total, err
		}

		if m.ExtVer != ext.NO_EXT_VER {
			if len(m.ExtBytes) >= ext.MaxExtLen {
				return total, errors.New("extend data exceed the limit")
			}
			binary.BigEndian.PutUint16(buf[1:1+2], uint16(len(m.ExtBytes)))
			n, err = w.Write(buf[1 : 1+2])
			total += int64(n)
			if err != nil {
				return total, err
			}

			n, err = w.Write(m.ExtBytes)
			total += int64(n)
			if err != nil {
				return total, err
			}
		}
	}

	if writeDetail {
		binary.BigEndian.PutUint64(buf[:8], uint64(m.Offset))
		binary.BigEndian.PutUint32(buf[8:8+4], uint32(m.RawMoveSize))
		n, err = w.Write(buf[:8+4])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func (m *Message) WriteDelayedTo(w io.Writer, writeExt bool) (int64, error) {
	combined := m.Attempts()
	if writeExt {
		combined += uint16(extMsgHighBits)
	}

	var buf [32]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], combined)

	n, err := w.Write(buf[:10])
	total += int64(n)
	if err != nil {
		return total, err
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(m.ID))
	binary.BigEndian.PutUint64(buf[8:8+MsgTraceIDLength], uint64(m.TraceID))
	n, err = w.Write(buf[:MsgIDLength])
	total += int64(n)
	if err != nil {
		return total, err
	}

	pos := 0
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(m.DelayedType))
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(m.DelayedTs))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(m.DelayedOrigID))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(m.DelayedChannel)))
	pos += 4
	n, err = w.Write(buf[:pos])
	total += int64(n)
	if err != nil {
		return total, err
	}
	n, err = w.Write([]byte(m.DelayedChannel))
	total += int64(n)
	if err != nil {
		return total, err
	}
	if m.DelayedType == PubDelayed {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(m.DelayedData)))
		n, err = w.Write(buf[:4])
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(m.DelayedData)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	//write ext content
	if writeExt {
		//write ext version
		buf[0] = byte(m.ExtVer)
		n, err = w.Write(buf[:1])
		total += int64(n)
		if err != nil {
			return total, err
		}

		if m.ExtVer != ext.NO_EXT_VER {
			if len(m.ExtBytes) >= ext.MaxExtLen {
				return total, errors.New("extend data exceed the limit")
			}
			binary.BigEndian.PutUint16(buf[:2], uint16(len(m.ExtBytes)))
			n, err = w.Write(buf[:2])
			total += int64(n)
			if err != nil {
				return total, err
			}
			n, err = w.Write(m.ExtBytes)
			total += int64(n)
			if err != nil {
				return total, err
			}
		}
	}

	if m.DelayedType == ChannelDelayed {
		// some data with original queue info
		binary.BigEndian.PutUint64(buf[:8], uint64(m.Offset))
		binary.BigEndian.PutUint32(buf[8:8+4], uint32(m.RawMoveSize))
		n, err = w.Write(buf[:8+4])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func DecodeMessage(b []byte, ext bool) (*Message, error) {
	return decodeMessage(b, ext)
}

// note: the message body is using the origin buffer, so never modify the buffer after decode.
// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte, isExt bool) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	combined := binary.BigEndian.Uint16(b[8:10])
	msg.ID = MessageID(binary.BigEndian.Uint64(b[10:18]))
	msg.TraceID = binary.BigEndian.Uint64(b[18:26])
	// we reused high 4-bits of attempts for message version to make it compatible with ext message and any other future change.
	// there some cases
	// 1. no ext topic, all high 4-bits should be 0
	// 2. ext topic, during update some old message has all high 4-bits with 0, and new message should equal 0xa for ext.
	// 3. do we need handle new message with ext but have all high 4-bits with 0?
	// 4. do we need handle some old attempts which maybe exceed the MaxAttempts? (very small possible)
	// 5. if any future change, hight 4-bits can be 0xb, 0xc, 0xd, 0xe (0x1~0x9 should be reserved for future)
	var highBits uint16
	if combined > MaxAttempts {
		highBits = combined & uint16(0xF000)
		msg.SetAttempts(combined & uint16(0x0FFF))
	} else {
		msg.SetAttempts(combined)
	}

	bodyStart := 26
	if highBits == extMsgHighBits && !isExt {
		// may happened while upgrading and the channel read the new data while ext flag not setting
		return nil, fmt.Errorf("invalid message, has the ext high bits but not decode as ext data: %v", b)
	}

	if isExt && highBits == extMsgHighBits {
		if len(b) < 27 {
			return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
		}
		extVer := ext.ExtVer(uint8(b[bodyStart]))
		msg.ExtVer = extVer
		switch extVer {
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
			bodyStart = 27
		default:
			if len(b) < 27+2 {
				return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
			}

			extLen := binary.BigEndian.Uint16(b[27 : 27+2])
			if len(b) < 27+2+int(extLen) {
				return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
			}
			msg.ExtBytes = b[29 : 29+extLen]
			bodyStart = 29 + int(extLen)
		}
	}

	msg.Body = b[bodyStart:]
	return &msg, nil
}

func DecodeDelayedMessage(b []byte, isExt bool) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	pos := 0
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[pos:8]))
	pos += 8
	combined := binary.BigEndian.Uint16(b[pos : pos+2])
	pos += 2
	msg.ID = MessageID(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	msg.TraceID = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8

	if len(b) < minValidMsgLength+4+8+8+4 {
		return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
	}

	msg.DelayedType = int32(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
	msg.DelayedTs = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	msg.DelayedOrigID = MessageID(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	nameLen := binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4

	if len(b) < pos+int(nameLen) {
		return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
	}

	msg.DelayedChannel = string(b[pos : pos+int(nameLen)])
	pos += int(nameLen)
	if msg.DelayedType == PubDelayed {
		if len(b) < pos+4 {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}
		dlen := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		if len(b) < pos+int(dlen) {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}
		msg.DelayedData = b[pos : pos+int(dlen)]
		pos += int(dlen)
	}
	var highBits uint16
	if combined > MaxAttempts {
		highBits = combined & uint16(0xF000)
		msg.SetAttempts(combined & uint16(0x0FFF))
	} else {
		msg.SetAttempts(combined)
	}

	if highBits == extMsgHighBits && !isExt {
		return nil, fmt.Errorf("invalid message, has the ext high bits but not decode as ext data: %v", b)
	}

	if isExt && highBits == extMsgHighBits {
		if len(b) < pos+1 {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}

		extVer := ext.ExtVer(uint8(b[pos]))
		msg.ExtVer = extVer
		pos++
		switch extVer {
		case ext.NO_EXT_VER:
			msg.ExtVer = ext.NO_EXT_VER
		default:
			if len(b) < pos+2 {
				return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
			}
			extLen := binary.BigEndian.Uint16(b[pos : pos+2])
			pos += 2
			if len(b) < pos+int(extLen) {
				return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
			}
			msg.ExtBytes = b[pos : pos+int(extLen)]
			pos += int(extLen)
		}
	}

	if msg.DelayedType == ChannelDelayed {
		if len(b) < pos+8+4 {
			return nil, fmt.Errorf("invalid delayed message buffer size (%d)", len(b))
		}
		// some data with original queue info
		msg.Offset = BackendOffset(binary.BigEndian.Uint64(b[pos : pos+8]))
		pos += 8
		msg.RawMoveSize = BackendOffset(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
	}
	msg.Body = b[pos:]
	return &msg, nil
}

var errJsonKeyNotFound = errors.New("json key not found")
var errJsonTypeNotMatch = errors.New("json value type not match")

func IsNotFoundJsonKey(err error) bool {
	return errJsonKeyNotFound == err
}

type IJsonExt interface {
	KeysCheck(func(string) bool)
	GetString(string) (string, error)
	GetBool(string) (bool, error)
	GetBoolOrStringBool(string) (bool, error)
}

func NewJsonExt(b []byte) (IJsonExt, error) {
	return newJsonExtObjV2(b)
	//return &JsonExtObj{
	//	jsonExt: jsoniter.Get(b),
	//}, nil
}

type JsonExtObj struct {
	jsonExt jsoniter.Any
}

func (jeo *JsonExtObj) KeysCheck(ck func(key string) bool) {
	if jeo.jsonExt == nil {
		return
	}
	if jeo.jsonExt.LastError() != nil {
		return
	}
	keys := jeo.jsonExt.Keys()
	for _, k := range keys {
		run := ck(k)
		if !run {
			break
		}
	}
}

func (jeo *JsonExtObj) GetString(key string) (string, error) {
	if jeo.jsonExt == nil {
		return "", errJsonKeyNotFound
	}
	if jeo.jsonExt.LastError() != nil {
		return "", jeo.jsonExt.LastError()
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader.LastError() != nil {
		return "", errJsonKeyNotFound
	}
	if extHeader.ValueType() == jsoniter.StringValue {
		return extHeader.ToString(), nil
	}
	return "", errJsonTypeNotMatch
}

func (jeo *JsonExtObj) GetBool(key string) (bool, error) {
	if jeo.jsonExt == nil {
		return false, errJsonKeyNotFound
	}
	if jeo.jsonExt.LastError() != nil {
		return false, jeo.jsonExt.LastError()
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader.LastError() != nil {
		return false, errJsonKeyNotFound
	}
	if extHeader.ValueType() == jsoniter.BoolValue {
		return extHeader.ToBool(), nil
	}
	return false, errJsonTypeNotMatch
}

func (jeo *JsonExtObj) GetBoolOrStringBool(key string) (bool, error) {
	if jeo.jsonExt == nil {
		return false, errJsonKeyNotFound
	}
	if jeo.jsonExt.LastError() != nil {
		return false, jeo.jsonExt.LastError()
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader.LastError() != nil {
		return false, errJsonKeyNotFound
	}
	if extHeader.ValueType() == jsoniter.BoolValue {
		return extHeader.ToBool(), nil
	}
	if extHeader.ValueType() == jsoniter.StringValue {
		ts := extHeader.ToString()
		tb := false
		if ts != "" {
			tb, _ = strconv.ParseBool(ts)
		}
		return tb, nil
	}
	return false, errJsonTypeNotMatch
}

type jsonExtObjV2 struct {
	jsonExt *fastjson.Value
}

func newJsonExtObjV2(b []byte) (IJsonExt, error) {
	v, err := fastjson.ParseBytes(b)
	if err != nil {
		return nil, err
	}
	return &jsonExtObjV2{
		jsonExt: v,
	}, nil
}
func (jeo *jsonExtObjV2) KeysCheck(ck func(string) bool) {
	if jeo.jsonExt == nil {
		return
	}
	o, err := jeo.jsonExt.Object()
	if err != nil {
		return
	}
	o.Visit(func(k []byte, v *fastjson.Value) {
		ck(string(k))
	})
}

func (jeo *jsonExtObjV2) GetString(key string) (string, error) {
	if jeo.jsonExt == nil {
		return "", errJsonKeyNotFound
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader == nil {
		return "", errJsonKeyNotFound
	}
	if extHeader.Type() != fastjson.TypeString {
		return "", errJsonTypeNotMatch
	}
	v, err := extHeader.StringBytes()
	return string(v), err
}

func (jeo *jsonExtObjV2) GetBool(key string) (bool, error) {
	if jeo.jsonExt == nil {
		return false, errJsonKeyNotFound
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader == nil {
		return false, errJsonKeyNotFound
	}
	if extHeader.Type() != fastjson.TypeTrue && extHeader.Type() != fastjson.TypeFalse {
		return false, errJsonTypeNotMatch
	}
	return extHeader.GetBool(), nil
}

func (jeo *jsonExtObjV2) GetBoolOrStringBool(key string) (bool, error) {
	if jeo.jsonExt == nil {
		return false, errJsonKeyNotFound
	}
	extHeader := jeo.jsonExt.Get(key)
	if extHeader == nil {
		return false, errJsonKeyNotFound
	}

	if extHeader.Type() == fastjson.TypeString {
		ts, _ := extHeader.StringBytes()
		tb := false
		if len(ts) > 0 {
			tb, _ = strconv.ParseBool(string(ts))
		}
		return tb, nil
	}

	return extHeader.GetBool(), nil
}
