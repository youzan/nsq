package nsqdserver

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/youzan/go-nsq"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/test"
	nsqdNs "github.com/youzan/nsq/nsqd"
)

func identify(t *testing.T, conn io.ReadWriter, extra map[string]interface{}, f int32) []byte {
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	ci["hostname"] = "test"
	ci["feature_negotiation"] = true
	if extra != nil {
		for k, v := range extra {
			ci[k] = v
		}
	}
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Equal(t, err, nil)
	if frameType != f {
		t.Errorf("identify err: %v", string(data))
	}
	test.Equal(t, f, frameType)
	return data
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func readFrameResponse(t *testing.T, conn io.ReadWriter) (int32, []byte, error) {
	for {
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			return 0, nil, err
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Equal(t, err, nil)
		if string(data) == string(heartbeatBytes) {
			cmd := nsq.Nop()
			cmd.WriteTo(conn)
			continue
		}
		return frameType, data, err
	}
}

func validatePubResponse(t *testing.T, conn io.ReadWriter) {
	frameType, data, err := readFrameResponse(t, conn)
	test.Equal(t, nil, err)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data) >= 2, true)
	test.Equal(t, data[:2], []byte("OK"))
}

func subWaitResp(t *testing.T, conn io.ReadWriter, topicName string, channelName string) ([]byte, error) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	_, data, err := readFrameResponse(t, conn)
	return data, err
}

func subOrdered(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.SubscribeOrdered(topicName, channelName, "0").WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func subTrace(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.SubscribeAndTrace(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func subOffset(t *testing.T, conn io.ReadWriter, topicName string, channelName string, queueOffset int64) {
	var startFrom nsq.ConsumeOffset
	if queueOffset == -1 {
		startFrom.SetToEnd()
	} else {
		startFrom.SetVirtualQueueOffset(queueOffset)
	}
	_, err := nsq.SubscribeAdvanced(topicName, channelName, "0", startFrom).WriteTo(conn)
	test.Equal(t, err, nil)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func authCmd(t *testing.T, conn io.ReadWriter, authSecret string, expectSuccess string) {
	auth, _ := nsq.Auth(authSecret)
	_, err := auth.WriteTo(conn)
	test.Equal(t, err, nil)
	if expectSuccess != "" {
		readValidate(t, conn, nsq.FrameTypeResponse, expectSuccess)
	}
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, _, _ := nsq.UnpackResponse(resp)
	test.Equal(t, frameType, frameTypeError)
}

func readValidate(t *testing.T, conn io.ReadWriter, f int32, d string) []byte {
	for {
		resp, err := nsq.ReadResponse(conn)
		test.Equal(t, err, nil)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Equal(t, err, nil)

		if d != string(heartbeatBytes) && string(data) == string(heartbeatBytes) {
			cmd := nsq.Nop()
			cmd.WriteTo(conn)
			continue
		}

		test.Equal(t, string(data), d)
		test.Equal(t, frameType, f)
		return data
	}
}

func closeConnAfterTimeout(conn net.Conn, to time.Duration, stop chan int) {
	go func() {
		select {
		case <-time.After(to):
			conn.Close()
		case <-stop:
			return
		}

	}()
}

func recvNextMsgAndCheckClientMsg(t *testing.T, conn io.ReadWriter, expLen int, expTraceID uint64, autoFin bool) *nsq.Message {
	for {
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			t.Logf("read response err: %v", err.Error())
		}

		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		if frameType == frameTypeError {
			t.Log(string(resp))
			continue
		}
		if frameType == frameTypeResponse {
			if string(data) == string(heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
			}
			continue
		}
		test.Equal(t, frameTypeMessage, frameType)
		msgOut, err := nsq.DecodeMessage(data)
		test.Nil(t, err)
		if expLen > 0 {
			test.Equal(t, expLen, len(msgOut.Body))
		}
		if expTraceID > 0 {
			traceID := binary.BigEndian.Uint64(msgOut.ID[8:])
			test.Equal(t, expTraceID, traceID)
		}
		if autoFin {
			_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
			test.Nil(t, err)
		}
		return msgOut
	}
}

func recvNextMsgAndCheckExcept4EOF(t *testing.T, conn io.ReadWriter, expLen int, expTraceID uint64, autoFin bool, ext bool) (*nsq.Message, bool) {
	for {
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			t.Logf("read response err: %v", err.Error())
		}
		if err != nil && err.Error() == "EOF" {
			t.Logf("read message end with EOF")
			return nil, true
		}
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		//fmt.Printf("%v %v\n", frameType, string(data))
		test.Nil(t, err)
		if frameType == frameTypeError {
			t.Log(string(resp))
			continue
		}
		if frameType == frameTypeResponse {
			if string(data) == string(heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
			}
			continue
		}
		test.Equal(t, frameTypeMessage, frameType)
		msgOut, err := nsq.DecodeMessageWithExt(data, ext)
		test.Nil(t, err)
		if expLen > 0 {
			test.Equal(t, expLen, len(msgOut.Body))
		}
		if expTraceID > 0 {
			traceID := binary.BigEndian.Uint64(msgOut.ID[8:])
			test.Equal(t, expTraceID, traceID)
		}
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
		if autoFin {
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
			test.Nil(t, err)
		}
		return msgOut, false
	}
}

func recvNextMsgAndCheck(t *testing.T, conn io.ReadWriter,
	expLen int, expTraceID uint64, autoFin bool) *nsq.Message {
	return recvNextMsgAndCheckExt(t, conn, expLen, expTraceID, autoFin, false)
}

func recvNextMsgAndCheckExt(t *testing.T, conn io.ReadWriter,
	expLen int, expTraceID uint64, autoFin bool, ext bool) *nsq.Message {
	return recvNextMsgAndCheckExtTimeout(t, conn, expLen, expTraceID, autoFin, ext, time.Second*30)
}

func recvNextMsgAndCheckExtTimeout(t *testing.T, conn io.ReadWriter,
	expLen int, expTraceID uint64, autoFin bool, ext bool, waitTimeout time.Duration) *nsq.Message {
	start := time.Now()
	for {
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			t.Logf("read response err: %v", err.Error())
			if strings.Contains(err.Error(), "closed") ||
				strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "reset by peer") {
				break
			}
		}
		if time.Since(start) > waitTimeout {
			t.Error("wait message too long")
			break
		}
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		if frameType == frameTypeError {
			t.Log(string(resp))
			continue
		}
		if frameType == frameTypeResponse {
			if string(data) == string(heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
			}
			continue
		}
		test.Equal(t, frameTypeMessage, frameType)
		msgOut, err := nsq.DecodeMessageWithExt(data, ext)
		t.Log(string(data))
		test.Nil(t, err)
		if expLen > 0 {
			test.Equal(t, expLen, len(msgOut.Body))
			t.Logf("msg body: %v", string(msgOut.Body))
		}
		if expTraceID > 0 {
			traceID := binary.BigEndian.Uint64(msgOut.ID[8:])
			test.Equal(t, expTraceID, traceID)
		}
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
		if autoFin {
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
			test.Nil(t, err)
		}
		return msgOut
	}
	return nil
}

func recvNextMsgAndCheckWithCloseChan(t *testing.T, conn io.ReadWriter,
	expLen int, expTraceID uint64, autoFin bool, ext bool, closeChan chan int) *nsq.Message {
	for {
		select {
		case <-closeChan:
			t.Logf("recvNextMsgAndCheckWithCloseChan exit")
			return nil
		default:
		}
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			t.Logf("read response err: %v", err.Error())
		}
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		if frameType == frameTypeError {
			t.Log(string(resp))
			continue
		}
		if frameType == frameTypeResponse {
			if string(data) == string(heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
			}
			continue
		}
		test.Equal(t, frameTypeMessage, frameType)
		msgOut, err := nsq.DecodeMessageWithExt(data, ext)
		test.Nil(t, err)
		if expLen > 0 {
			test.Equal(t, expLen, len(msgOut.Body))
		}
		if expTraceID > 0 {
			traceID := binary.BigEndian.Uint64(msgOut.ID[8:])
			test.Equal(t, expTraceID, traceID)
		}
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
		if autoFin {
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
			test.Nil(t, err)
		}
		return msgOut
	}
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	test.Equal(t, protocol.IsValidChannelName("test"), true)
	test.Equal(t, protocol.IsValidChannelName("test-with_period."), true)
	test.Equal(t, protocol.IsValidChannelName("test#ephemeral"), true)
	test.Equal(t, protocol.IsValidTopicName("test"), true)
	test.Equal(t, protocol.IsValidTopicName("test-with_period."), true)
	test.Equal(t, protocol.IsValidTopicName("test#ephemeral"), true)
	test.Equal(t, protocol.IsValidTopicName("test:ephemeral"), false)
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, true)
	test.NotNil(t, msgOut)
}

func TestMultipleConsumerV2(t *testing.T) {
	msgChan := make(chan *nsq.Message)

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		defer conn.Close()

		identify(t, conn, nil, frameTypeResponse)
		sub(t, conn, topicName, "ch"+i)

		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		go func(c net.Conn) {
			resp, _ := nsq.ReadResponse(c)
			_, data, _ := nsq.UnpackResponse(resp)
			recvdMsg, _ := nsq.DecodeMessageWithExt(data, false)
			msgChan <- recvdMsg
		}(conn)
	}

	msgOut := <-msgChan
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)
	msgOut = <-msgChan
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)
}

func TestClientTimeout(t *testing.T) {
	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = 150 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")
	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	clients := ch.GetClients()
	test.Equal(t, 1, len(clients))
	for _, c := range clients {
		cs := c.Stats()
		test.Equal(t, int64(opts.MsgTimeout), cs.MsgTimeout)
	}
	time.Sleep(opts.ClientTimeout)

	// depending on timing there may be 1 or 2 hearbeats sent
	// just read until we get an error
	timer := time.After(100 * time.Millisecond)
	for {
		select {
		case <-timer:
			t.Fatalf("test timed out")
		default:
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				goto done
			}
		}
	}
done:
}

func TestClientHeartbeat(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, data, []byte("_heartbeat_"))

	time.Sleep(20 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(100 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = 100 * time.Millisecond
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)

	time.Sleep(150 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Equal(t, err, nil)
}

func TestMaxHeartbeatIntervalValid(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval / time.Millisecond)
	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeResponse)
}

func TestMaxHeartbeatIntervalInvalid(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval/time.Millisecond + 1)
	data := identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeError)
	test.Equal(t, string(data), "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid")
}

func TestClientIdentifyMsgTimeout(t *testing.T) {
	topicName := "test_client_msgtimeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = 150 * time.Millisecond
	opts.MsgTimeout = time.Second * 2
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")
	extra := make(map[string]interface{})
	extra["msg_timeout"] = opts.MsgTimeout / 2 / time.Millisecond
	identify(t, conn, extra, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	clients := ch.GetClients()
	test.Equal(t, 1, len(clients))
	for _, c := range clients {
		cs := c.Stats()
		test.Equal(t, int64(opts.MsgTimeout/2), cs.MsgTimeout)
	}
}

func TestClientOutputBufferTimeout(t *testing.T) {
	topicName := "test_outputbuffer_timeout" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	outputBufferTimeout := 100
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	extra := make(map[string]interface{})
	extra["output_buffer_timeout"] = outputBufferTimeout
	identify(t, conn, extra, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(20 * time.Millisecond)
	clients := ch.GetClients()
	test.Equal(t, 1, len(clients))
	for _, c := range clients {
		cs := c.Stats()
		test.Equal(t, int64(outputBufferTimeout)*int64(time.Millisecond), cs.OutputBufferTimeout)
	}
}

func TestSkipping(t *testing.T) {
	topicName := "test_skip_v2" + strconv.Itoa(int(time.Now().Unix()))
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, true)
	test.Equal(t, msgOut.Body, []byte("test body"))

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// skip the channel... the client shouldn't receive any more messages
	channel.Skip()

	// sleep to allow the skipped state to take effect
	time.Sleep(50 * time.Millisecond)

	offset1 := channel.GetConfirmed().Offset()

	msg = nsqdNs.NewMessage(0, []byte("test body2"))
	topic.PutMessage(msg)
	topic.ForceFlush()

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working on the internal clientMsgChan read
	time.Sleep(50 * time.Millisecond)
	offset2 := channel.GetConfirmed().Offset()

	test.NotEqual(t, offset1, offset2)
	select {
	case msg = <-channel.GetClientMsgChan():
		t.Logf("message should not be received.")
		t.Fail()
	case <-time.Tick(500 * time.Millisecond):
	}
	test.Equal(t, int64(0), channel.Depth())

	// unskip the channel... the client should now be pushed a message
	channel.UnSkip()

	msg = nsqdNs.NewMessage(0, []byte("test body3"))
	topic.PutMessage(msg)
	topic.ForceFlush()
	channel.TryWakeupRead()

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, true)
	test.Equal(t, msgOut.Body, []byte("test body3"))
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

func createJsonHeaderExtWithTagBenchmark(b *testing.B, tag string) *ext.JsonHeaderExt {
	jsonHeader := make(map[string]interface{})
	jsonHeader[ext.CLIENT_DISPATCH_TAG_KEY] = tag
	jsonHeaderBytes, err := json.Marshal(&jsonHeader)
	if err != nil {
		b.FailNow()
	}
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes(jsonHeaderBytes)
	return jhe
}

func createJsonHeaderExtBenchmark(b *testing.B) *ext.JsonHeaderExt {
	jsonHeader := make(map[string]interface{})
	jsonHeader["X-Nsqext-key1"] = "val1"
	jsonHeader["X-Nsqext-key2"] = "val2"
	jsonHeader["X-Nsqext-key3"] = "val3"
	jsonHeader["X-Nsqext-key4"] = "val4"
	jsonHeader["X-Nsqext-key5"] = "val5"
	jsonHeaderBytes, err := json.Marshal(&jsonHeader)
	if err != nil {
		b.FailNow()
	}
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes(jsonHeaderBytes)
	return jhe
}

func TestConsumeTagMessageNormal(t *testing.T) {
	topicName := "test_tag_normal" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")
	tagName := "TAG"

	//subscribe tag client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["desired_tag"] = tagName
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	//subscribe normal client
	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_wo_tag"
	client2Params["hostname"] = "client_wo_tag"
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)

	tag := createJsonHeaderExtWithTag(t, tagName)

	//case 1: tagged message goes to client with tag
	msg := nsqdNs.NewMessageWithExt(0, []byte("test body"), tag.ExtVersion(), tag.GetBytes())
	topic.GetChannel("ch")
	_, _, _, _, putErr := topic.PutMessage(msg)
	test.Nil(t, putErr)

	// receive the first message via the client, finish it, and send new RDY
	closeChan := make(chan int, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		msgOut := recvNextMsgAndCheckWithCloseChan(t, conn2, len(msg.Body), msg.TraceID, true, true, closeChan)
		test.Nil(t, msgOut)
		t.Logf("subscribe without tag stops.")
		wg.Done()
	}()

	msgOut := recvNextMsgAndCheckExt(t, conn1, len(msg.Body), msg.TraceID, true, true)
	test.NotNil(t, msgOut)
	closeChan <- 1
	wg.Wait()

	//case 2: tagged messages goes to client with tag
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessageWithExt(0, []byte("test body"), tag.ExtVersion(), tag.GetBytes())
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	wg.Add(1)
	go func() {
		msgOut := recvNextMsgAndCheckWithCloseChan(t, conn2, len(msg.Body), msg.TraceID, true, true, closeChan)
		test.Nil(t, msgOut)
		t.Logf("subscrieb without tag stops.")
		wg.Done()
	}()
	closeChan <- 1
	wg.Wait()

	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut)
	}

	conn1.Close()
	conn2.Close()

	time.Sleep(1 * time.Second)
	_, exist := topic.GetChannel("ch").GetClientTagMsgChan(tagName)
	//assert chan cnt
	test.Equal(t, false, exist)
}

func TestConsumeIllegalZanTestWithCompatibility(t *testing.T) {
	topicName := "test_zan_test_illegal" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	//subscribe normal client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client"
	client1Params["hostname"] = "client"
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	jsonHeaderStr := "{\"##client_dispatch_tag\":\"test_tag\",\"##channel_filter_tag\":\"test\",\"zan_test\":\"false\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))

	jsonHeaderZanTestStr := "{\"##client_dispatch_tag\":\"test_tag\",\"##channel_filter_tag\":\"test\",\"zan_test\":true}"
	jheZantest := ext.NewJsonHeaderExt()
	jheZantest.SetJsonHeaderBytes([]byte(jsonHeaderZanTestStr))

	conn, err := mustConnectNSQD(tcpAddr)
	msgBodyZanTest := []byte("test body zan test")
	msgBody := []byte("test body")

	//case 1: tagged message goes to client with tag
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
			topic.GetChannel("ch")
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
		} else {
			cmd, err := nsq.PublishWithJsonExt(topicName, "0", msgBodyZanTest, jheZantest.GetBytes())
			test.Nil(t, err)
			cmd.WriteTo(conn)
			frameType, data, _ := readFrameResponse(t, conn)
			t.Logf("frameType: %d, data: %s", frameType, data)
			test.Equal(t, int32(1), frameType)
		}
	}

	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msgBody), 0, true, false)
		test.NotNil(t, msgOut)
		test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
	}

	conn1.Close()
	conn.Close()
	time.Sleep(1 * time.Second)
}

func TestConsumeJsonHeaderMessageNormal(t *testing.T) {
	topicName := "test_json_header_normal" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	//subscribe normal client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	jsonHeaderStr := "{\"##client_dispatch_tag\":\"test_tag\",\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))
	msgBody := []byte("test body")
	//case 1: tagged message goes to client with tag
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
		topic.GetChannel("ch")
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
		test.Assert(t, bytes.Equal(msgOut.ExtBytes, []byte(jsonHeaderStr)), "json header from message not equals, expected: %v, got: %v", jsonHeaderStr, string(msgOut.ExtBytes))
	}

	conn1.Close()
	time.Sleep(1 * time.Second)
}

func TestConsumeJsonHeaderMessageTag(t *testing.T) {
	topicName := "test_json_header_tag" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	//subscribe tag client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["desired_tag"] = "test_tag"
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	//subscribe normal client
	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_wo_tag"
	client2Params["hostname"] = "client_wo_tag"
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)

	jsonHeaderTagStr := "{\"##client_dispatch_tag\":\"test_tag\",\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderTagStr))
	msgBody := []byte("test body")
	for i := 0; i < 5; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
		topic.GetChannel("ch")
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	jsonHeaderStr := "{\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	jhe = ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
		topic.GetChannel("ch")
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	//consume tag message
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
		test.Assert(t, bytes.Equal(msgOut.ExtBytes, []byte(jsonHeaderTagStr)), "json header from message not equals, expected: %v, got: %v", jsonHeaderTagStr, string(msgOut.ExtBytes))
	}

	//consume normal message
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
		test.Assert(t, bytes.Equal(msgOut.ExtBytes, []byte(jsonHeaderStr)), "json header from message not equals, expected: %v, got: %v", jsonHeaderStr, string(msgOut.ExtBytes))
	}

	conn1.Close()
	conn2.Close()
	time.Sleep(1 * time.Second)
}

func TestPubJsonHeaderIgnored(t *testing.T) {
	topicName := "test_json_header_ignore" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.AllowExtCompatible = true
	opts.AllowSubExtCompatible = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	var jext nsq.MsgExt
	jext.TraceID = 1
	cmd, _ := nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	jext.DispatchTag = "tag"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	jext.TraceID = 0
	jext.DispatchTag = "tag"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), []byte("{}"))
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	jext.Custom[ext.ZAN_TEST_KEY] = "true"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	jext.Custom["k1"] = "v1"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))
}

func TestPubJsonHeaderNotCompatible(t *testing.T) {
	topicName := "test_json_header_nocompatible" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.AllowExtCompatible = false
	opts.AllowSubExtCompatible = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	var jext nsq.MsgExt
	jext.TraceID = 1
	cmd, _ := nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))

	jext.DispatchTag = "tag"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))

	jext.TraceID = 0
	jext.DispatchTag = "tag"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))

	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), []byte("{}"))
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))

	jext.Custom[ext.ZAN_TEST_KEY] = "true"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))

	jext.Custom["k1"] = "v1"
	cmd, _ = nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jext.ToJson())
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ext.E_EXT_NOT_SUPPORT))
}

func TestConsumeRateLimit(t *testing.T) {
	topicName := "test_consume_normal" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	ch := topic.GetChannel("ch")
	ch.ChangeLimiterBytes(1)
	// limit the message above 1KB
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessage(0, make([]byte, 1025))
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	//subscribe client
	conn1, err := mustConnectNSQD(tcpAddr)
	defer conn1.Close()
	test.Equal(t, err, nil)
	params := make(map[string]interface{})
	params["client_id"] = "client"
	params["hostname"] = "client"
	params["extend_support"] = true
	identify(t, conn1, params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(10).WriteTo(conn1)
	test.Equal(t, err, nil)

	last := time.Now()
	start := last
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, 0, 0, true, true)
		test.NotNil(t, msgOut)
		current := time.Now()
		t.Logf("subscribe got messages at %s", current)
		// allow the burst
		if i > 4 {
			test.Equal(t, true, current.Sub(last) > time.Second/4)
		}
		last = current
	}
	test.Equal(t, true, time.Since(start) > time.Second*5)
	test.Equal(t, true, time.Since(start) < time.Second*9)
}

func TestConsumeMessageWhileUpgrade(t *testing.T) {
	topicName := "test_ext_topic_upgrade" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["extend_support"] = false
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_wo_tag"
	client2Params["hostname"] = "client_wo_tag"
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	subFail(t, conn2, topicName, "ch")

	msgBody := []byte("test body")
	traceID := uint64((0))
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessage(0, msgBody)
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	// use old conn consume old message
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msgBody), 0, true, false)
		test.NotNil(t, msgOut)
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}
	// conn1 last message may be requeued
	t.Logf("begin upgrade topic")

	topicDynConf.Ext = true
	topic.SetDynamicInfo(topicDynConf, nil)
	jsonHeaderStr := "{\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))
	extTraceID := traceID
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	conn1.Close()
	conn2.Close()
	t.Logf("end write upgrade topic")
	// all connection should be closed after upgrade, we need reconnect
	conn1, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn1, client1Params, frameTypeResponse)
	subFail(t, conn1, topicName, "ch")

	t.Logf("old conn should fail")
	conn2, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)
	// use new conn consume old message
	t.Logf("receiving old messages using new conn")
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		if msgOut.GetTraceID() >= extTraceID {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			test.Equal(t, []byte(jsonHeaderStr), msgOut.ExtBytes)
		} else {
			test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
		}
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}

	t.Logf("receiving extend messages using new conn")
	//  use new conn consume new message
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		if msgOut.GetTraceID() >= extTraceID {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			test.Equal(t, []byte(jsonHeaderStr), msgOut.ExtBytes)
		} else {
			test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
		}
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}
	conn2.Close()
	time.Sleep(1 * time.Second)
}

func TestConsumeDelayedMessageWhileUpgrade(t *testing.T) {
	topicName := "test_ext_topic_upgrade" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ReqToEndThreshold = time.Second
	opts.MsgTimeout = time.Second * 5
	opts.MaxConfirmWin = 50
	opts.MaxReqTimeout = opts.ReqToEndThreshold * 10
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["extend_support"] = false
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	msgBody := []byte("test body")
	traceID := uint64((0))
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessage(0, msgBody)
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	// make sure delayed queue is enabled  before upgrade
	_, err = topic.GetOrCreateDelayedQueueNoLock(nil)
	test.Nil(t, err)
	delayedNonExtMsgID := uint64(0)
	// use old conn consume old message
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn1, len(msgBody), 0, false, false)
		test.NotNil(t, msgOut)
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
		if delayedNonExtMsgID == 0 && msgOut.GetTraceID() >= 1 {
			_, err := nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold*2).WriteTo(conn1)
			test.Nil(t, err)
			delayedNonExtMsgID = msgOut.GetTraceID()
		} else {
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn1)
			test.Nil(t, err)
		}
	}
	// conn1 last message may be requeued
	t.Logf("begin upgrade topic")

	topicDynConf.Ext = true
	topic.SetDynamicInfo(topicDynConf, nil)
	jsonHeaderStr := "{\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))
	extTraceID := traceID
	for i := 0; i < 10; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msgBody, jhe.ExtVersion(), jhe.GetBytes())
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	conn1.Close()
	t.Logf("end write upgrade topic")
	// all connection should be closed after upgrade, we need reconnect
	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_wo_tag"
	client2Params["hostname"] = "client_wo_tag"
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)
	delayedExtMsgID := uint64(0)
	delayedRecved := 0
	// use new conn consume old message
	t.Logf("receiving old messages using new conn")
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, false, true)
		test.NotNil(t, msgOut)
		if msgOut.GetTraceID() >= extTraceID {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			test.Equal(t, []byte(jsonHeaderStr), msgOut.ExtBytes)
			if msgOut.GetTraceID() > 1 && delayedExtMsgID == 0 {
				_, err := nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold*2).WriteTo(conn2)
				test.Nil(t, err)
				delayedExtMsgID = msgOut.GetTraceID()
			} else {
				_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn2)
				test.Nil(t, err)
			}
		} else {
			test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
			if msgOut.GetTraceID() == delayedNonExtMsgID {
				ts := time.Now().UnixNano() - msgOut.Timestamp
				if ts < opts.ReqToEndThreshold.Nanoseconds()*2 {
					t.Logf("delay msg %v  , now: %v", msgOut, time.Now().UnixNano())
					test.Assert(t, false, "delay return early")
				}
				delayedRecved++
			}
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn2)
			test.Nil(t, err)
		}
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}

	t.Logf("receiving extend messages using new conn")
	//  use new conn consume new message
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, false, true)
		test.NotNil(t, msgOut)
		if msgOut.GetTraceID() >= extTraceID {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			test.Equal(t, []byte(jsonHeaderStr), msgOut.ExtBytes)
			if msgOut.GetTraceID() > 1 && delayedExtMsgID == 0 {
				_, err := nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold*2).WriteTo(conn2)
				test.Nil(t, err)
				delayedExtMsgID = msgOut.GetTraceID()
			} else {
				_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn2)
				test.Nil(t, err)
				if msgOut.GetTraceID() == delayedExtMsgID {
					ts := time.Now().UnixNano() - msgOut.Timestamp
					if ts < opts.ReqToEndThreshold.Nanoseconds()*2 {
						t.Logf("delay msg %v  , now: %v", msgOut, time.Now().UnixNano())
						test.Assert(t, false, "delay return early")
					}
					delayedRecved++
				}
			}

		} else {
			_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn2)
			test.Nil(t, err)
			test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
			if msgOut.GetTraceID() == delayedNonExtMsgID {
				ts := time.Now().UnixNano() - msgOut.Timestamp
				if ts < opts.ReqToEndThreshold.Nanoseconds()*2 {
					t.Logf("delay msg %v  , now: %v", msgOut, time.Now().UnixNano())
					test.Assert(t, false, "delay return early")
				}
				delayedRecved++
			}
		}
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}
	time.Sleep(opts.ReqToEndThreshold*3 + 2*opts.QueueScanInterval)
	for delayedRecved < 2 {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msgBody), 0, true, true)
		test.NotNil(t, msgOut)
		if msgOut.GetTraceID() >= extTraceID {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			test.Equal(t, []byte(jsonHeaderStr), msgOut.ExtBytes)
			if msgOut.GetTraceID() == delayedExtMsgID {
				ts := time.Now().UnixNano() - msgOut.Timestamp
				if ts < opts.ReqToEndThreshold.Nanoseconds()*2 {
					t.Logf("delay msg %v  , now: %v", msgOut, time.Now().UnixNano())
					test.Assert(t, false, "delay return early")
				}
				delayedRecved++
			}
		} else {
			test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
			if msgOut.GetTraceID() == delayedNonExtMsgID {
				ts := time.Now().UnixNano() - msgOut.Timestamp
				if ts < opts.ReqToEndThreshold.Nanoseconds()*2 {
					t.Logf("delay msg %v  , now: %v", msgOut, time.Now().UnixNano())
					test.Assert(t, false, "delay return early")
				}
				delayedRecved++
			}
		}
		test.Equal(t, msgBody, msgOut.Body)
		test.Assert(t, msgOut.Attempts <= 4000, "attempts should less than 4000")
	}
	conn2.Close()
	time.Sleep(1 * time.Second)
	test.Equal(t, 2, delayedRecved)
}

func prepareTestOptsForDelayedMsg(t *testing.T, reqToEnd time.Duration) *nsqdNs.Options {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	if testing.Verbose() {
		opts.LogLevel = 4
	}
	nsqdNs.SetLogger(opts.Logger)
	opts.QueueScanInterval = time.Millisecond * 200
	opts.ReqToEndThreshold = reqToEnd
	opts.MsgTimeout = time.Second * 10
	opts.MaxConfirmWin = 50
	opts.MaxReqTimeout = reqToEnd * 3
	return opts
}

func TestConsumeDelayedMessageWhileSample(t *testing.T) {
	topicName := "test_consume_delayed_sample" + strconv.Itoa(int(time.Now().Unix()))

	opts := prepareTestOptsForDelayedMsg(t, time.Millisecond*200)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn1, map[string]interface{}{
		"sample_rate": int32(50)}, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	msgBody := []byte("test body")
	traceID := uint64((0))
	for i := 0; i < 100; i++ {
		msg := nsqdNs.NewMessage(0, msgBody)
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	_, err = topic.GetOrCreateDelayedQueueNoLock(nil)
	test.Nil(t, err)
	for i := 0; i < 100; i++ {
		msgOut := recvNextMsgAndCheck(t, conn1, len(msgBody), 0, false)
		test.NotNil(t, msgOut)
		// req to end
		_, err := nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold*2).WriteTo(conn1)
		test.Nil(t, err)
		chStats := nsqdNs.NewChannelStats(ch, nil, 0)
		if chStats.DelayedQueueCount > 10 && chStats.DeferredFromDelayCount > 0 {
			break
		}
	}
	time.Sleep(opts.ReqToEndThreshold * 2)
	chStats := nsqdNs.NewChannelStats(ch, nil, 0)
	t.Log(chStats)
	test.Assert(t, chStats.DelayedQueueCount > 0, "should have disk delayed msgs")
	test.Assert(t, chStats.DeferredFromDelayCount > 0, "should have defer message in mem")
	conn1.Close()

	time.Sleep(time.Second * 2)
	// make sure we can consume delayed message even sampled
	cnt := 0
	conn1, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn1, nil, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)
	conn1.SetDeadline(time.Now().Add(time.Second * 30))
	conn1.SetReadDeadline(time.Now().Add(time.Second * 30))
	conn1.SetWriteDeadline(time.Now().Add(time.Second * 30))
	for {
		msgOut := recvNextMsgAndCheck(t, conn1, len(msgBody), 0, false)
		test.NotNil(t, msgOut)
		if msgOut == nil {
			break
		}
		nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn1)
		cnt++
		time.Sleep(time.Millisecond)
		chStats := nsqdNs.NewChannelStats(ch, nil, 0)
		test.Assert(t, chStats.DeferredFromDelayCount >= 0, "deferred from delay counter should >=0")
		if chStats.DelayedQueueCount == 0 && chStats.Depth == 0 && chStats.DeferredCount == 0 &&
			chStats.InFlightCount == 0 {
			break
		}
	}
	conn1.Close()
	t.Logf("fin %v messages", cnt)
	test.Assert(t, cnt >= 40, "should consume enough messages")
	chStats = nsqdNs.NewChannelStats(ch, nil, 0)
	t.Log(chStats)
	test.Assert(t, chStats.DelayedQueueCount == 0, "should consume all delayed messages")
	test.Assert(t, chStats.DeferredFromDelayCount == 0, "deferred from delay counter should be 0")
}

func TestConsumeDelayedMessageWhileDisableEnableSwitch(t *testing.T) {
	topicName := "test_consume_delayed_switch_master" + strconv.Itoa(int(time.Now().Unix()))

	opts := prepareTestOptsForDelayedMsg(t, time.Millisecond*100)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	msgBody := []byte("test body")
	traceID := uint64((0))
	for i := 0; i < 100; i++ {
		msg := nsqdNs.NewMessage(0, msgBody)
		msg.TraceID = traceID
		traceID++
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}
	conn1, err := mustConnectAndSub(t, tcpAddr, topicName, "ch")
	defer conn1.Close()
	test.Equal(t, err, nil)
	_, err = topic.GetOrCreateDelayedQueueNoLock(nil)
	test.Nil(t, err)
	for i := 0; i < 100; i++ {
		msgOut := recvNextMsgAndCheck(t, conn1, len(msgBody), 0, false)
		test.NotNil(t, msgOut)
		// req to end
		_, err := nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold*2).WriteTo(conn1)
		test.Nil(t, err)
		chStats := nsqdNs.NewChannelStats(ch, nil, 0)
		if chStats.DelayedQueueCount > 50 && chStats.DeferredFromDelayCount > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// wait delayed message peek from db
	start := time.Now()
	for {
		time.Sleep(opts.ReqToEndThreshold)
		chStats := nsqdNs.NewChannelStats(ch, nil, 0)
		t.Log(chStats)
		test.Assert(t, chStats.DelayedQueueCount > 0, "should have disk delayed msgs")
		if chStats.ClientNum == 0 {
			// the client connection is closed by accident, we reconnect
			conn1, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
			test.Equal(t, err, nil)
		}
		if chStats.DeferredFromDelayCount > 0 {
			break
		}
		if time.Since(start) > time.Minute {
			break
		}
	}

	chStats := nsqdNs.NewChannelStats(ch, nil, 0)
	test.Assert(t, chStats.DeferredFromDelayCount > 0, "should have defer message in mem")
	ch.DisableConsume(true)

	time.Sleep(time.Second * 2)
	// maybe some inflight message while disable?
	chStats = nsqdNs.NewChannelStats(ch, nil, 0)
	t.Log(chStats)
	test.Assert(t, chStats.DelayedQueueCount > 0, "should have disk delayed msgs")
	test.Assert(t, chStats.DeferredCount == 0, "should have no defer message in mem while disable")
	// push inflight may fail due to consume disabled, so the requeued messages should be removed from
	// waiting list
	test.Assert(t, chStats.DeferredFromDelayCount == 0, "should have no delayed queue message while disable")

	ch.DisableConsume(false)
	// make sure we can consume all after enable consume
	cnt := 0
	conn1, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)
	conn1.SetDeadline(time.Now().Add(time.Second * 10))
	conn1.SetReadDeadline(time.Now().Add(time.Second * 10))
	conn1.SetWriteDeadline(time.Now().Add(time.Second * 10))
	for {
		msgOut := recvNextMsgAndCheck(t, conn1, len(msgBody), 0, false)
		test.NotNil(t, msgOut)
		if msgOut == nil {
			break
		}
		nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn1)
		cnt++
		time.Sleep(time.Millisecond)
		chStats := nsqdNs.NewChannelStats(ch, nil, 0)
		test.Assert(t, chStats.DeferredFromDelayCount >= 0, "deferred from delay counter should >=0")
		if chStats.DelayedQueueCount == 0 && chStats.Depth == 0 && chStats.DeferredCount == 0 &&
			chStats.InFlightCount == 0 {
			break
		}
	}
	t.Logf("fin %v messages", cnt)
	test.Assert(t, cnt >= 100, "should consume enough messages")
	chStats = nsqdNs.NewChannelStats(ch, nil, 0)
	t.Log(chStats)
	test.Assert(t, chStats.DelayedQueueCount == 0, "should consume all delayed messages")
	test.Assert(t, chStats.DeferredFromDelayCount == 0, "deferred from delay counter should be 0")
}

func TestConsumeMultiTagMessages(t *testing.T) {
	topicName := "test_tag_multiTag" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = time.Second * 5
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	//subscribe tag client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag1"
	client1Params["hostname"] = "client_w_tag1"

	tagName1 := "TAG1"
	client1Params["desired_tag"] = tagName1
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	//subscribe normal client
	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_w_tag2"
	client2Params["hostname"] = "client_w_tag2"

	tagName2 := "TAG2"
	client2Params["desired_tag"] = tagName2
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)

	//subscribe normal client
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_wo_tag"
	clientParams["hostname"] = "client_wo_tag"
	clientParams["extend_support"] = true
	identify(t, conn, clientParams, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	tag1 := createJsonHeaderExtWithTag(t, tagName1)
	//case 1: tagged message goes to client with tag
	msg := nsqdNs.NewMessageWithExt(0, []byte("test body tag1"), tag1.ExtVersion(), tag1.GetBytes())
	topic.GetChannel("ch")
	_, _, _, _, putErr := topic.PutMessage(msg)
	test.Nil(t, putErr)

	tag2 := createJsonHeaderExtWithTag(t, tagName2)
	//case 1: tagged message goes to client with tag
	msg = nsqdNs.NewMessageWithExt(0, []byte("test body tag2"), tag2.ExtVersion(), tag2.GetBytes())
	topic.GetChannel("ch")
	_, _, _, _, putErr = topic.PutMessage(msg)
	test.Nil(t, putErr)

	// receive the first message via the client, finish it, and send new RDY
	closeChan := make(chan int, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgOut := recvNextMsgAndCheckWithCloseChan(t, conn, len(msg.Body), msg.TraceID, true, true, closeChan)
		test.Nil(t, msgOut)
		t.Logf("subscrieb without tag stops.")
	}()

	var wgTag sync.WaitGroup
	wgTag.Add(1)
	go func() {
		defer wgTag.Done()
		msgOut1 := recvNextMsgAndCheckExt(t, conn1, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut1)
	}()
	wgTag.Add(1)
	go func() {
		defer wgTag.Done()
		msgOut2 := recvNextMsgAndCheckExt(t, conn2, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut2)
	}()

	wgTag.Wait()
	closeChan <- 1
	wg.Wait()

	conn.Close()
	conn1.Close()
	conn2.Close()

	time.Sleep(1 * time.Second)
	//assert chan cnt
	_, exist := topic.GetChannel("ch").GetClientTagMsgChan(tagName1)
	test.Equal(t, false, exist)
	_, exist = topic.GetChannel("ch").GetClientTagMsgChan(tagName2)
	test.Equal(t, false, exist)
}

func TestRemoveTagClientWhileConsuming(t *testing.T) {
	topicName := "test_tag_remove" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second * 5

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")
	tagName := "TAG"

	//subscribe tag client
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["desired_tag"] = tagName
	client1Params["extend_support"] = true
	identify(t, conn, client1Params, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	var msg *nsqdNs.Message
	tag := createJsonHeaderExtWithTag(t, tagName)
	for i := 0; i < 20; i++ {
		msg = nsqdNs.NewMessageWithExt(0, []byte("test body tag"), tag.ExtVersion(), tag.GetBytes())
		topic.GetChannel("ch")
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	//case1 disconnect and connect
	conn.Close()

	time.Sleep(1 * time.Second)
	ch := topic.GetChannel("ch")
	_, exist := ch.GetClientTagMsgChan(tagName)
	test.Equal(t, false, exist)

	//subscribe back
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, client1Params, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	//consume 10 of message in stock
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut)
	}

	//close conn, again
	conn.Close()

	//subscribe back
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, client1Params, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	//consume 10 of message in stock
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut)
	}

	//create another consumer with TAG and consume all
	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_w_tag2"
	client2Params["hostname"] = "client_w_tag2"
	client2Params["desired_tag"] = tagName
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)

	//consume 10 of message in stock
	for i := 0; i < 5; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn2, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut)
	}

	conn.Close()
	time.Sleep(1 * time.Second)
	ch = topic.GetChannel("ch")
	_, exist = ch.GetClientTagMsgChan(tagName)
	test.Equal(t, true, exist)

	conn2.Close()
	time.Sleep(1 * time.Second)
	_, exist = ch.GetClientTagMsgChan(tagName)
	test.Equal(t, false, exist)
}

//subscribe to old topic, with desired tag, which is a valid hehavior
func TestSubWTagToOldTopic(t *testing.T) {
	topicName := "test_tag_unset" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	//pub without ext content
	body := fmt.Sprintf("msg for old topic")
	msg := nsqdNs.NewMessageWithExt(0, []byte(body), ext.NO_EXT_VER, nil)
	topic.GetChannel("ch")
	_, _, _, _, putErr := topic.PutMessage(msg)
	test.Nil(t, putErr)

	conn, err := mustConnectNSQD(tcpAddr)
	defer conn.Close()
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["desired_tag"] = "valid_tag_123"
	identify(t, conn, client1Params, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	//consume 10 of message in stock
	msgOut := recvNextMsgAndCheckExt(t, conn, len(msg.Body), msg.TraceID, true, false)
	test.NotNil(t, msgOut)
	test.Equal(t, "msg for old topic", string(msgOut.Body))
}

func TestSubToChannelNotRegistered(t *testing.T) {
	topicName := "test_ch_notregistered" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit:               1,
		SyncEvery:                1,
		Ext:                      false,
		DisableChannelAutoCreate: true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	//pub without ext content
	body := fmt.Sprintf("msg for old topic")
	msg := nsqdNs.NewMessageWithExt(0, []byte(body), ext.NO_EXT_VER, nil)
	_, _, _, _, putErr := topic.PutMessage(msg)
	test.Nil(t, putErr)

	conn, err := mustConnectNSQD(tcpAddr)
	defer conn.Close()
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client"
	client1Params["hostname"] = "client"
	identify(t, conn, client1Params, frameTypeResponse)

	resp, err := subWaitResp(t, conn, topicName, "ch")
	test.Equal(t, nil, err)
	test.Assert(t, strings.HasPrefix(string(resp), "E_SUB_CHANNEL_NOT_REGISTERED"), "sub to registered channel shoudl fail")
}

func TestInvalidTagSub(t *testing.T) {
	topicName := "test_tag_invalid" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	//pub without ext content
	//tag := ext.NewTag(string("tag1"))
	body := fmt.Sprintf("msg tag 1")
	msg := nsqdNs.NewMessageWithExt(0, []byte(body), ext.NO_EXT_VER, nil)
	topic.GetChannel("ch")
	_, _, _, _, putErr := topic.PutMessage(msg)
	test.Nil(t, putErr)

	conn, err := mustConnectNSQD(tcpAddr)
	defer conn.Close()
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag1"
	client1Params["hostname"] = "client_w_tag1"
	client1Params["desired_tag"] = "valid_tag_123"
	client1Params["extend_support"] = true
	identify(t, conn, client1Params, frameTypeResponse)

	client1Params["desired_tag"] = "this should be invalid looooooooooooooooooooooooooooo0ooooooooooooooooooooooooooooo0000000oooooong tag"
	client1Params["extend_support"] = true
	identify(t, conn, client1Params, frameTypeError)
}

func TestConsumeTagConcurrent(t *testing.T) {
	ticker := time.NewTicker(30 * time.Second)
	consumeTagConcurrent(t, false, ticker)
}

func TestConsumeTagConcurrentProduceFirst(t *testing.T) {
	ticker := time.NewTicker(30 * time.Second)
	consumeTagConcurrent(t, true, ticker)
}

func consumeTagConcurrent(t *testing.T, producerFirst bool, ticker *time.Ticker) {
	topicName := "test_tag_concurrent" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = time.Second * 2
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	tags := make([]*ext.JsonHeaderExt, 3)
	tagName1 := "tag1"
	tagName2 := "tag2"
	tagName3 := "tag3"

	tags[0] = createJsonHeaderExtWithTag(t, tagName1)
	tags[1] = createJsonHeaderExtWithTag(t, tagName2)
	tags[2] = createJsonHeaderExtWithTag(t, tagName3)

	clsP1 := make(chan int, 1)
	cntP1 := make(chan int64, 1)
	clsP2 := make(chan int, 1)
	cntP2 := make(chan int64, 1)
	clsP3 := make(chan int, 1)
	cntP3 := make(chan int64, 1)

	clsP := make([]chan int, 3)
	clsP[0] = clsP1
	clsP[1] = clsP2
	clsP[2] = clsP3

	cntP := make([]chan int64, 3)
	cntP[0] = cntP1
	cntP[1] = cntP2
	cntP[2] = cntP3

	startP := make([]chan int, 3)
	startP[0] = make(chan int)
	startP[1] = make(chan int)
	startP[2] = make(chan int)

	go func(start chan int, closeChan chan int, cntChan chan int64) {
		if !producerFirst {
			<-start
		}
		cnt := int64(0)
		t.Logf("producer1 starts")
	loop:
		for {
			tag := tags[0]
			body := fmt.Sprintf("%v tag 1", 100+cnt)
			msg := nsqdNs.NewMessageWithExt(0, []byte(body), tag.ExtVersion(), tag.GetBytes())
			topic.GetChannel("ch")
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
			cnt++
			timeBase := rand.Intn(3)
			select {
			case <-closeChan:
				t.Logf("producer1 exit, total msg published %v", cnt)
				break loop
			case <-time.After(time.Duration(timeBase) * time.Second):
			}
		}
		cntChan <- cnt
	}(startP[0], clsP1, cntP1)

	go func(start chan int, closeChan chan int, cntChan chan int64) {
		if !producerFirst {
			<-start
		}
		cnt := int64(0)
		t.Logf("producer2 starts")
	loop:
		for {
			tag := tags[1]
			body := fmt.Sprintf("%v tag 2", 100+cnt)
			msg := nsqdNs.NewMessageWithExt(0, []byte(body), tag.ExtVersion(), tag.GetBytes())
			topic.GetChannel("ch")
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
			cnt++
			timeBase := rand.Intn(3)
			select {
			case <-closeChan:
				t.Logf("producer2 exit, total msg published %v", cnt)
				break loop
			case <-time.After(time.Duration(timeBase) * time.Second):
			}
		}
		cntChan <- cnt
	}(startP[1], clsP2, cntP2)

	go func(start chan int, closeChan chan int, cntChan chan int64) {
		if !producerFirst {
			<-start
		}
		cnt := int64(0)
		t.Logf("producer3 starts")
	loop:
		for {
			tag := tags[2]
			body := fmt.Sprintf("%v tag 3", 100+cnt)
			msg := nsqdNs.NewMessageWithExt(0, []byte(body), tag.ExtVersion(), tag.GetBytes())
			topic.GetChannel("ch")
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
			cnt++
			timeBase := rand.Intn(3)
			select {
			case <-closeChan:
				t.Logf("producer3 exit, total msg published %v", cnt)
				break loop
			case <-time.After(time.Duration(timeBase) * time.Second):
			}
		}
		cntChan <- cnt
	}(startP[2], clsP3, cntP3)

	msg := nsqdNs.NewMessageWithExt(0, []byte("100 tag 0"), tags[0].ExtVersion(), tags[0].GetBytes())
	//consumer behavior
	clsC1 := make(chan int, 1)
	cntC1 := make(chan int64, 1)
	resC1 := make(chan int, 1)
	clsC2 := make(chan int, 1)
	cntC2 := make(chan int64, 1)
	resC2 := make(chan int, 1)
	clsC3 := make(chan int, 1)
	cntC3 := make(chan int64, 1)
	resC3 := make(chan int, 1)

	clsC4 := make(chan int, 1)
	cntC4 := make(chan int64, 1)
	resC4 := make(chan int, 1)

	resChans := make([]chan int, 3)
	resChans[0] = resC1
	resChans[1] = resC2
	resChans[2] = resC3

	clsC := make([]chan int, 3)
	clsC[0] = clsC1
	clsC[1] = clsC2
	clsC[2] = clsC3

	cntC := make([]chan int64, 3)
	cntC[0] = cntC1
	cntC[1] = cntC2
	cntC[2] = cntC3

	go func(closeChan chan int, cntChan chan int64, restart chan int, len int, traceID uint64) {
		//subscribe tag client
		cnt := int64(0)
	restart:
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		client1Params := make(map[string]interface{})
		client1Params["client_id"] = "client_w_tag1"
		client1Params["hostname"] = "client_w_tag1"
		client1Params["desired_tag"] = tagName1
		client1Params["extend_support"] = true
		identify(t, conn, client1Params, frameTypeResponse)
		sub(t, conn, topicName, "ch")
		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		//subsume
	loop:
		for {
			msgOut, eof := recvNextMsgAndCheckExcept4EOF(t, conn, len, traceID, true, true)
			if eof {
				time.Sleep(1 * time.Second)
			} else {
				test.NotNil(t, msgOut)
				cnt++
				t.Logf("con1, cnt: %v %v", cnt, string(msgOut.Body))
			}
			select {
			case <-closeChan:
				t.Logf("consumer1 exit, total msg consuemd %v", cnt)
				conn.Close()
				break loop
			case <-restart:
				t.Logf("consuemr1 restart")
				conn.Close()
				goto restart
			default:
			}
		}
		cntChan <- cnt
	}(clsC1, cntC1, resC1, len(msg.Body), msg.TraceID)

	go func(closeChan chan int, cntChan chan int64, restart chan int, len int, traceID uint64) {
		//subscribe tag client
		cnt := int64(0)
	restart:
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		client1Params := make(map[string]interface{})
		client1Params["client_id"] = "client_w_tag2"
		client1Params["hostname"] = "client_w_tag2"
		client1Params["desired_tag"] = tagName2
		client1Params["extend_support"] = true
		identify(t, conn, client1Params, frameTypeResponse)
		sub(t, conn, topicName, "ch")
		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		//subsume
	loop:
		for {
			msgOut, eof := recvNextMsgAndCheckExcept4EOF(t, conn, len, traceID, true, true)
			if eof {
				time.Sleep(1 * time.Second)
			} else {
				test.NotNil(t, msgOut)
				cnt++
				t.Logf("con2, cnt: %v %v", cnt, string(msgOut.Body))
			}
			select {
			case <-closeChan:
				t.Logf("consumer2 exit, total msg consuemd %v", cnt)
				conn.Close()
				break loop
			case <-restart:
				t.Logf("consuemr2 restart")
				conn.Close()
				goto restart
			default:
			}
		}
		cntChan <- cnt
	}(clsC2, cntC2, resC2, len(msg.Body), msg.TraceID)

	go func(closeChan chan int, cntChan chan int64, restart chan int, len int, traceID uint64) {
		//subscribe tag client
		cnt := int64(0)
	restart:
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		client1Params := make(map[string]interface{})
		client1Params["client_id"] = "client_w_tag3"
		client1Params["hostname"] = "client_w_tag3"
		client1Params["desired_tag"] = tagName3
		client1Params["extend_support"] = true
		identify(t, conn, client1Params, frameTypeResponse)
		sub(t, conn, topicName, "ch")
		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		//subsume
	loop:
		for {
			msgOut, eof := recvNextMsgAndCheckExcept4EOF(t, conn, len, traceID, true, true)
			if eof {
				time.Sleep(1 * time.Second)
			} else {
				test.NotNil(t, msgOut)
				cnt++
				t.Logf("con3, cnt: %v %v", cnt, string(msgOut.Body))
			}
			select {
			case <-closeChan:
				t.Logf("consumer3 exit, total msg consuemd %v", cnt)
				conn.Close()
				break loop
			case <-restart:
				t.Logf("consuemr3 restart")
				conn.Close()
				goto restart
			default:
			}
		}
		cntChan <- cnt
	}(clsC3, cntC3, resC3, len(msg.Body), msg.TraceID)

	//common client consume ALL messages
	go func(closeChan chan int, cntChan chan int64, restart chan int, len int, traceID uint64) {
		//subscribe tag client
		cnt := int64(0)
	restart:
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		client1Params := make(map[string]interface{})
		client1Params["client_id"] = "client_wo_tag"
		client1Params["hostname"] = "client_wo_tag"
		client1Params["extend_support"] = true
		//client1Params["desired_tag"] = "tag3"
		identify(t, conn, client1Params, frameTypeResponse)
		sub(t, conn, topicName, "ch")
		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		//subscribe
	loop:
		for {
			msgOut, eof := recvNextMsgAndCheckExcept4EOF(t, conn, len, traceID, true, true)
			if eof {
				time.Sleep(1 * time.Second)
			} else {
				test.NotNil(t, msgOut)
				cnt++
				t.Logf("con4, cnt: %v %v", cnt, string(msgOut.Body))
			}
			select {
			case <-closeChan:
				t.Logf("consumer4 exit, total msg consuemd %v", cnt)
				conn.Close()
				break loop
			case <-restart:
				t.Logf("consuemr4 restart")
				conn.Close()
				goto restart
			default:
			}
		}
		cntChan <- cnt
	}(clsC4, cntC4, resC4, len(msg.Body), msg.TraceID)

	time.Sleep(5 * time.Second)
	if !producerFirst {
		for idx, start := range startP {
			start <- 1
			t.Logf("signal prducer %v to start", idx+1)
		}
	}

loop:
	for {
		idx := rand.Intn(3)
		t.Logf("restart consumer %v", idx+1)
		resChans[idx] <- 1
		time.Sleep(2 * time.Second)
		select {
		case <-ticker.C:
			t.Logf("close producer & consumer...")
			for _, clsCh := range clsP {
				clsCh <- 1
			}
			time.Sleep(2 * time.Second)
			for _, clsCh := range clsC {
				clsCh <- 1
			}
			clsC4 <- 1
			time.Sleep(2 * time.Second)
			break loop
		default:
		}
	}
	t.Logf("for loop exits")
	time.Sleep(time.Second)
}

func TestWriteAndConsumeTagMix(t *testing.T) {
	topicName := "test_tag_stuck" + strconv.Itoa(int(time.Now().Unix()))
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	tagName := "TAG"
	tag := createJsonHeaderExtWithTag(t, tagName)

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func(wg *sync.WaitGroup) {
		for i := 0; i < 10; i++ {
			msgBody := fmt.Sprintf("this is message %v", i)
			msg := nsqdNs.NewMessage(0, []byte(msgBody))
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
		}
		wg.Done()
	}(&wg1)

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func(wg *sync.WaitGroup) {
		for i := 0; i < 10; i++ {
			msgBody := fmt.Sprintf("this is message %v", i)
			msg := nsqdNs.NewMessageWithExt(0, []byte(msgBody), tag.ExtVersion(), tag.GetBytes())
			_, _, _, _, putErr := topic.PutMessage(msg)
			test.Nil(t, putErr)
		}
		wg.Done()
	}(&wg2)

	wg2.Wait()
	wg1.Wait()

	t.Logf("starts consumers")
	//subscribe tag client
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_w_tag"
	clientParams["hostname"] = "client_w_tag"
	clientParams["desired_tag"] = tagName
	clientParams["extend_support"] = true
	identify(t, conn, clientParams, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	defer conn.Close()

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_wo_tag"
	client1Params["hostname"] = "client_wo_tag"
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)
	defer conn1.Close()

	var tagWg sync.WaitGroup
	tagWg.Add(1)
	go func(wg *sync.WaitGroup) {
		tagCnt := 0
		for {
			msgBody := fmt.Sprintf("this is message %v", tagCnt)
			msgOut := recvNextMsgAndCheckExt(t, conn, len("this is message 0"), 0, true, true)
			test.NotNil(t, msgOut)
			test.Equal(t, msgBody, string(msgOut.Body))
			var jsonHeader map[string]interface{}
			err = json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
			test.Nil(t, err)
			test.Equal(t, tagName, jsonHeader[ext.CLIENT_DISPATCH_TAG_KEY])
			tagCnt++
			if tagCnt == 10 {
				break
			}
		}
		wg.Done()
		test.Equal(t, 10, tagCnt)
	}(&tagWg)

	var noTagWg sync.WaitGroup
	noTagWg.Add(1)
	go func(wg *sync.WaitGroup) {
		cnt := 0
		for {
			msgBody := fmt.Sprintf("this is message %v", cnt)
			msgOut := recvNextMsgAndCheckExt(t, conn1, len("this is message 0"), 0, true, true)
			test.NotNil(t, msgOut)
			test.Equal(t, msgBody, string(msgOut.Body))
			cnt++
			if cnt == 10 {
				break
			}
		}
		wg.Done()
		test.Equal(t, 10, cnt)
	}(&noTagWg)

	tagWg.Wait()
	noTagWg.Wait()
}

func TestStuckOnAnotherTag(t *testing.T) {
	topicName := "test_tag_stuck" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = time.Second
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)

	topic.GetChannel("ch")

	tagName1 := "TAG1"
	//subscribe tag client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag1"
	client1Params["hostname"] = "client_w_tag1"
	client1Params["desired_tag"] = tagName1
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	var msg *nsqdNs.Message
	tagName2 := "TAG2"
	tag2 := createJsonHeaderExtWithTag(t, tagName2)
	for i := 0; i < 10; i++ {
		msg = nsqdNs.NewMessageWithExt(0, []byte("test body tag2"), tag2.ExtVersion(), tag2.GetBytes())
		topic.GetChannel("ch")
		_, _, _, _, putErr := topic.PutMessage(msg)
		test.Nil(t, putErr)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	closeChan := make(chan int, 1)
	go func() {
		msg := recvNextMsgAndCheckWithCloseChan(t, conn1, len(msg.Body), msg.TraceID, true, true, closeChan)
		test.Nil(t, msg)
		wg.Done()
	}()
	t.Logf("sleep %v secs...", 5)
	time.Sleep(5 * time.Second)

	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_w_tag2"
	client2Params["hostname"] = "client_w_tag2"
	client2Params["desired_tag"] = tagName2
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)

	for i := 0; i < 10; i++ {
		msgOut1 := recvNextMsgAndCheckExt(t, conn2, len(msg.Body), msg.TraceID, true, true)
		test.NotNil(t, msgOut1)
	}

	closeChan <- 1
	wg.Wait()

	conn1.Close()
	conn2.Close()
}

func TestPausing(t *testing.T) {
	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	conn, err := mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)
	defer conn.Close()

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, true)
	test.Equal(t, msgOut.Body, []byte("test body"))

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// pause the channel... the client shouldn't receive any more messages
	channel.Pause()

	// sleep to allow the paused state to take effect
	time.Sleep(50 * time.Millisecond)

	msg = nsqdNs.NewMessage(0, []byte("test body2"))
	topic.PutMessage(msg)

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working on the internal clientMsgChan read
	time.Sleep(50 * time.Millisecond)
	msgChan := channel.GetClientMsgChan()
	msg = <-msgChan
	test.Equal(t, msg.Body, []byte("test body2"))

	// unpause the channel... the client should now be pushed a message
	channel.UnPause()

	msg = nsqdNs.NewMessage(0, []byte("test body3"))
	topic.PutMessage(msg)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, true)
	test.Equal(t, msgOut.Body, []byte("test body3"))
}

func TestEmptyCommand(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("\n\n"))
	test.Equal(t, err, nil)

	// if we didn't panic here we're good, see issue #120
}

func TestTcpPUBTRACE(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tcp_pubtrace" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	identify(t, conn, nil, frameTypeResponse)

	// PUBTRACE that's valid
	cmd, _ := nsq.PublishTrace(topicName, "0", 123, make([]byte, 5))
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2+nsqdNs.MsgIDLength+8+4)
	test.Equal(t, data[:2], []byte("OK"))

	// PUBTRACE that's invalid (too big)
	cmd, _ = nsq.PublishTrace(topicName, "0", 123, make([]byte, 105))
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	// note: the trace body length should include the trace id
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE message too big 113 > 100"))

	conn.Close()
	conn, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)
	recvNextMsgAndCheck(t, conn, 5, uint64(123), true)
	conn.Close()

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	//MPUB
	mpub := make([][]byte, 5)
	traceIDList := make([]uint64, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 1)
		traceIDList[i] = uint64(i)
	}
	cmd, _ = nsq.MultiPublishTrace(topicName, "0", traceIDList, mpub)
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2+nsqdNs.MsgIDLength+8+4)
	test.Equal(t, data[:2], []byte("OK"))

	// MPUB body that's invalid (body too big)
	mpub = make([][]byte, 11)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 1148 > 1000"))
}

func TestTcpPub(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_tcp_pub" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	// PUB that's valid
	cmd := nsq.Publish(topicName, make([]byte, 5))
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2)
	test.Equal(t, data[:], []byte("OK"))

	// PUB that's invalid (too big)
	cmd = nsq.Publish(topicName, make([]byte, 105))
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	// note: the trace body length should include the trace id
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE message too big 105 > 100"))

	conn.Close()
	conn, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	recvNextMsgAndCheck(t, conn, 5, uint64(0), true)
	conn.Close()

	connList := make([]net.Conn, 0)
	for i := 0; i < 100; i++ {
		conn, err := mustConnectNSQD(tcpAddr)
		t.Logf("conn %v : %v", i, err)
		test.Equal(t, err, nil)
		defer conn.Close()
		identify(t, conn, nil, frameTypeResponse)
		connList = append(connList, conn)
	}
	goStart := make(chan bool)
	// test several client pub and check pub loop
	for i := 0; i < len(connList); i++ {
		cmd := nsq.Publish(topicName, make([]byte, 5))
		go func(index int) {
			<-goStart
			cmd.WriteTo(connList[index])
		}(i)
	}
	close(goStart)
	for i := 0; i < len(connList); i++ {
		validatePubResponse(t, connList[i])
	}

	conn, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)

	for i := 0; i < len(connList); i++ {
		frameType, data, _ := readFrameResponse(t, conn)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
	conn.Close()
}

func TestTcpPubMultiTopicStats(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_tcp_pub" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")
	topicName2 := "test_tcp_pub2" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName2).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	// PUB that's valid
	cmd := nsq.Publish(topicName, make([]byte, 5))
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	cmd = nsq.Publish(topicName2, make([]byte, 5))
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	t1Stat := nsqd.GetTopicIgnPart(topicName).GetDetailStats().GetPubClientStats()
	test.Equal(t, 1, len(t1Stat))
	test.Equal(t, int64(1), t1Stat[0].PubCount)
	t2Stat := nsqd.GetTopicIgnPart(topicName).GetDetailStats().GetPubClientStats()
	test.Equal(t, 1, len(t2Stat))
	test.Equal(t, int64(1), t2Stat[0].PubCount)
	test.Equal(t, t2Stat[0].RemoteAddress, t1Stat[0].RemoteAddress)
	test.Equal(t, conn.LocalAddr().String(), t1Stat[0].RemoteAddress)
	conn.Close()
	time.Sleep(time.Second)

	t1Stat = nsqd.GetTopicIgnPart(topicName).GetDetailStats().GetPubClientStats()
	test.Equal(t, 0, len(t1Stat))
	t2Stat = nsqd.GetTopicIgnPart(topicName).GetDetailStats().GetPubClientStats()
	test.Equal(t, 0, len(t2Stat))
}

func TestTcpPubPopQueueTimeout(t *testing.T) {
	atomic.StoreInt32(&testPopQueueTimeout, 1)
	defer func() {
		atomic.StoreInt32(&testPopQueueTimeout, 0)
	}()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_tcp_pub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	time.Sleep(time.Millisecond)

	cmd := nsq.Publish(topicName, []byte("12345"))
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ErrPubPopQueueTimeout.Error()))

	atomic.StoreInt32(&testPopQueueTimeout, 0)
	time.Sleep(time.Second)
	cmd = nsq.Publish(topicName, []byte("22345"))
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)
}

func TestTcpMPubPopQueueTimeout(t *testing.T) {
	atomic.StoreInt32(&testPopQueueTimeout, 1)
	defer func() {
		atomic.StoreInt32(&testPopQueueTimeout, 0)
	}()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_tcp_mpub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	time.Sleep(time.Millisecond)

	cmd, _ := nsq.MultiPublish(topicName, [][]byte{[]byte("12345")})
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), ErrPubPopQueueTimeout.Error()))

	atomic.StoreInt32(&testPopQueueTimeout, 0)
	time.Sleep(time.Second)
	cmd, _ = nsq.MultiPublish(topicName, [][]byte{[]byte("22345")})
	cmd.WriteTo(conn)
	frameType, data, err = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s, err: %v", frameType, data, err)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2)
	test.Equal(t, data[:], []byte("OK"))
}
func TestTcpPubWaitQueueFullAndTimeout(t *testing.T) {
	nsqdNs.PubQueue = 5
	atomic.StoreInt32(&testPutMessageTimeout, int32(3+pubWaitTimeout.Seconds()))
	defer func() {
		atomic.StoreInt32(&testPutMessageTimeout, 0)
		nsqdNs.PubQueue = 500
	}()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tcp_pub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	timeoutCnt := int32(0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			identify(t, conn, nil, frameTypeResponse)
			time.Sleep(time.Millisecond)
			s := time.Now()
			cmd := nsq.Publish(topicName, []byte("12345"))
			cmd.WriteTo(conn)
			frameType, data, _ := readFrameResponse(t, conn)

			cost := time.Since(s)
			t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
			test.Equal(t, true, cost >= pubWaitTimeout)
			if frameType == 0 {
				return
			}
			atomic.AddInt32(&timeoutCnt, 1)
			test.Equal(t, frameType, frameTypeError)
			timeoutErr := strings.Contains(string(data), ErrPubToWaitTimeout.Error()) || strings.Contains(string(data), ErrPubPopQueueTimeout.Error())
			test.Equal(t, true, timeoutErr)
		}()
	}
	wg.Wait()
	t.Logf("timeout pub cnt : %v", timeoutCnt)
	test.Equal(t, true, timeoutCnt >= 5)

	atomic.StoreInt32(&testPutMessageTimeout, int32(pubWaitTimeout.Seconds()-1))
	timeoutCnt = 0

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			identify(t, conn, nil, frameTypeResponse)
			for j := 0; j < 5; j++ {
				time.Sleep(time.Millisecond)
				s := time.Now()
				cmd := nsq.Publish(topicName, []byte("12345"))
				cmd.WriteTo(conn)
				frameType, data, _ := readFrameResponse(t, conn)

				cost := time.Since(s)
				t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
				test.Equal(t, true, int32(cost.Seconds()) >= atomic.LoadInt32(&testPutMessageTimeout))
				if frameType == 0 {
					continue
				}
				atomic.AddInt32(&timeoutCnt, 1)
				test.Equal(t, frameType, frameTypeError)
				timeoutErr := strings.Contains(string(data), ErrPubToWaitTimeout.Error()) || strings.Contains(string(data), ErrPubPopQueueTimeout.Error())
				test.Equal(t, true, timeoutErr)
			}
		}()
	}
	wg.Wait()
	t.Logf("timeout pub cnt : %v", timeoutCnt)
	test.Equal(t, int32(0), timeoutCnt)
}

func TestTcpMPubWaitQueueFullAndTimeout(t *testing.T) {
	nsqdNs.PubQueue = 5
	atomic.StoreInt32(&testPutMessageTimeout, int32(3+pubWaitTimeout.Seconds()))
	defer func() {
		atomic.StoreInt32(&testPutMessageTimeout, 0)
		nsqdNs.PubQueue = 500
	}()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tcp_mpub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	timeoutCnt := int32(0)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			identify(t, conn, nil, frameTypeResponse)
			time.Sleep(time.Millisecond)
			s := time.Now()
			cmd, _ := nsq.MultiPublish(topicName, [][]byte{[]byte("12345")})
			cmd.WriteTo(conn)
			frameType, data, _ := readFrameResponse(t, conn)
			cost := time.Since(s)
			t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
			test.Equal(t, true, cost >= pubWaitTimeout)
			if frameType == 0 {
				return
			}
			atomic.AddInt32(&timeoutCnt, 1)
			test.Equal(t, frameType, frameTypeError)
			timeoutErr := strings.Contains(string(data), ErrPubToWaitTimeout.Error()) || strings.Contains(string(data), ErrPubPopQueueTimeout.Error())
			test.Equal(t, true, timeoutErr)
		}()
	}
	wg.Wait()
	t.Logf("timeout pub cnt : %v", timeoutCnt)
	test.Equal(t, true, timeoutCnt >= 15)

	atomic.StoreInt32(&testPutMessageTimeout, int32(pubWaitTimeout.Seconds()-1))
	timeoutCnt = 0

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			identify(t, conn, nil, frameTypeResponse)
			for j := 0; j < 5; j++ {
				time.Sleep(time.Millisecond)
				s := time.Now()
				cmd, _ := nsq.MultiPublish(topicName, [][]byte{[]byte("12345")})
				cmd.WriteTo(conn)
				frameType, data, _ := readFrameResponse(t, conn)
				cost := time.Since(s)
				t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
				test.Equal(t, true, int32(cost.Seconds()) >= atomic.LoadInt32(&testPutMessageTimeout))
				if frameType == 0 {
					continue
				}
				atomic.AddInt32(&timeoutCnt, 1)
				test.Equal(t, frameType, frameTypeError)
				timeoutErr := strings.Contains(string(data), ErrPubToWaitTimeout.Error()) || strings.Contains(string(data), ErrPubPopQueueTimeout.Error())
				test.Equal(t, true, timeoutErr)
			}
		}()
	}
	wg.Wait()
	t.Logf("timeout pub cnt : %v", timeoutCnt)
	test.Equal(t, int32(0), timeoutCnt)
}

func TestTcpPubWaitTooMuchBytes(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	opts.MaxPubWaitingSize = 50
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tcp_pub_toomuch" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	errCnt := int32(0)
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	identify(t, conn, nil, frameTypeResponse)
	s := time.Now()
	cmd := nsq.Publish(topicName, make([]byte, opts.MaxPubWaitingSize+1))
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	cost := time.Since(s)
	t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
	atomic.AddInt32(&errCnt, 1)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, true, strings.Contains(string(data), "E_PUB_TOO_MUCH_WAITING"))
	t.Logf("timeout pub cnt : %v", errCnt)
	test.Equal(t, true, errCnt >= 1)
}

func TestTcpMPubWaitTooMuchBytes(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 200
	opts.MaxBodySize = 1000
	opts.MaxPubWaitingSize = 50
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tcp_mpub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	timeoutCnt := int32(0)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			identify(t, conn, nil, frameTypeResponse)
			s := time.Now()
			cmd, _ := nsq.MultiPublish(topicName, [][]byte{make([]byte, opts.MaxPubWaitingSize)})
			cmd.WriteTo(conn)
			frameType, data, _ := readFrameResponse(t, conn)
			cost := time.Since(s)
			t.Logf("frameType: %d, data: %s, cost: %s", frameType, data, cost)
			if frameType == 0 {
				return
			}
			atomic.AddInt32(&timeoutCnt, 1)
			test.Equal(t, frameType, frameTypeError)
			test.Equal(t, true, strings.Contains(string(data), "E_PUB_TOO_MUCH_WAITING"))
		}()
	}
	wg.Wait()
	t.Logf("timeout pub cnt : %v", timeoutCnt)
	test.Equal(t, true, timeoutCnt >= 1)
}

func TestTcpMpubExt(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_tcp_mpub_ext" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicWithExt(topicName, 0, false).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	var msgExtList []*nsq.MsgExt
	var msgBody [][]byte
	// PUB ext to non-ext topic
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			msgExtList = append(msgExtList, &nsq.MsgExt{})
			msgBody = append(msgBody, []byte("msg has no ext"))
		} else {
			msgExtList = append(msgExtList, &nsq.MsgExt{
				TraceID:     123,
				DispatchTag: "desiredTag",
				Custom:      map[string]interface{}{"key1": "val1", "key2": "val2"},
			})
			msgBody = append(msgBody, []byte("msg has ext"))
		}
	}

	cmd, err := nsq.MultiPublishWithJsonExt(topicName, "0", msgExtList, msgBody)
	test.Nil(t, err)
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)
	conn.Close()

	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, map[string]interface{}{"extend_support": true}, frameTypeResponse)
	test.Equal(t, err, nil)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	isOdd := false
	cnt := 0
	for {
		frameType, data, err := readFrameResponse(t, conn)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessageWithExt(data, true)
		//test.Equal(t, 5, len(msgOut.Body))
		if !isOdd {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			var extMap map[string]interface{}
			err := json.Unmarshal(msgOut.ExtBytes, &extMap)
			test.Nil(t, err)
			test.Assert(t, len(extMap) == 0, "extMap size is not 0: %v", extMap)
		} else {
			test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
			var extMap map[string]interface{}
			err := json.Unmarshal(msgOut.ExtBytes, &extMap)
			test.Nil(t, err)
			test.Assert(t, extMap["key1"] == "val1", "ext json kv invalid %v", extMap["key1"])
			test.Assert(t, extMap["key2"] == "val2", "ext json kv invalid %v", extMap["key2"])

		}
		isOdd = !isOdd
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		if cnt++; cnt == 10 {
			break
		}
	}
	conn.Close()

	connList := make([]net.Conn, 0)
	for i := 0; i < 100; i++ {
		conn, err := mustConnectNSQD(tcpAddr)
		t.Logf("conn %v: %v", i, err)
		test.Equal(t, err, nil)
		defer conn.Close()
		identify(t, conn, nil, frameTypeResponse)
		connList = append(connList, conn)
	}
	goStart := make(chan bool)
	// test several client pub and check pub loop
	for i := 0; i < len(connList); i++ {
		mbody := make([][]byte, 5)
		for j := 0; j < len(mbody); j++ {
			mbody[j] = make([]byte, 5)
		}
		cmd, _ := nsq.MultiPublish(topicName, mbody)
		go func(index int) {
			<-goStart
			cmd.WriteTo(connList[index])
		}(i)
	}
	close(goStart)
	for i := 0; i < len(connList); i++ {
		validatePubResponse(t, connList[i])
	}
}

func TestTcpPubExtToNonExtTopic(t *testing.T) {
	testTcpPubExtToNonExtTopic(t, true)
}
func TestTcpPubExtToNonExtTopicNotAllow(t *testing.T) {
	testTcpPubExtToNonExtTopic(t, false)
}

func testTcpPubExtToNonExtTopic(t *testing.T, allow bool) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	opts.AllowExtCompatible = allow
	opts.AllowSubExtCompatible = allow
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_tcp_pub" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	// PUB ext to non-ext topic
	jsonHeaderStr := "{\"##channel_filter_tag\":\"test\"}"
	jhe := ext.NewJsonHeaderExt()
	jhe.SetJsonHeaderBytes([]byte(jsonHeaderStr))
	cmd, err := nsq.PublishWithJsonExt(topicName, "0", make([]byte, 5), jhe.GetBytes())
	test.Nil(t, err)
	cmd.WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	if !allow {
		test.Equal(t, frameType, frameTypeError)
		return
	}
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, len(data), 2)
	test.Equal(t, data[:], []byte("OK"))
	conn.Close()

	conn, err = mustConnectAndSub(t, tcpAddr, topicName, "ch")
	test.Equal(t, err, nil)

	for {
		frameType, data, err := readFrameResponse(t, conn)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		test.Equal(t, uint8(ext.NO_EXT_VER), msgOut.ExtVer)
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		break
	}
	conn.Close()
}

func TestConsumeWithFilter(t *testing.T) {
	testConsumeWithFilter(t, false)
}

func TestConsumeWithInverseFilter(t *testing.T) {
	testConsumeWithFilter(t, true)
}

func testConsumeWithFilter(t *testing.T, inverse bool) {
	topicName := "test_channel_filter" + strconv.Itoa(int(time.Now().Unix()))
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("chA")
	topic.GetChannel("chB")
	topic.GetChannel("chC")
	topic.GetChannel("chGlob")
	topic.GetChannel("chRegexp")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)

	var jext nsq.MsgExt
	jext.Custom = make(map[string]interface{})
	filterExtKey := "my_filter_key"
	pubTotal := 6 * 10
	for i := 0; i < 10; i++ {
		jext.Custom[filterExtKey] = "filterA"
		msgBody := fmt.Sprintf("this is message A %v", i)
		cmd, _ := nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext.Custom[filterExtKey] = "filterAB"
		msgBody = fmt.Sprintf("this is message AB %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext.Custom[filterExtKey] = "filterB"
		msgBody = fmt.Sprintf("this is message B %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext.Custom[filterExtKey] = "filterBA"
		msgBody = fmt.Sprintf("this is message BA %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext.Custom[filterExtKey] = "filterA"
		msgBody = fmt.Sprintf("this is message A %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
		// write some message with no ext to test filter non-ext message
		delete(jext.Custom, filterExtKey)
		msgBody = fmt.Sprintf("this is message no ext %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jext.ToJson())
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
	}
	conn.Close()
	t.Logf("starts consumers")
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_a"
	clientParams["hostname"] = "client_a"

	clientParams["ext_filter"] = nsqdNs.ExtFilterData{1, inverse, filterExtKey, "filterA", nil}
	clientParams["extend_support"] = true
	identify(t, conn, clientParams, frameTypeResponse)
	sub(t, conn, topicName, "chA")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	defer conn.Close()

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_b"
	client1Params["hostname"] = "client_b"
	client1Params["ext_filter"] = nsqdNs.ExtFilterData{1, inverse, filterExtKey, "filterB", nil}
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "chB")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)
	defer conn1.Close()

	conn2, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client2Params := make(map[string]interface{})
	client2Params["client_id"] = "client_c"
	client2Params["hostname"] = "client_c"
	client2Params["extend_support"] = true
	identify(t, conn2, client2Params, frameTypeResponse)
	sub(t, conn2, topicName, "chC")
	_, err = nsq.Ready(1).WriteTo(conn2)
	test.Equal(t, err, nil)
	defer conn2.Close()

	conn3, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client3Params := make(map[string]interface{})
	client3Params["client_id"] = "client_c"
	client3Params["hostname"] = "client_c"
	client3Params["ext_filter"] = nsqdNs.ExtFilterData{3, inverse, filterExtKey, "filterA*", nil}
	client3Params["extend_support"] = true
	identify(t, conn3, client3Params, frameTypeResponse)
	sub(t, conn3, topicName, "chGlob")
	_, err = nsq.Ready(1).WriteTo(conn3)
	test.Equal(t, err, nil)
	defer conn3.Close()

	conn4, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client4Params := make(map[string]interface{})
	client4Params["client_id"] = "client_c"
	client4Params["hostname"] = "client_c"
	client4Params["ext_filter"] = nsqdNs.ExtFilterData{2, inverse, filterExtKey, "^filterA$|^filterB$", nil}
	client4Params["extend_support"] = true
	identify(t, conn4, client4Params, frameTypeResponse)
	sub(t, conn4, topicName, "chRegexp")
	_, err = nsq.Ready(1).WriteTo(conn4)
	test.Equal(t, err, nil)
	defer conn4.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	var cntA int32
	var cntB int32
	var cntAPrefix int32
	var cntAOrB int32
	go func() {
		defer wg.Done()
		msgPrefix := "this is message A"
		for {
			msgOut := recvNextMsgAndCheckExt(t, conn, 0, 0, true, true)
			test.NotNil(t, msgOut)
			test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")

			jsonHeader := make(map[string]interface{})
			err := json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
			test.Nil(t, err)
			if inverse {
				// maybe AB
				test.NotEqual(t, "filterA", jsonHeader[filterExtKey])
				if atomic.AddInt32(&cntA, 1) >= int32(pubTotal-20) {
					break
				}
			} else {
				test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
				test.Equal(t, "filterA", jsonHeader[filterExtKey])
				if atomic.AddInt32(&cntA, 1) >= 20 {
					break
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		msgPrefix := "this is message B"
		for {
			msgOut := recvNextMsgAndCheckExt(t, conn1, 0, 0, true, true)
			test.NotNil(t, msgOut)
			test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")

			jsonHeader := make(map[string]interface{})
			err := json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
			test.Nil(t, err)
			if inverse {
				// maybe BA
				test.NotEqual(t, "filterB", jsonHeader[filterExtKey])
				if atomic.AddInt32(&cntB, 1) >= int32(pubTotal-10) {
					break
				}
			} else {
				test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
				test.Equal(t, "filterB", jsonHeader[filterExtKey])
				if atomic.AddInt32(&cntB, 1) >= 10 {
					break
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		msgPrefix := "this is message A"
		for {
			msgOut := recvNextMsgAndCheckExt(t, conn3, 0, 0, true, true)
			test.NotNil(t, msgOut)
			test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")
			jsonHeader := make(map[string]interface{})
			err := json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
			test.Nil(t, err)
			if inverse {
				test.NotEqual(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
				test.NotEqual(t, "filterA", jsonHeader[filterExtKey])
				test.NotEqual(t, "filterAB", jsonHeader[filterExtKey])
				if atomic.AddInt32(&cntAPrefix, 1) >= int32(pubTotal-30) {
					break
				}
			} else {
				test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
				if len(jsonHeader[filterExtKey].(string)) == 7 {
					test.Equal(t, "filterA", jsonHeader[filterExtKey])
				} else {
					test.Equal(t, "filterAB", jsonHeader[filterExtKey])
				}
				if atomic.AddInt32(&cntAPrefix, 1) >= 30 {
					break
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		msgPrefix := "this is message"
		for {
			msgOut := recvNextMsgAndCheckExt(t, conn4, 0, 0, true, true)
			test.NotNil(t, msgOut)
			test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")
			test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
			jsonHeader := make(map[string]interface{})
			err := json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
			test.Nil(t, err)
			filterData := jsonHeader[filterExtKey]
			if inverse {
				test.NotEqual(t, "filterA", filterData)
				test.NotEqual(t, "filterB", filterData)
				if atomic.AddInt32(&cntAOrB, 1) >= int32(pubTotal-30) {
					break
				}
			} else {
				if filterData != "filterA" && filterData != "filterB" {
					test.Assert(t, false, "filter should match: "+filterData.(string))
				}
				if atomic.AddInt32(&cntAOrB, 1) >= 30 {
					break
				}
			}
		}
	}()
	wg.Wait()
	if inverse {
		test.Equal(t, pubTotal-20, int(cntA))
		test.Equal(t, pubTotal-10, int(cntB))
		test.Equal(t, pubTotal-30, int(cntAPrefix))
		test.Equal(t, pubTotal-30, int(cntAOrB))
	} else {
		test.Equal(t, 20, int(cntA))
		test.Equal(t, 10, int(cntB))
		test.Equal(t, 30, int(cntAPrefix))
		test.Equal(t, 30, int(cntAOrB))
	}
	totalCnt := 0
	for {
		msgOut := recvNextMsgAndCheckExt(t, conn2, 0, 0, true, true)
		test.NotNil(t, msgOut)
		totalCnt++
		if totalCnt >= pubTotal {
			break
		}
	}
	test.Equal(t, pubTotal, totalCnt)
}

func TestConsumeWithFilterComplex(t *testing.T) {
	testConsumeWithFilterComplex(t, false)
}

func TestConsumeWithInverseFilterComplex(t *testing.T) {
	testConsumeWithFilterComplex(t, true)
}

func testConsumeWithFilterComplex(t *testing.T, inverse bool) {
	topicName := "test_channel_filter" + strconv.Itoa(int(time.Now().Unix()))
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")
	topic.GetChannel("chMultiFilter1")
	topic.GetChannel("chMultiFilter2")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)

	jext := make(map[string]interface{})
	filterExtKey := "my_filter_key"
	filterExtKey1 := "my_filter_key2"
	totalCnt := 10 * 6
	for i := 0; i < 10; i++ {
		jext[filterExtKey] = true
		msgBody := fmt.Sprintf("this is message true %v", i)
		jextJson, _ := json.Marshal(jext)
		cmd, _ := nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext[filterExtKey] = false
		msgBody = fmt.Sprintf("this is message false %v", i)
		jextJson, _ = json.Marshal(jext)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext[filterExtKey] = "filterA"
		jext[filterExtKey1] = "filter1A"
		msgBody = fmt.Sprintf("this is message Multi %v", i)
		jextJson, _ = json.Marshal(jext)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext[filterExtKey] = "filterB"
		jext[filterExtKey1] = "filter1B"
		msgBody = fmt.Sprintf("this is message Multi %v", i)
		jextJson, _ = json.Marshal(jext)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		jext[filterExtKey] = "filterA"
		delete(jext, filterExtKey1)
		msgBody = fmt.Sprintf("this is message A %v", i)
		jextJson, _ = json.Marshal(jext)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)

		// write some message with no ext to test filter non-ext message
		delete(jext, filterExtKey)
		delete(jext, filterExtKey1)
		jextJson, _ = json.Marshal(jext)
		msgBody = fmt.Sprintf("this is message no ext %v", i)
		cmd, _ = nsq.PublishWithJsonExt(topicName, "0", []byte(msgBody), jextJson)
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
	}
	conn.Close()
	t.Logf("starts consumers")
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_a"
	clientParams["hostname"] = "client_a"

	clientParams["ext_filter"] = nsqdNs.ExtFilterData{1, inverse, filterExtKey, "true", nil}
	clientParams["extend_support"] = true
	identify(t, conn, clientParams, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	cnt := int32(0)
	time.AfterFunc(time.Second, func() {
		conn.Close()
	})
	t.Logf("check filter1")
	for {
		msgOut := recvNextMsgAndCheckExt(t, conn, 0, 0, true, true)
		if msgOut != nil {
			if !inverse {
				test.Assert(t, false, "should not match any string extend")
			} else {
				break
			}
		} else {
			break
		}
	}

	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_b"
	client1Params["hostname"] = "client_b"
	client1Params["ext_filter"] = nsqdNs.ExtFilterData{4, inverse, "any", "",
		[]nsqdNs.MultiFilterData{
			nsqdNs.MultiFilterData{filterExtKey, "filterA"},
			nsqdNs.MultiFilterData{filterExtKey, "filterB"},
		},
	}
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "chMultiFilter1")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	msgPrefix := "this is message"
	cnt = int32(0)
	for {
		msgOut := recvNextMsgAndCheckExt(t, conn1, 0, 0, true, true)
		test.NotNil(t, msgOut)
		test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")
		test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
		var jsonHeader map[string]interface{}
		err = json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
		test.Nil(t, err)
		filterData := jsonHeader[filterExtKey]
		if inverse {
			test.NotEqual(t, "filterA", filterData)
			test.NotEqual(t, "filterB", filterData)
			if atomic.AddInt32(&cnt, 1) >= int32(totalCnt-30) {
				break
			}
		} else {
			if filterData != "filterA" && filterData != "filterB" {
				t.Logf("got messgae: %v", string(msgOut.ExtBytes))
				test.Assert(t, false, "filter should match: "+filterData.(string))
			}

			if atomic.AddInt32(&cnt, 1) >= 30 {
				break
			}
		}
	}
	conn1.Close()

	conn1, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	client1Params = make(map[string]interface{})
	client1Params["client_id"] = "client_b"
	client1Params["hostname"] = "client_b"
	client1Params["ext_filter"] = nsqdNs.ExtFilterData{4, inverse, "all", "",
		[]nsqdNs.MultiFilterData{
			nsqdNs.MultiFilterData{filterExtKey, "filterA"},
			nsqdNs.MultiFilterData{filterExtKey1, "filter1A"},
		},
	}
	client1Params["extend_support"] = true
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "chMultiFilter2")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)
	msgPrefix = "this is message Multi"
	cnt = int32(0)
	t.Logf("check filter2")
	for {
		msgOut := recvNextMsgAndCheckExt(t, conn1, 0, 0, true, true)
		test.NotNil(t, msgOut)
		if inverse {
		} else {
			test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")
			test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
		}
		var jsonHeader map[string]interface{}
		err = json.Unmarshal(msgOut.ExtBytes, &jsonHeader)
		test.Nil(t, err)
		if inverse {
			if jsonHeader[filterExtKey] == "filterA" && jsonHeader[filterExtKey1] == "filter1A" {
				t.Log(jsonHeader)
				test.Assert(t, false, "filter should not match")
			}
			if atomic.AddInt32(&cnt, 1) >= int32(totalCnt-10) {
				break
			}
		} else {
			test.Equal(t, "filterA", jsonHeader[filterExtKey])
			test.Equal(t, "filter1A", jsonHeader[filterExtKey1])
			if atomic.AddInt32(&cnt, 1) >= 10 {
				break
			}
		}
	}
	conn1.Close()
}

func TestConsumeWithFilterOnNonExtendTopic(t *testing.T) {
	topicName := "test_channel_filter" + strconv.Itoa(int(time.Now().Unix()))
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)

	for i := 0; i < 10; i++ {
		msgBody := fmt.Sprintf("this is message %v", i)
		cmd := nsq.Publish(topicName, []byte(msgBody))
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
	}
	conn.Close()
	t.Logf("starts consumers")
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_a"
	clientParams["hostname"] = "client_a"
	clientParams["ext_filter"] = nsqdNs.ExtFilterData{1, false, "test_ext", "true", nil}
	identify(t, conn, clientParams, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	cnt := int32(0)
	msgPrefix := "this is message"
	for {
		msgOut := recvNextMsgAndCheckExt(t, conn, 0, 0, true, false)
		test.NotNil(t, msgOut)
		test.Assert(t, len(msgOut.Body) >= len(msgPrefix), "body should have enough length")
		test.Equal(t, msgPrefix, string(msgOut.Body)[:len(msgPrefix)])
		if atomic.AddInt32(&cnt, 1) >= 10 {
			break
		}
	}
	conn.Close()
	test.Equal(t, 10, int(cnt))
}

func TestSizeLimits(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// PUB that's valid
	// small body
	nsq.Publish(topicName, make([]byte, 1)).WriteTo(conn)
	validatePubResponse(t, conn)

	// normal body
	nsq.Publish(topicName, make([]byte, 95)).WriteTo(conn)
	validatePubResponse(t, conn)

	// PUB that's invalid (too big)
	nsq.Publish(topicName, make([]byte, 105)).WriteTo(conn)
	frameType, data, _ := readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE message too big 105 > 100"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// PUB thats empty
	nsq.Publish(topicName, []byte{}).WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY invalid body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// MPUB body that's valid
	// mpub small for each body
	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 1)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	// MPUB body that's invalid (body too big)
	mpub = make([][]byte, 11)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_BODY body too big 1148 > 1000"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// MPUB that's invalid (one message empty)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	mpub = append(mpub, []byte{})
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB invalid message(5) body size 0"))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	// MPUB body that's invalid (one of the messages is too big)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 101)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	frameType, data, _ = readFrameResponse(t, conn)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data), fmt.Sprintf("E_BAD_MESSAGE MPUB message too big 101 > 100"))
}

func TestZanTestSkip(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second * 2
	opts.MaxReqTimeout = time.Second * 100
	opts.AllowZanTestSkip = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	//send messages, zan_test and normal combined
	topicName := "test_zan_test_skip" + strconv.Itoa(int(time.Now().Unix()))
	ch := nsqd.GetTopicWithExt(topicName, 0, false).GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)
	var msgExtList []*nsq.MsgExt
	var msgBody [][]byte
	// PUB ext to non-ext topic
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			msgExtList = append(msgExtList, &nsq.MsgExt{
				Custom: map[string]interface{}{"key1": "val1", "key2": "val2"},
			})
			msgBody = append(msgBody, []byte("msg has no zan_test ext"))
		} else {
			msgExtList = append(msgExtList, &nsq.MsgExt{
				TraceID:     123,
				DispatchTag: "desiredTag",
				Custom:      map[string]interface{}{"zan_test": "true", "key1": "val1"},
			})
			msgBody = append(msgBody, []byte("msg has zan_test ext"))
		}
	}

	cmd, err := nsq.MultiPublishWithJsonExt(topicName, "0", msgExtList, msgBody)
	test.Nil(t, err)
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)
	conn.Close()

	//skip zan test messages
	ch.SkipZanTest()
	//consume&validate zan_test messages
	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, map[string]interface{}{"extend_support": true}, frameTypeResponse)
	test.Equal(t, err, nil)
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(10).WriteTo(conn)
	test.Equal(t, err, nil)
	var cnt int
	for {
		frameType, data, err := readFrameResponse(t, conn)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessageWithExt(data, true)
		//test.Equal(t, 5, len(msgOut.Body))

		test.Equal(t, uint8(ext.JSON_HEADER_EXT_VER), msgOut.ExtVer)
		var extMap map[string]interface{}
		err = json.Unmarshal(msgOut.ExtBytes, &extMap)
		test.Nil(t, err)
		test.Assert(t, extMap["zan_test"] == nil, "json ext header should not contains zan_test:%v", extMap)
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		if cnt++; cnt == 5 {
			break
		}
	}
	conn.Close()
}

func TestDelayMessage(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second + time.Millisecond
	opts.MaxReqTimeout = time.Second * 10
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_requeue_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, map[string]interface{}{
		"output_buffer_timeout": 10,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	time.Sleep(opts.QueueScanRefreshInterval)

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)
	topic.ForceFlush()

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))

	time.Sleep(75 * time.Millisecond)

	// requeue with valid timeout
	delayStart := time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID = uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	delayDone := time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout, true)
	test.Equal(t, delayDone < opts.MsgTimeout+time.Duration(time.Millisecond*500*2), true)

	// requeue timeout less than msg timeout
	delayStart = time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout-time.Second).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID = uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout-time.Second, true)
	test.Equal(t, delayDone < opts.MsgTimeout-time.Second+time.Duration(time.Millisecond*500*2), true)

	// requeue timeout larger than msg timeout
	delayStart = time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MsgTimeout+time.Second).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID = uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))

	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MsgTimeout+time.Second, true)
	test.Equal(t, delayDone < opts.MsgTimeout+time.Second+time.Duration(time.Millisecond*500*2), true)

	time.Sleep(500 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(25 * time.Millisecond)

	// requeue duration out of range
	msg = nsqdNs.NewMessage(0, []byte("test body 2"))
	_, _, _, _, err = topic.PutMessage(msg)
	test.Equal(t, err, nil)
	topic.ForceFlush()

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID = uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))

	time.Sleep(75 * time.Millisecond)
	delayStart = time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msg.GetFullMsgID()), opts.MaxReqTimeout+time.Second*2).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheckExtTimeout(t, conn, len(msg.Body), msg.TraceID, false, false, opts.MaxReqTimeout*2)
	msgOutID = uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.NotNil(t, msgOutID)
	test.Equal(t, msgOutID, uint64(msg.ID))

	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone > opts.MaxReqTimeout, true)
	test.Equal(t, delayDone < opts.MaxReqTimeout+time.Second+time.Duration(time.Millisecond*500*2), true)
}

func TestDelayMessageToQueueEnd(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanInterval = time.Millisecond * 20
	opts.MsgTimeout = time.Second * 2
	opts.MaxReqTimeout = time.Second * 100
	opts.MaxConfirmWin = 10
	opts.ReqToEndThreshold = time.Millisecond * 20
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_requeue_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, map[string]interface{}{
		"output_buffer_timeout": 10,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")
	time.Sleep(opts.QueueScanRefreshInterval)

	putCnt := 0
	recvCnt := 0
	finCnt := 0
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)
	putCnt++
	topic.ForceFlush()

	_, err = nsq.Ready(int(opts.MaxConfirmWin)).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	recvCnt++

	time.Sleep(75 * time.Millisecond)

	// requeue with timeout that put to end
	delayToEnd := opts.ReqToEndThreshold * time.Duration(11+int(opts.MaxConfirmWin))
	delayStart := time.Now()
	_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), delayToEnd).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	recvCnt++
	test.Equal(t, uint64(nsq.GetNewMessageID(msgOut.ID[:])) >= uint64(msg.ID), true)
	delayDone := time.Since(delayStart)
	t.Log(delayDone)
	test.Equal(t, delayDone >= delayToEnd, true)
	test.Equal(t, delayDone < delayToEnd+time.Duration(time.Millisecond*500*2), true)

	var longestDelayMsg *nsqdNs.Message
	var largestID uint64
	for i := 1; i < int(opts.MaxConfirmWin)*4; i++ {
		msg := nsqdNs.NewMessage(0, []byte("test body"+strconv.Itoa(i)))
		msg.TraceID = uint64(i)
		topic.PutMessage(msg)
		if i == 9 {
			longestDelayMsg = msg
			t.Logf("longest delay msg %v", msg.ID)
		}
		putCnt++
		if uint64(msg.ID) > largestID {
			largestID = uint64(msg.ID)
		}
	}
	topic.ForceFlush()

	delayStart = time.Now()

	var longestDelayOutMsg *nsq.Message
	// requeue while blocking
	var longestDelay time.Duration
	for i := 0; ; i++ {
		msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, msg.TraceID, false)
		recvCnt++
		t.Logf("recv msg %v, %v", msgOut.ID, recvCnt)
		if uint64(nsq.GetNewMessageID(msgOut.ID[:])) == uint64(longestDelayMsg.ID) {
			test.Equal(t, longestDelayMsg.Body, msgOut.Body)
			_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), delayToEnd).WriteTo(conn)
			longestDelay += delayToEnd
			test.Nil(t, err)
			longestDelayOutMsg = msgOut
			t.Logf("longest delay msg %v, %v", msgOut.ID, recvCnt)
			break
		}
		if i > 10 {
			nsq.Finish(msgOut.ID).WriteTo(conn)
			finCnt++
		} else {
			delay := opts.ReqToEndThreshold
			if i < int(opts.MaxConfirmWin)/2 {
				delay = delay * time.Duration(i)
			}
			nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), delay).WriteTo(conn)
		}
	}

	var msgClientOut *nsq.Message
	reqToEndAttempts := 5
	for finCnt < putCnt+10 {
		msgClientOut = recvNextMsgAndCheckClientMsg(t, conn, 0, msg.TraceID, false)
		recvCnt++
		t.Logf("recv msg %v, %v", msgClientOut.ID, recvCnt)

		traceID := binary.BigEndian.Uint64(msgClientOut.ID[8:])
		if traceID == longestDelayOutMsg.GetTraceID() {
			if reqToEndAttempts < 0 {
				nsq.Finish(msgClientOut.ID).WriteTo(conn)
				finCnt++
				break
			}
			reqToEndAttempts--
			test.Equal(t, longestDelayOutMsg.Body, msgClientOut.Body)
			delayTime := time.Second
			if reqToEndAttempts < 1 {
				delayTime = delayToEnd
			}
			_, err = nsq.Requeue(msgClientOut.ID, delayTime).WriteTo(conn)
			longestDelay += delayTime
			test.Nil(t, err)
		} else {
			finCnt++
			nsq.Finish(msgClientOut.ID).WriteTo(conn)
		}
	}

	test.Equal(t, msgClientOut.Body, longestDelayOutMsg.Body)
	test.Equal(t, msgClientOut.Body, []byte("test body9"))
	if string(msgClientOut.ID[:]) != string(longestDelayOutMsg.ID[:]) {
		test.Equal(t, true, uint64(nsq.GetNewMessageID(msgClientOut.ID[:])) > largestID)
	}

	delayDone = time.Since(delayStart)
	t.Log(delayDone)
	t.Logf("delay should be: %v", longestDelay)
	test.Equal(t, delayDone >= longestDelay, true)
	test.Equal(t, delayDone < longestDelay+opts.MsgTimeout*2+5*opts.QueueScanInterval, true)
	test.Equal(t, true, int(msgClientOut.Attempts) > reqToEndAttempts)

	t.Logf("put %v,  fin : %v, recv: %v", putCnt, finCnt, recvCnt)
	test.Equal(t, true, putCnt <= finCnt)
	test.Equal(t, true, recvCnt-finCnt > 10)
	time.Sleep(time.Second)
}

func TestDelayMessageToQueueEndAgainAndAgain(t *testing.T) {
	// this would test the req of delayed queue message and
	// other delayed queue message can be poped to consumer
	// make sure delayed message will not be blocked too long time if some
	// of them req again and again

	// note: the delayed message will be peeked in batch with MaxWaitingDelayed size
	// so we need test MaxWaitingDelayed req for delayed messages and to see if MaxWaitingDelayed + 1
	// message can be peeked to consumer

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Second * 10
	opts.MaxReqTimeout = time.Second * 100
	opts.MaxConfirmWin = 50
	opts.ReqToEndThreshold = time.Millisecond * 150
	opts.SyncEvery = 1000
	maxIntervalDelayed := opts.ReqToEndThreshold * nsqdNs.MaxWaitingDelayed * 2
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_req_delay_again" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	identify(t, conn, map[string]interface{}{
		"output_buffer_timeout": 10,
	}, frameTypeResponse)

	sub(t, conn, topicName, "ch")
	time.Sleep(opts.QueueScanRefreshInterval)
	_, err = nsq.Ready(nsqdNs.MaxWaitingDelayed).WriteTo(conn)
	test.Equal(t, err, nil)

	putCnt := 0
	recvCnt := 0
	finCnt := 0

	for i := 0; i < nsqdNs.MaxWaitingDelayed*2; i++ {
		msg := nsqdNs.NewMessage(0, []byte("test body"+strconv.Itoa(i+1)))
		msg.TraceID = uint64(i + 1)
		topic.PutMessage(msg)
		putCnt++
	}
	topic.ForceFlush()

	delayStart := time.Now()
	delayToEnd := opts.ReqToEndThreshold * time.Duration(11+int(opts.MaxConfirmWin))
	// delay all messages to the delayed queue
	for i := 0; i < nsqdNs.MaxWaitingDelayed*2; i++ {
		msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
		recvCnt++
		_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), delayToEnd).WriteTo(conn)
		test.Nil(t, err)
		t.Logf("delay msg to end %v, %v", msgOut.ID, recvCnt)
	}
	topic.ForceFlush()
	delayStart2 := time.Now()
	// delay some delayed message again
	for {
		msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
		recvCnt++
		delayDone := time.Since(delayStart)
		t.Logf("===== recv msg %v, %v, delayed: %v", msgOut.ID, recvCnt, delayDone)
		test.Assert(t, delayDone > delayToEnd, "should delay enough")
		if delayDone > delayToEnd*4 {
			t.Errorf("timeout for waiting finish other messages: %v", delayDone)
			break
		}
		delayDone = time.Since(delayStart2)
		if uint64(nsq.GetNewMessageID(msgOut.ID[:])) <= uint64(nsqdNs.MaxWaitingDelayed) {
			t.Logf("delay msg : %v, %v", msgOut.ID, delayDone)
			test.Assert(t, delayDone < delayToEnd+maxIntervalDelayed, "should not delay too long time", delayDone)
			_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.ReqToEndThreshold-time.Millisecond).WriteTo(conn)
			test.Nil(t, err)
			//test.Assert(t, msgOut.Attempts < 6, "delayed again messages should attemp less")
			continue
		}
		t.Logf("fin msg: %v, delayed: %v", msgOut.ID, delayDone)
		nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Assert(t, delayDone < delayToEnd+maxIntervalDelayed, "should not delay too long time")
		finCnt++
		test.Equal(t, uint16(2), msgOut.Attempts)
		if finCnt >= nsqdNs.MaxWaitingDelayed {
			break
		}
	}

	t.Logf("recv %v, put:%v, fincnt: %v", recvCnt, putCnt, finCnt)
	test.Assert(t, finCnt >= nsqdNs.MaxWaitingDelayed, "should consume other delayed messages")
	test.Assert(t, recvCnt > putCnt+putCnt/2, "recv should larger than put")
	//test.Assert(t, recvCnt < putCnt*2+finCnt, "recv should less")
	delayStart = time.Now()
	for finCnt < putCnt {
		msgClientOut := recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
		recvCnt++
		t.Logf("recv msg %v, %v", msgClientOut.ID, recvCnt)

		delayDone := time.Since(delayStart)
		t.Logf("delayed: %v", delayDone)
		test.Assert(t, delayDone < maxIntervalDelayed, "should not delay too long time")

		finCnt++
		t.Logf("fin msg: %v", msgClientOut.ID)
		nsq.Finish(msgClientOut.ID).WriteTo(conn)
	}

	t.Logf("put %v,  fin : %v, recv: %v", putCnt, finCnt, recvCnt)
	test.Equal(t, true, putCnt <= finCnt)
	test.Equal(t, true, putCnt+10 > finCnt)
	test.Equal(t, true, recvCnt-finCnt > putCnt/2)
	//test.Equal(t, true, recvCnt < putCnt*3)
}

func TestDelayManyMessagesToQueueEndWithLeaderChanged(t *testing.T) {
	testDelayManyMessagesToQueueEnd(t, true)
}

func TestDelayManyMessagesToQueueEnd(t *testing.T) {
	testDelayManyMessagesToQueueEnd(t, false)
}

func testDelayManyMessagesToQueueEnd(t *testing.T, changedLeader bool) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.SyncEvery = 10000
	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Second
	opts.MaxConfirmWin = 50
	opts.ReqToEndThreshold = nsqdNs.MaxWaitingDelayed*time.Millisecond*50 + time.Millisecond*800
	opts.MaxReqTimeout = time.Second*10 + opts.ReqToEndThreshold*3
	opts.MaxOutputBufferTimeout = 10 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_requeue_delay" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	putCnt := nsqdNs.MaxWaitingDelayed + 100
	recvCnt := int32(0)
	reqCnt := int32(0)
	finCnt := int32(0)
	myRand := rand.New(rand.NewSource(time.Now().Unix()))
	allmsgs := make(map[int]bool)
	for i := 0; i < putCnt; i++ {
		if i%5 == 0 {
			dms := opts.ReqToEndThreshold/2 + time.Duration(myRand.Intn(int(opts.ReqToEndThreshold*2)))
			delayTs := int(time.Now().Add(dms).UnixNano())
			delayBody := []byte(strconv.Itoa(delayTs))
			msg := nsqdNs.NewMessage(0, delayBody)
			topic.PutMessage(msg)
			allmsgs[int(msg.ID)] = true
		} else {
			msg := nsqdNs.NewMessage(0, []byte("nodelay"))
			topic.PutMessage(msg)
			allmsgs[int(msg.ID)] = true
		}
	}
	topic.ForceFlush()

	dumpCheck := make(map[uint64]*nsq.Message)
	var dumpLock sync.Mutex
	dumpCnt := int32(0)
	gcnt := 5
	msgChan := make(chan nsq.Message, 10)
	for i := 0; i < gcnt; i++ {
		go func() {
		RECONNECT:
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			conn.(*net.TCPConn).SetNoDelay(true)
			identify(t, conn, map[string]interface{}{
				"output_buffer_timeout": 10,
			}, frameTypeResponse)
			subRsp, err := subWaitResp(t, conn, topicName, "ch")
			if changedLeader {
				if atomic.LoadInt32(&finCnt) >= int32(putCnt) {
					return
				}
				if err != nil || string(subRsp) != "OK" {
					conn.Close()
					time.Sleep(time.Millisecond * 3)
					goto RECONNECT
				}
			} else {
				test.Nil(t, err)
				test.Equal(t, "OK", string(subRsp))
			}
			_, err = nsq.Ready(10).WriteTo(conn)
			test.Equal(t, err, nil)

			for {
				msgOut := recvNextMsgAndCheck(t, conn, 0, 0, false)
				if msgOut == nil {
					if changedLeader {
						if atomic.LoadInt32(&finCnt) >= int32(putCnt) {
							break
						}
						conn.Close()
						time.Sleep(time.Millisecond * 3)
						goto RECONNECT
					}
					if atomic.LoadInt32(&finCnt) < int32(putCnt) {
						t.Logf("\033[31m error recv: %v, %v, %v", atomic.LoadInt32(&recvCnt),
							atomic.LoadInt32(&reqCnt), atomic.LoadInt32(&finCnt))
						conn.Close()
						time.Sleep(time.Millisecond * 3)
						goto RECONNECT
					}
					break
				}
				if atomic.AddInt32(&recvCnt, 1) >= int32(putCnt) {
					t.Logf("recving: %v, %v, %v", atomic.LoadInt32(&recvCnt),
						atomic.LoadInt32(&reqCnt), atomic.LoadInt32(&finCnt))
				}
				if len(msgOut.Body) >= 10 {
					delayTs, err := strconv.Atoi(string(msgOut.Body))
					test.Nil(t, err)
					now := int(time.Now().UnixNano())
					if delayTs > now+int(time.Millisecond) {
						if msgOut.Attempts > 1 {
							t.Errorf("\033[31m got delayed message early: %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
						}
						nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), time.Duration(delayTs-now)).WriteTo(conn)
						atomic.AddInt32(&reqCnt, 1)
						continue
					} else if now-delayTs > int(opts.QueueScanInterval*3+500*time.Millisecond) {
						t.Logf("\033[31m got delayed message too late: %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
						if msgOut.Attempts > 3 {
							t.Errorf("\033[31m got delayed message too late: %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
						} else if now-delayTs > int(opts.QueueScanInterval*3+time.Second*10) {
							t.Errorf("\033[31m got delayed message too late: %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
						}
					}
				} else {
					test.Equal(t, "nodelay", string(msgOut.Body))
				}

				dumpLock.Lock()
				received := false
				var dup *nsq.Message
				msgID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
				dup, received = dumpCheck[msgID]
				dumpCheck[msgID] = msgOut
				dumpLock.Unlock()
				if received {
					if changedLeader {
						t.Logf("found duplicate message fin %v", dup)
					} else {
						test.Assert(t, false, fmt.Sprintf("should no duplicate message fin %v", dup))
					}
					atomic.AddInt32(&dumpCnt, 1)
				}
				if msgOut.Attempts >= 3 {
					if changedLeader {
						t.Logf("found message attempts 3 more %v", msgOut)
					} else {
						test.Assert(t, msgOut.Attempts < 3, "should never attempt more than 2")
					}
				}
				nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
				newCnt := atomic.LoadInt32(&finCnt)
				if !received {
					newCnt = atomic.AddInt32(&finCnt, 1)
				}
				msgChan <- *msgOut
				if newCnt >= int32(putCnt) {
					break
				}
			}
		}()
	}
	done := false
	changed := false
	for !done {
		select {
		case m := <-msgChan:
			msgID := uint64(nsq.GetNewMessageID(m.ID[:]))
			delete(allmsgs, int(msgID))
			if len(allmsgs) == putCnt/2 && changedLeader && !changed {
				t.Logf("try disable channel ==== ")
				ch.DisableConsume(true)
				time.Sleep(time.Millisecond * 10)
				t.Logf("try enable channel ==== ")
				ch.DisableConsume(false)
				changed = true
			}
			if len(allmsgs) == 0 {
				t.Logf("done since all msgs is finished")
				done = true
				break
			}
			if atomic.LoadInt32(&finCnt) >= int32(putCnt) {
				done = true
				break
			}
		case <-time.After(time.Second*30 + opts.ReqToEndThreshold*3):
			t.Errorf("\033[31m timeout recv: %v, %v, %v\033[39m\n\n", atomic.LoadInt32(&recvCnt),
				atomic.LoadInt32(&reqCnt), atomic.LoadInt32(&finCnt))
			done = true
			break
		}
	}

	t.Logf("final: %v, %v, %v, %v", atomic.LoadInt32(&recvCnt),
		atomic.LoadInt32(&reqCnt), atomic.LoadInt32(&finCnt),
		atomic.LoadInt32(&dumpCnt))
	test.Equal(t, atomic.LoadInt32(&recvCnt), atomic.LoadInt32(&reqCnt)+atomic.LoadInt32(&finCnt)+
		atomic.LoadInt32(&dumpCnt))
	test.Equal(t, 0, len(allmsgs))
}

func TestTouch(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))

	time.Sleep(opts.MsgTimeout / 2)

	_, err = nsq.Touch(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(opts.MsgTimeout / 2)

	_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	stats := nsqdNs.NewChannelStats(ch, nil, 0)
	test.Equal(t, stats.TimeoutCount, uint64(0))
}

func TestSubOrderedMulti(t *testing.T) {
	topicName := "test_sub_ordered_multi" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = time.Second
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ordered_ch")
	conf := nsqdNs.TopicDynamicConf{
		SyncEvery:    1,
		AutoCommit:   1,
		OrderedMulti: true,
	}
	topic.SetDynamicInfo(conf, nil)

	identify(t, conn, nil, frameTypeResponse)
	_, err = nsq.Subscribe(topicName, "ordered_ch").WriteTo(conn)
	test.Nil(t, err)
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, _, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	// should failed if not ordered sub
	test.Equal(t, frameTypeError, frameType)
	conn.Close()
	time.Sleep(time.Second)

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	resp, err = nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, _, err = nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.NotEqual(t, frameTypeError, frameType)

	conn.Close()
}

func TestSubTimeoutMany(t *testing.T) {
	// test only one message timeout too much and blocking other continue consume
	// should go delayed queue for a while and the channel depth should be 0
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.ClientTimeout = time.Second
	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Second / 2
	opts.MaxConfirmWin = 5
	opts.ReqToEndThreshold = nsqdNs.MaxWaitingDelayed*time.Millisecond*10 + time.Millisecond*80
	opts.MaxReqTimeout = time.Second*10 + opts.ReqToEndThreshold*3
	opts.MaxOutputBufferTimeout = 10 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_timeout_toomuch" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	putCnt := nsqdNs.MaxWaitingDelayed + 100
	myRand := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < putCnt; i++ {
		// generate not-continued timeouted to make confirm interval more than 1
		if i%10 == 0 {
			dms := opts.ReqToEndThreshold/2 + time.Duration(myRand.Intn(int(opts.ReqToEndThreshold*2)))
			delayTs := int(time.Now().Add(dms).UnixNano())
			delayBody := []byte(strconv.Itoa(delayTs))
			msg := nsqdNs.NewMessage(0, delayBody)
			topic.PutMessage(msg)
		} else {
			msg := nsqdNs.NewMessage(0, []byte("nodelay"))
			topic.PutMessage(msg)
		}
	}
	topic.ForceFlush()

	var wg sync.WaitGroup
	done := make(chan struct{})

	gcnt := 5
	for i := 0; i < gcnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		RECONNECT:
			select {
			case <-done:
				return
			default:
			}
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			conn.(*net.TCPConn).SetNoDelay(true)
			identify(t, conn, map[string]interface{}{
				"output_buffer_timeout": 10,
			}, frameTypeResponse)
			subRsp, err := subWaitResp(t, conn, topicName, "ch")
			if err != nil || string(subRsp) != "OK" {
				conn.Close()
				time.Sleep(time.Millisecond * 3)
				goto RECONNECT
			}
			test.Nil(t, err)
			test.Equal(t, "OK", string(subRsp))
			_, err = nsq.Ready(10).WriteTo(conn)
			test.Equal(t, err, nil)

			for {
				select {
				case <-done:
					return
				default:
				}
				msgOut := recvNextMsgAndCheck(t, conn, 0, 0, false)
				if msgOut == nil {
					conn.Close()
					time.Sleep(time.Millisecond * 3)
					goto RECONNECT
				}
				if len(msgOut.Body) >= 10 {
					test.Nil(t, err)
					now := int(time.Now().UnixNano())
					t.Logf("\033[31m got delayed message : %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
					// wait timeout
					continue
				} else {
					test.Equal(t, "nodelay", string(msgOut.Body))
				}
				nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
			}
		}()
	}
	start := time.Now()
	for {
		if ch.Depth() == 0 || ch.Depth() < ch.GetChannelEnd().TotalMsgCnt() {
			break
		}
		t.Logf("current channel: %s", ch.GetChannelDebugStats())
		time.Sleep(time.Second)
		if time.Since(start) > time.Minute*3 {
			t.Error("timeout waiting")
			break
		}
	}
	close(done)
	wg.Wait()
	stats := nsqdNs.NewChannelStats(ch, nil, 0)
	t.Logf("channel stats: %v", stats)
	test.Equal(t, 0, stats.DeferredCount)
	test.Assert(t, stats.TimeoutCount >= nsqdNs.MaxMemReqTimes, "should have enough timeout")
	test.Assert(t, stats.RequeueCount > 2, "should have requeued")
	test.Assert(t, stats.DelayedQueueCount > 1, "should have delayed")
}

func TestSubFewMsgTimeoutAlwaysShouldNotBlocking(t *testing.T) {
	nsqdNs.ChangeIntervalForTest()
	// test few messages timeout too much times (which will less than ready) and blocking other continue consume
	// should go delayed queue for a while and the channel depth should be 0
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Millisecond * 10
	opts.MaxConfirmWin = 50
	opts.ReqToEndThreshold = nsqdNs.MaxWaitingDelayed*time.Millisecond + time.Millisecond*80
	opts.MaxReqTimeout = time.Second*10 + opts.ReqToEndThreshold*3
	opts.MaxOutputBufferTimeout = 10 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_few_timeout_toomuch" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	putCnt := nsqdNs.MaxWaitingDelayed + 100
	for i := 0; i < putCnt; i++ {
		// generate not-continued timeouted to make confirm interval more than 1
		if i <= 3 {
			delayBody := []byte("timeout-message")
			msg := nsqdNs.NewMessage(0, delayBody)
			topic.PutMessage(msg)
		} else {
			msg := nsqdNs.NewMessage(0, []byte("nodelay"))
			topic.PutMessage(msg)
		}
	}
	topic.ForceFlush()

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
	RECONNECT:
		select {
		case <-done:
			return
		default:
		}
		conn, err := mustConnectNSQD(tcpAddr)
		test.Equal(t, err, nil)
		defer conn.Close()
		identify(t, conn, map[string]interface{}{
			"output_buffer_timeout": 10,
		}, frameTypeResponse)
		subRsp, err := subWaitResp(t, conn, topicName, "ch")
		if err != nil || string(subRsp) != "OK" {
			conn.Close()
			time.Sleep(time.Millisecond * 3)
			goto RECONNECT
		}
		test.Nil(t, err)
		test.Equal(t, "OK", string(subRsp))
		_, err = nsq.Ready(1).WriteTo(conn)
		test.Equal(t, err, nil)

		for {
			select {
			case <-done:
				return
			default:
			}
			msgOut := recvNextMsgAndCheck(t, conn, 0, 0, false)
			if msgOut == nil {
				conn.Close()
				time.Sleep(time.Millisecond * 3)
				goto RECONNECT
			}
			if len(msgOut.Body) >= 10 {
				test.Nil(t, err)
				now := int(time.Now().UnixNano())
				t.Logf("\033[31m got timeout message : %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
				// wait timeout
				continue
			} else {
				test.Equal(t, "nodelay", string(msgOut.Body))
			}
			nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
		}
	}()
	start := time.Now()
	for {
		if ch.Depth() == 0 {
			break
		}
		t.Logf("current channel: %s", ch.GetChannelDebugStats())
		time.Sleep(time.Second)
		if time.Since(start) > time.Minute*3 {
			t.Error("timeout waiting")
			break
		}
	}
	close(done)
	wg.Wait()
	stats := nsqdNs.NewChannelStats(ch, nil, 0)
	t.Logf("channel stats: %v", stats)
	test.Equal(t, 0, stats.DeferredCount)
	test.Assert(t, stats.TimeoutCount >= nsqdNs.MaxMemReqTimes, "should have enough timeout")
	test.Assert(t, stats.RequeueCount > 2, "should have requeued")
	test.Assert(t, stats.DelayedQueueCount > 1, "should have delayed")
	test.Assert(t, stats.DelayedQueueCount < 5, "should have less delayed")
}

func TestSubReqToEndFailedPartial(t *testing.T) {
	// req to end success but the return is failed (while leader changed or write is disabled) and should req in memory,
	// later the memory delayed failed and it should be req to end again
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	opts.QueueScanInterval = time.Millisecond * 10
	opts.MsgTimeout = time.Second
	opts.MaxConfirmWin = 50
	opts.ReqToEndThreshold = nsqdNs.MaxWaitingDelayed*time.Millisecond*100 + time.Millisecond*800
	opts.MaxReqTimeout = time.Second*10 + opts.ReqToEndThreshold*3
	opts.MaxOutputBufferTimeout = 10 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)

	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_reqend_failed" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	ch := topic.GetChannel("ch")

	putCnt := nsqdNs.MaxWaitingDelayed + 100
	myRand := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < putCnt; i++ {
		if i == 0 {
			dms := opts.ReqToEndThreshold/2 + time.Duration(myRand.Intn(int(opts.ReqToEndThreshold*2)))
			delayTs := int(time.Now().Add(dms).UnixNano())
			delayBody := []byte(strconv.Itoa(delayTs))
			msg := nsqdNs.NewMessage(0, delayBody)
			topic.PutMessage(msg)
		} else {
			msg := nsqdNs.NewMessage(0, []byte("nodelay"))
			topic.PutMessage(msg)
		}
	}
	topic.ForceFlush()

	var wg sync.WaitGroup
	done := make(chan struct{})

	gcnt := 5
	// make req to end failed by test
	atomic.StoreInt32(&testFailedReqToDelayTimeout, 1)
	defer atomic.StoreInt32(&testFailedReqToDelayTimeout, 0)

	for i := 0; i < gcnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		RECONNECT:
			select {
			case <-done:
				return
			default:
			}
			conn, err := mustConnectNSQD(tcpAddr)
			test.Equal(t, err, nil)
			defer conn.Close()
			conn.(*net.TCPConn).SetNoDelay(true)
			identify(t, conn, map[string]interface{}{
				"output_buffer_timeout": 10,
			}, frameTypeResponse)
			subRsp, err := subWaitResp(t, conn, topicName, "ch")
			if err != nil || string(subRsp) != "OK" {
				conn.Close()
				time.Sleep(time.Millisecond * 3)
				goto RECONNECT
			}
			test.Nil(t, err)
			test.Equal(t, "OK", string(subRsp))
			_, err = nsq.Ready(10).WriteTo(conn)
			test.Equal(t, err, nil)

			for {
				select {
				case <-done:
					return
				default:
				}
				msgOut := recvNextMsgAndCheck(t, conn, 0, 0, false)
				if msgOut == nil {
					conn.Close()
					time.Sleep(time.Millisecond * 3)
					goto RECONNECT
				}
				if len(msgOut.Body) >= 10 {
					delayTs, err := strconv.Atoi(string(msgOut.Body))
					test.Nil(t, err)
					now := int(time.Now().UnixNano())
					t.Logf("\033[31m got delayed message : %v (id %v), now: %v\033[39m\n\n", string(msgOut.Body), msgOut.ID, now)
					reqDelay := time.Duration(delayTs - now)
					if delayTs <= now {
						reqDelay = time.Second
					}
					nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), reqDelay).WriteTo(conn)
					continue
				} else {
					test.Equal(t, "nodelay", string(msgOut.Body))
				}
				nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
			}
		}()
	}
	start := time.Now()
	for {
		if ch.Depth() == 0 {
			break
		}
		t.Logf("current channel: %s", ch.GetChannelDebugStats())
		time.Sleep(time.Second)
		if time.Since(start) > time.Minute*3 {
			t.Error("timeout waiting")
			break
		}
	}
	close(done)
	wg.Wait()
	stats := nsqdNs.NewChannelStats(ch, nil, 0)
	t.Logf("channel stats: %v", stats)
	test.Assert(t, stats.DeferredCount <= 1, "should at most 1 deferred")
	test.Assert(t, stats.TimeoutCount >= 2, "should have timeout")
	test.Assert(t, stats.RequeueCount > 2, "should have requeued")
	test.Equal(t, uint64(1), stats.DelayedQueueCount)
}

func TestSubOrdered(t *testing.T) {
	topicName := "test_sub_ordered" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ordered_ch")

	identify(t, conn, nil, frameTypeResponse)
	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}

	expectedOffset := int64(0)
	var lastMsgID nsq.NewMessageID
	for i := 0; i < 50; i++ {

		msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, msg.TraceID, false)

		msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
		msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
		msgOut.Body = msgOut.Body[12:]
		test.Equal(t, msgOut.Body, msg.Body)
		if expectedOffset != int64(0) {
			if nsq.GetNewMessageID(msgOut.ID[:]) != lastMsgID {
				test.Equal(t, expectedOffset, int64(msgOut.Offset))
			} else {
				t.Logf("got dump message id: %v", lastMsgID)
			}
		}
		expectedOffset = int64(msgOut.Offset) + int64(msgOut.RawSize)
		lastMsgID = nsq.GetNewMessageID(msgOut.ID[:])
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
	conn.Close()
	time.Sleep(time.Second)
	// reconnect and try consume the message
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	defer conn.Close()
	for i := 0; i < 50; i++ {
		msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, msg.TraceID, false)
		msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
		msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
		msgOut.Body = msgOut.Body[12:]
		test.Equal(t, msgOut.Body, msg.Body)
		if expectedOffset != int64(0) {
			if nsq.GetNewMessageID(msgOut.ID[:]) != lastMsgID {
				test.Equal(t, expectedOffset, int64(msgOut.Offset))
			} else {
				t.Logf("got dump message id: %v", lastMsgID)
			}
		}
		expectedOffset = int64(msgOut.Offset) + int64(msgOut.RawSize)
		lastMsgID = nsq.GetNewMessageID(msgOut.ID[:])
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
	}
}

func TestSubOrderedWithFilter(t *testing.T) {
	topicName := "test_sub_ordered" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.SyncTimeout = time.Minute

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  100,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	channel := topic.GetChannel("ordered_ch")

	clientParams := make(map[string]interface{})
	clientParams["client_id"] = "client_b"
	clientParams["hostname"] = "client_b"
	clientParams["ext_filter"] = nsqdNs.ExtFilterData{1, false, "nomatch", "nomatch", nil}
	clientParams["extend_support"] = true
	identify(t, conn, clientParams, frameTypeResponse)

	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	for i := 0; i < 10; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, []byte("first")))
	}

	// since no match, will recv no message
	for {
		time.Sleep(time.Second)
		if channel.Depth() == 0 {
			break
		}
	}
	conn.Close()
	// wait old connection exit
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, []byte("second")))
	}
	// reconnect and try consume without filter the message
	// make sure we can receive new message after remove filter.
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	delete(clientParams, "ext_filter")
	identify(t, conn, clientParams, frameTypeResponse)

	subOrdered(t, conn, topicName, "ordered_ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	defer conn.Close()
	closeConnAfterTimeout(conn, time.Second*10, nil)
	for i := 0; i < 10; i++ {
		msgOut := recvNextMsgAndCheckExt(t, conn, 0, msg.TraceID, true, true)
		test.NotNil(t, msgOut)
		msgOut.Body = msgOut.Body[12:]
		t.Logf("recv: %v", string(msgOut.Body))
		test.Equal(t, msgOut.Body, []byte("second"))
	}
}

func TestSubWithLargeReady(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxRdyCount = 250

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_large_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	for i := 0; i < int(opts.MaxRdyCount*2); i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, []byte("test body")))
	}
	topic.ForceFlush()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	defer conn.Close()

	_, err = nsq.Ready(int(opts.MaxRdyCount)).WriteTo(conn)
	test.Equal(t, err, nil)

	for i := 0; i < int(opts.MaxRdyCount*2); i++ {
		msgOut := recvNextMsgAndCheckClientMsg(t, conn, len(msg.Body), 0, false)
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		if err != nil {
			t.Logf("fin error: %v", err.Error())
		}
		test.Nil(t, err)
	}
}

func TestMaxRdyCount(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxRdyCount = 50
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	data := identify(t, conn, nil, frameTypeResponse)
	r := struct {
		MaxRdyCount int64 `json:"max_rdy_count"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.MaxRdyCount, int64(50))
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(int(opts.MaxRdyCount)).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))

	_, err = nsq.Ready(int(opts.MaxRdyCount) + 1).WriteTo(conn)
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.Equal(t, frameType, int32(1))
	test.Equal(t, string(data), "E_INVALID RDY count 51 out of range 0-50")
}

func TestFatalError(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	_, err = conn.Write([]byte("ASDF\n"))
	test.Equal(t, err, nil)

	resp, err := nsq.ReadResponse(conn)
	test.Equal(t, err, nil)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Equal(t, frameType, int32(1))
	test.Equal(t, strings.HasPrefix(string(data), "E_INVALID "), true)

	_, err = nsq.ReadResponse(conn)
	test.NotNil(t, err)
}

func TestOutputBuffering(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = opts.MsgTimeout
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	outputBufferSize := 256 * 1024
	outputBufferTimeout := int(opts.MsgTimeout / 2 / time.Millisecond)

	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	msg := nsqdNs.NewMessage(0, make([]byte, outputBufferSize-1024))
	topic.PutMessage(msg)

	start := time.Now()
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    outputBufferSize,
		"output_buffer_timeout": outputBufferTimeout,
	}, frameTypeResponse)
	var decoded map[string]interface{}
	json.Unmarshal(data, &decoded)
	v, ok := decoded["output_buffer_size"]
	test.Equal(t, ok, true)
	test.Equal(t, int(v.(float64)), outputBufferSize)
	v, _ = decoded["output_buffer_timeout"]
	test.Equal(t, int(v.(float64)), outputBufferTimeout)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(10).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	end := time.Now()
	t.Logf("recv cost : %v", end.Sub(start))

	test.Equal(t, int(end.Sub(start)/time.Millisecond) >= outputBufferTimeout, true)

	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
}

func TestOutputBufferingValidity(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512 * 1024,
		"output_buffer_timeout": 1000,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    -1,
		"output_buffer_timeout": -1,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 0,
	}, frameTypeResponse)
	// exceed size will be convert to max
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512*1024 + 1,
		"output_buffer_timeout": 0,
	}, frameTypeResponse)

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 1001,
	}, frameTypeError)
	test.Equal(t, string(data), "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid")
}

func TestTLS(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestTLSRequired(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequiredExceptHTTP

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_tls_required" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	subFail(t, conn, topicName, "ch")

	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestTLSAuthRequire(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	// No Certs
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// With Unsigned Cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)

	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

}

func TestTLSAuthRequireVerify(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"

	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	// with no cert
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// with invalid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.NotNil(t, err)

	// with valid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestDeflate(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.DeflateEnabled = true
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.Deflate, true)

	compressConn := flate.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

type readWriter struct {
	io.Reader
	io.Writer
}

func TestSnappy(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.SnappyEnabled = true
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.Snappy, true)

	compressConn := snappy.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	msgBody := make([]byte, 128000)
	w := snappy.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_snappy" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")
	sub(t, rw, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(rw)
	test.Equal(t, err, nil)

	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, msgBody)
	topic.PutMessage(msg)
	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s, %v", frameType, data, resp)
	msgOut, _ := nsq.DecodeMessageWithExt(data, topic.IsExt())
	test.Equal(t, frameType, frameTypeMessage)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)
}

func TestTLSDeflate(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.DeflateEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1":  true,
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1   bool `json:"tls_v1"`
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	test.Equal(t, r.Deflate, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	compressConn := flate.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestSampling(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	num := 1000
	sampleRate := 42
	slack := 10

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	// make sure no timeout while waiting inflight sample
	opts.MsgTimeout = time.Second * 10
	opts.MaxRdyCount = int64(num)
	opts.MaxConfirmWin = int64(num * 10)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"sample_rate": int32(sampleRate),
	}, frameTypeResponse)
	r := struct {
		SampleRate int32 `json:"sample_rate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, r.SampleRate, int32(sampleRate))

	topicName := "test_sampling" + strconv.Itoa(int(time.Now().Unix()))
	testBody := []byte("test body")
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")

	for i := 0; i < num; i++ {
		msg := nsqdNs.NewMessage(0, testBody)
		topic.PutMessage(msg)
	}

	topic.ForceFlush()
	// let the topic drain into the channel
	time.Sleep(50 * time.Millisecond)

	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(num).WriteTo(conn)
	test.Nil(t, err)
	start := time.Now()

	doneChan := make(chan int)
	go func() {
		for {
			resp, err := nsq.ReadResponse(conn)
			if err != nil {
				return
			}
			select {
			case <-doneChan:
				return
			default:
			}
			frameType, data, _ := nsq.UnpackResponse(resp)
			if frameType == frameTypeResponse {
				if !bytes.Equal(data, heartbeatBytes) {
					t.Fatalf("got response not heartbeat:" + string(data))
				}
				nsq.Nop().WriteTo(conn)
				continue
			}
		}
	}()

	go func() {
		for {
			// TODO: check if we read all disk data and waiting ack
			time.Sleep(500 * time.Millisecond)
			numInFlight := channel.GetInflightNum()
			if numInFlight <= int(float64(num)*float64(sampleRate+slack)/100.0) &&
				numInFlight >= int(float64(num)*float64(sampleRate-slack)/100.0) {
				close(doneChan)
				return
			}
			if time.Since(start) > time.Second*30 {
				t.Errorf("timeout waiting sampling")
				close(doneChan)
				return
			}
		}
	}()
	<-doneChan

	time.Sleep(time.Second * 3)
	numInFlight := channel.GetInflightNum()
	test.Equal(t, numInFlight <= int(float64(num)*float64(sampleRate+slack)/100.0), true)
	test.Equal(t, numInFlight >= int(float64(num)*float64(sampleRate-slack)/100.0), true)
}

func TestTLSSnappy(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.SnappyEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1  bool `json:"tls_v1"`
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Equal(t, err, nil)
	test.Equal(t, r.TLSv1, true)
	test.Equal(t, r.Snappy, true)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Equal(t, err, nil)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))

	compressConn := snappy.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameType, frameTypeResponse)
	test.Equal(t, data, []byte("OK"))
}

func TestChannelMsgBacklogRequeueStat(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_channel_msg_requeue_backlog" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 2000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	//one for inflight
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	//one for channel msg chan
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	//init backlogs 2
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)

	topic.ForceFlush()
	_, err = nsq.Ready(2).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut1 := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOut2 := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	nsq.Requeue(nsq.MessageID(msgOut1.GetFullMsgID()), 1*time.Second).WriteTo(conn)
	nsq.Requeue(nsq.MessageID(msgOut2.GetFullMsgID()), 1*time.Second).WriteTo(conn)

	time.Sleep(time.Millisecond)
	tstats := nsqd.GetTopicStats(true, topicName)
	chStats := tstats[0].Channels[0]
	test.Equal(t, int64(3), chStats.Backlogs)

	time.Sleep(2000*time.Millisecond + opts.QueueScanInterval)
	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]

	_, err = nsq.Finish(nsq.MessageID(msgOut1.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)
	_, err = nsq.Finish(nsq.MessageID(msgOut2.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	test.Equal(t, int64(1), chStats.Backlogs)
	time.Sleep(time.Millisecond)
}

func TestChannelMsgBacklogStat(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_channel_msg_backlog" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 2000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	//one for inflight
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	//one for channel msg chan
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	//init backlogs 2
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)
	msg = nsqdNs.NewMessage(0, make([]byte, 100))
	_, _, _, _, err = topic.PutMessage(msg)

	topic.ForceFlush()
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	time.Sleep(500 * time.Millisecond)
	tstats := nsqd.GetTopicStats(true, topicName)
	chStats := tstats[0].Channels[0]
	clientStats := tstats[0].Channels[0].Clients[0]
	test.Equal(t, int64(2), chStats.Backlogs)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(2000*time.Millisecond + opts.QueueScanInterval)
	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]

	test.Equal(t, int64(2), chStats.Backlogs)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(2), clientStats.MessageCount)
	//after rdy=1, read another from backend
	test.Equal(t, int64(1), chStats.Backlogs)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, int64(1), chStats.Backlogs)
	time.Sleep(time.Millisecond)

	_, err = nsq.Ready(2).WriteTo(conn)
	test.Equal(t, err, nil)
	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	//nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	t.Log(chStats)
	test.Equal(t, int64(0), chStats.Backlogs)
}

func TestClientMsgTimeoutReqCount(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))

	topic.ForceFlush()
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)

	tstats := nsqd.GetTopicStats(true, topicName)
	test.Equal(t, 1, len(tstats))
	test.Equal(t, 1, len(tstats[0].Channels))
	test.Equal(t, 1, len(tstats[0].Channels[0].Clients))
	chStats := tstats[0].Channels[0]
	clientStats := tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(1), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.InFlightCount)

	test.Equal(t, 1, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(0), chStats.RequeueCount)
	test.Equal(t, uint64(0), chStats.TimeoutCount)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)
	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(1), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(0), clientStats.RequeueCount)
	test.Equal(t, uint64(0), clientStats.FinishCount)

	test.Equal(t, 0, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(1), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	frameType, data, _ := readFrameResponse(t, conn)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data),
		fmt.Sprintf("E_FIN_FAILED FIN %v failed Message ID not in flight", msgOut.GetFullMsgID()))

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, int64(0), clientStats.InFlightCount)
	test.Equal(t, uint64(1), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(0), clientStats.RequeueCount)
	test.Equal(t, uint64(0), clientStats.FinishCount)

	test.Equal(t, 0, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(1), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(2), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(0), clientStats.RequeueCount)
	test.Equal(t, uint64(0), clientStats.FinishCount)

	test.Equal(t, 1, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(1), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)
	_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), 0).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(3), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(1), clientStats.RequeueCount)
	test.Equal(t, uint64(0), clientStats.FinishCount)

	test.Equal(t, 1, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(2), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)

	_, err = nsq.Requeue(nsq.MessageID(msgOut.GetFullMsgID()), opts.MsgTimeout*2).WriteTo(conn)
	test.Equal(t, err, nil)

	start := time.Now()
	for {
		if time.Since(start) > (opts.MsgTimeout * 2) {
			t.Errorf("timeout waiting for message")
			break
		}
		time.Sleep(time.Millisecond)
		tstats = nsqd.GetTopicStats(true, topicName)
		chStats = tstats[0].Channels[0]
		clientStats = tstats[0].Channels[0].Clients[0]
		t.Log(chStats)
		if chStats.InFlightCount != 2 {
			//wait until the second message is processed
			continue
		}
		test.Equal(t, uint64(2), clientStats.RequeueCount)

		test.Equal(t, 2, chStats.InFlightCount)
		test.Equal(t, 1, chStats.DeferredCount)
		test.Equal(t, uint64(2), chStats.RequeueCount)
		test.Equal(t, uint64(1), chStats.TimeoutCount)
		break
	}

	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(4), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(2), clientStats.RequeueCount)
	test.Equal(t, uint64(0), clientStats.FinishCount)

	// delayed message still in delay, another message waiting fin
	test.Equal(t, 2, chStats.InFlightCount)
	test.Equal(t, 1, chStats.DeferredCount)
	test.Equal(t, uint64(2), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(4), clientStats.MessageCount)
	test.Equal(t, int64(0), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(2), clientStats.RequeueCount)
	test.Equal(t, uint64(1), clientStats.FinishCount)

	test.Equal(t, 1, chStats.InFlightCount)
	test.Equal(t, 1, chStats.DeferredCount)
	test.Equal(t, uint64(2), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	msgOut = recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(5), clientStats.MessageCount)
	test.Equal(t, int64(1), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(2), clientStats.RequeueCount)
	test.Equal(t, uint64(1), clientStats.FinishCount)

	test.Equal(t, 1, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(3), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)
	time.Sleep(time.Millisecond)

	tstats = nsqd.GetTopicStats(true, topicName)
	chStats = tstats[0].Channels[0]
	clientStats = tstats[0].Channels[0].Clients[0]
	test.Equal(t, uint64(5), clientStats.MessageCount)
	test.Equal(t, int64(0), clientStats.InFlightCount)
	test.Equal(t, int64(1), clientStats.TimeoutCount)
	test.Equal(t, uint64(2), clientStats.RequeueCount)
	test.Equal(t, uint64(2), clientStats.FinishCount)

	test.Equal(t, 0, chStats.InFlightCount)
	test.Equal(t, 0, chStats.DeferredCount)
	test.Equal(t, uint64(3), chStats.RequeueCount)
	test.Equal(t, uint64(1), chStats.TimeoutCount)
}

func TestClientMsgTimeout(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Body, msg.Body)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Equal(t, err, nil)

	time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)

	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Equal(t, err, nil)

	frameType, data, _ := readFrameResponse(t, conn)
	test.Equal(t, frameType, frameTypeError)
	test.Equal(t, string(data),
		fmt.Sprintf("E_FIN_FAILED FIN %v failed Message ID not in flight", msgOut.GetFullMsgID()))
}

// test connection no nop and any ack, stats should not block

func TestTimeoutShouldNotBlockStats(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout_requeue" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 10000)))
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 10000)))
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100000)))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 2000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	// ack one message, and wait process and do not response
	// wait heartbeat timeout and get stats should not block
	frameType, data, err := readFrameResponse(t, conn)
	test.Nil(t, err)
	if frameType == frameTypeError {
		t.Errorf(string(data))
		return
	}
	test.Equal(t, frameTypeMessage, frameType)
	msgOut, err := nsq.DecodeMessage(data)
	test.Nil(t, err)
	_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
	test.Nil(t, err)
	time.Sleep(opts.ClientTimeout*2 - time.Second*4)
	cnt := 0
	for {
		b := time.Now()
		stats := nsqd.GetStats(false, false)
		if time.Since(b) > time.Millisecond {
			t.Errorf("should  not block: %v", time.Since(b))
			break
		}

		if len(stats) == 0 {
			t.Errorf("should  not empty: %v", time.Since(b))
			break
		}
		t.Logf("block at %v : %v", time.Now(), time.Since(b))
		time.Sleep(time.Millisecond * 100)
		cnt++
		if cnt > 10 {
			break
		}
	}
}

// fail to finish some messages and wait server requeue.
func TestTimeoutFin(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_cmsg_timeout_requeue" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	attempt := 1
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Attempts, uint16(attempt))
	test.Equal(t, msgOut.Body, msg.Body)

	attempt++
	for i := 0; i < 6; i++ {
		//time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)

		// wait timeout and requeue
		msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)

		msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
		if uint64(msgOutID) == uint64(msg.ID) {
			t.Log(msgOut)
			test.Equal(t, msgOut.Attempts, uint16(attempt))
			test.Equal(t, msgOut.Body, msg.Body)
			attempt++
			if i > 3 {
				_, err = nsq.Finish(nsq.MessageID(msg.GetFullMsgID())).WriteTo(conn)
				test.Nil(t, err)
			}
		} else {
			test.Equal(t, msgOutID, uint64(msg.ID+1))
		}
	}

	time.Sleep(10 * time.Millisecond)

	for {
		msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
		test.NotEqual(t, msgOut.ID, msg.ID)
		break
	}
}

// too much fail to finish some messages and this client should be slow down.
// the slow time should be adjust according to the msg timeout by the client
// and should wait until the enough messages timeout
func TestTimeoutTooMuch(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)

	opts.ClientTimeout = time.Second * 10
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topicName := "test_cmsg_timeout_requeue" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)

	for i := 0; i < 20; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	attempt := 1
	_, err = nsq.Ready(3).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
	msgOutID := uint64(nsq.GetNewMessageID(msgOut.ID[:]))
	test.Equal(t, msgOutID, uint64(msg.ID))
	test.Equal(t, msgOut.Attempts, uint16(attempt))
	test.Equal(t, msgOut.Body, msg.Body)

	startTime := time.Now()
	done := int32(0)
	go func() {
		for {
			time.Sleep(time.Millisecond * 300)
			if atomic.LoadInt32(&done) == 1 {
				break
			}
			if time.Since(startTime) >= time.Second*10 {
				t.Fatalf("should stop test ")
				conn.Close()
			}
		}
	}()
	// wait until slow down threshold
	cnt := 0
	for cnt < 2+5 {
		//time.Sleep(1100*time.Millisecond + opts.QueueScanInterval)
		// wait timeout and requeue
		recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
		cnt++
	}
	if time.Since(startTime) >= time.Second*10 {
		t.Fatalf("test should expect shorter")
	}

	cnt = 0
	for cnt < 21 {
		msgOut := recvNextMsgAndCheck(t, conn, len(msg.Body), msg.TraceID, false)
		if msgOut == nil {
			break
		}
		t.Logf("recv msg: %v, %v", msgOut.ID, cnt)
		_, err = nsq.Finish(nsq.MessageID(msgOut.GetFullMsgID())).WriteTo(conn)
		test.Nil(t, err)
		cnt++
	}
	conn.Close()
	atomic.StoreInt32(&done, 1)
	t.Log(time.Since(startTime))
	// should longer than ready*msg_timout
	if time.Since(startTime) < time.Second*3 {
		t.Errorf("should not stop early")
		t.FailNow()
	}
}

func TestSetChannelOffset(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_channel_setoffset" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	tmpCh := topic.GetChannel("ch")
	tmpCh.EnableTrace = 1
	msg := nsqdNs.NewMessage(0, make([]byte, 100))
	topic.PutMessage(msg)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}
	topic.ForceFlush()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	subTrace(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	msgOut := recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
	msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
	msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
	msgOut.Body = msgOut.Body[12:]
	test.Equal(t, msgOut.Body, msg.Body)

	conn.Close()
	time.Sleep(time.Millisecond * 100)

	msgRawSize := msgOut.RawSize
	conn, err = mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)

	subOffset(t, conn, topicName, "ch", int64(-1))
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	for i := 0; i < 100; i++ {
		topic.PutMessage(nsqdNs.NewMessage(0, make([]byte, 100)))
	}

	msgOut = recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
	msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
	msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
	msgOut.Body = msgOut.Body[12:]
	if msgOut.Offset == uint64(msgRawSize) {
		// this is msg before reset, continue to read for the reset msg
		msgOut = recvNextMsgAndCheckClientMsg(t, conn, 0, 0, false)
		msgOut.Offset = uint64(binary.BigEndian.Uint64(msgOut.Body[:8]))
		msgOut.RawSize = uint32(binary.BigEndian.Uint32(msgOut.Body[8:12]))
		msgOut.Body = msgOut.Body[12:]
	}
	test.Equal(t, int64(msgRawSize*101), int64(msgOut.Offset))
	test.Equal(t, uint64(nsq.GetNewMessageID(msgOut.ID[:])), uint64(msg.ID+101))
	test.Equal(t, msgOut.Body, msg.Body)

	conn.Close()
}

func TestBadFin(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()
	nsqd.GetTopicIgnPart("test_fin").GetChannel("ch")

	identify(t, conn, map[string]interface{}{}, frameTypeResponse)
	sub(t, conn, "test_fin", "ch")
	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)

	var emptyID nsq.MessageID
	fin := nsq.Finish(emptyID)
	fin.Params[0] = emptyID[:]
	_, err = fin.WriteTo(conn)
	test.Equal(t, err, nil)

	frameType, data, _ := readFrameResponse(t, conn)
	test.Equal(t, string(data), "E_INVALID Invalid Message ID")
	test.Equal(t, frameType, frameTypeError)
}

func TestClientAuth(t *testing.T) {
	authResponse := `{"ttl":1, "authorizations":[]}`
	authSecret := "testsecret"
	authError := "E_UNAUTHORIZED AUTH No authorizations found"
	authSuccess := ""
	runAuthTest(t, authResponse, authSecret, authError, authSuccess)

	// now one that will succeed
	authResponse = `{"ttl":10, "authorizations":
		[{"topic":"test", "channels":[".*"], "permissions":["subscribe","publish"]}]
	}`
	authError = ""
	authSuccess = `{"identity":"","identity_url":"","permission_count":1}`
	runAuthTest(t, authResponse, authSecret, authError, authSuccess)

}

func runAuthTest(t *testing.T, authResponse, authSecret, authError, authSuccess string) {
	var err error
	var expectedAuthIP string
	expectedAuthTLS := "false"

	authd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("in test auth handler %s", r.RequestURI)
		r.ParseForm()
		test.Equal(t, r.Form.Get("remote_ip"), expectedAuthIP)
		test.Equal(t, r.Form.Get("tls"), expectedAuthTLS)
		test.Equal(t, r.Form.Get("secret"), authSecret)
		fmt.Fprint(w, authResponse)
	}))
	defer authd.Close()

	addr, err := url.Parse(authd.URL)
	test.Equal(t, err, nil)

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 2
	opts.AuthHTTPAddresses = []string{addr.Host}
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	expectedAuthIP, _, _ = net.SplitHostPort(conn.LocalAddr().String())

	identify(t, conn, map[string]interface{}{
		"tls_v1": false,
	}, nsq.FrameTypeResponse)

	authCmd(t, conn, authSecret, authSuccess)
	if authError != "" {
		readValidate(t, conn, nsq.FrameTypeError, authError)
	} else {
		nsqd.GetTopicIgnPart("test").GetChannel("ch")
		sub(t, conn, "test", "ch")
	}

}

func TestResetChannelToOld(t *testing.T) {
	// test many confirmed messages and waiting inflight is empty,
	// and while confirming message offset, the channel end is changed
	// to old offset.
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second * 2
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	opts.MaxConfirmWin = 10
	opts.QueueScanRefreshInterval = time.Second * 2
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_reset_channel" + strconv.Itoa(int(time.Now().Unix()))
	localTopic := nsqd.GetTopicIgnPart(topicName)
	channel := localTopic.GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	var resetOldEnd nsqdNs.BackendQueueEnd
	var realEnd nsqdNs.BackendQueueEnd
	// PUB that's valid
	for i := 0; i < int(opts.MaxConfirmWin)*6; i++ {
		cmd := nsq.Publish(topicName, make([]byte, 5))
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
		if i == int(opts.MaxConfirmWin) {
			localTopic.ForceFlush()
			resetOldEnd = channel.GetChannelEnd()
		}
	}
	localTopic.ForceFlush()
	conn.Close()
	realEnd = channel.GetChannelEnd()

	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, nil, frameTypeResponse)
	test.Equal(t, err, nil)
	defer conn.Close()
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(15).WriteTo(conn)
	test.Equal(t, err, nil)
	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	recvCnt := 0
	startTime := time.Now()
	go func() {
		for {
			time.Sleep(time.Second)
			// check the client stats for channel
			ccs := channel.GetClients()
			for _, cc := range ccs {
				cs := cc.Stats()
				test.Equal(t, cs.MessageCount, uint64(cs.InFlightCount)+cs.FinishCount+cs.RequeueCount+uint64(cs.TimeoutCount))
			}
			if time.Since(startTime) > time.Second*30 {
				if channel.GetConfirmed().Offset() == realEnd.Offset() {
					return
				}
				t.Errorf("should stop on : %v, %v, %v", recvCnt, channel.GetChannelDebugStats(), realEnd)
				conn.Close()
				return
			}
		}
	}()
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		resp, err := nsq.ReadResponse(conn)
		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)

		test.Nil(t, err)
		if frameType == frameTypeError {
			if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
				break
			}
			if bytes.Contains(data, []byte("E_FIN_FAILED")) {
				continue
			}
			t.Logf("got error response: %v", string(data))
		}
		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			if bytes.Equal(data, heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
				if channel.GetConfirmed().Offset() == realEnd.Offset() {
					break
				}
			}
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		recvCnt++
		time.Sleep(time.Millisecond * 10)
		if recvCnt == int(opts.MaxConfirmWin)*2 {
			continue
		}
		if recvCnt == int(opts.MaxConfirmWin)*2+1 {
			// reset channel to old and will be reset  to  new end by topic flush
			end := channel.GetChannelEnd()
			channel.UpdateQueueEnd(resetOldEnd, false)
			test.Equal(t, end, realEnd)
			channel.UpdateQueueEnd(resetOldEnd, true)
			t.Logf("channel update end to old %v", resetOldEnd)
			test.NotEqual(t, realEnd, resetOldEnd)
			end = channel.GetChannelEnd()
			test.Equal(t, end, resetOldEnd)
			// check the client stats for channel
			ccs := channel.GetClients()
			for _, cc := range ccs {
				cs := cc.Stats()
				test.Equal(t, cs.MessageCount, uint64(cs.InFlightCount)+cs.FinishCount+cs.RequeueCount+uint64(cs.TimeoutCount))
				test.Equal(t, true, cs.InFlightCount >= 0)
				test.Equal(t, true, cs.InFlightCount <= 15)
			}
		}
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)

		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		if err != nil {
			t.Errorf("FIN msg %v error: %v", msgOut.ID, err.Error())
		}
		if recvCnt > int(opts.MaxConfirmWin)*2+1 {
			// force flush will update the new end to channel
			localTopic.ForceFlush()
		}
		if recvCnt > int(opts.MaxConfirmWin)*12 {
			t.Errorf("should stop on : %v, %v, %v", recvCnt, channel.GetChannelDebugStats(), realEnd)
			break
		}
		if time.Since(startTime) > time.Second*30 {
			t.Errorf("should stop on : %v", recvCnt)
			break
		}
	}
}

func TestConsumerEmpty(t *testing.T) {
	// test many confirmed messages and waiting inflight is empty,
	// and while confirming message offset, the channel end is changed
	// to new offset.
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MsgTimeout = time.Second * 2
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	opts.MaxConfirmWin = 10
	opts.QueueScanRefreshInterval = time.Second * 2
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)

	topicName := "test_reset_channel" + strconv.Itoa(int(time.Now().Unix()))
	localTopic := nsqd.GetTopicIgnPart(topicName)
	channel := localTopic.GetChannel("ch")

	identify(t, conn, nil, frameTypeResponse)

	var realEnd nsqdNs.BackendQueueEnd
	// PUB that's valid
	for i := 0; i < int(opts.MaxConfirmWin)*6; i++ {
		cmd := nsq.Publish(topicName, make([]byte, 5))
		cmd.WriteTo(conn)
		validatePubResponse(t, conn)
		if i == int(opts.MaxConfirmWin) {
			localTopic.ForceFlush()
		}
	}
	localTopic.ForceFlush()
	realEnd = channel.GetChannelEnd()
	conn.Close()

	conn, err = mustConnectNSQD(tcpAddr)
	identify(t, conn, nil, frameTypeResponse)
	test.Equal(t, err, nil)
	defer conn.Close()
	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(15).WriteTo(conn)
	test.Equal(t, err, nil)
	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	recvCnt := 0
	startTime := time.Now()
	go func() {
		for {
			time.Sleep(time.Second)
			ccs := channel.GetClients()
			for _, cc := range ccs {
				cs := cc.Stats()
				test.Equal(t, cs.MessageCount, uint64(cs.InFlightCount)+cs.FinishCount+cs.RequeueCount+uint64(cs.TimeoutCount))
			}
			if time.Since(startTime) > time.Second*30 {
				if channel.GetConfirmed().Offset() == realEnd.Offset() {
					return
				}
				t.Errorf("should stop on : %v, %v, %v", recvCnt, channel.GetChannelDebugStats(), realEnd)
				conn.Close()
				return
			}
		}
	}()
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		resp, err := nsq.ReadResponse(conn)
		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)

		test.Nil(t, err)
		if frameType == frameTypeError {
			if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
				break
			}
			if bytes.Contains(data, []byte("E_FIN_FAILED")) {
				continue
			}
			t.Logf("got error response: %v", string(data))
		}
		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			if bytes.Equal(data, heartbeatBytes) {
				cmd := nsq.Nop()
				cmd.WriteTo(conn)
				if channel.GetConfirmed().Offset() == realEnd.Offset() {
					break
				}
			}
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, 5, len(msgOut.Body))
		recvCnt++
		time.Sleep(time.Millisecond * 10)
		if recvCnt == int(opts.MaxConfirmWin)*2+1 {
			// reset channel to old and will be reset  to  new end by topic flush
			end := channel.GetChannelEnd()
			confirmEnd := channel.GetConfirmed()
			channel.SetConsumeOffset(end.Offset(), end.TotalMsgCnt(), true)
			t.Logf("channel update confirmed %v to end %v", confirmEnd, end)
			time.Sleep(time.Second)
			ccs := channel.GetClients()
			for _, cc := range ccs {
				cs := cc.Stats()
				test.Equal(t, cs.MessageCount, uint64(cs.InFlightCount)+cs.FinishCount+cs.RequeueCount+uint64(cs.TimeoutCount))
				//test.Equal(t, int64(0), cs.InFlightCount)
			}
		}
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)

		if recvCnt >= int(opts.MaxConfirmWin)*2+1 && channel.GetConfirmed().Offset() == realEnd.Offset() {
			break
		}
		if err != nil {
			t.Errorf("FIN msg %v error: %v", msgOut.ID, err.Error())
		}
		if recvCnt > int(opts.MaxConfirmWin)*2+1 {
			// force flush will update the new end to channel
			localTopic.ForceFlush()
		}
		if recvCnt > int(opts.MaxConfirmWin)*12 {
			t.Errorf("should stop on : %v, %v, %v", recvCnt, channel.GetChannelDebugStats(), realEnd)
			break
		}
		if time.Since(startTime) > time.Second*30 {
			t.Errorf("should stop on : %v", recvCnt)
			break
		}
	}

	conn.Close()
}
func TestTooMuchClient(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts = adjustDefaultOptsForTest(opts)
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	opts.MaxConnForClient = 2

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	topicName := "test_tcp_pub_timeout" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName).GetChannel("ch")
	ropts := *nsqd.GetOpts()
	ropts.MaxConnForClient = 2
	nsqd.SwapOpts(&ropts)

	identify(t, conn, nil, frameTypeResponse)
	time.Sleep(time.Millisecond)

	cmd := nsq.Publish(topicName, []byte("12345"))
	cmd.WriteTo(conn)
	validatePubResponse(t, conn)

	conn2, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn2.Close()
	t.Logf("second connection should ok")
	identify(t, conn2, nil, frameTypeResponse)

	conn3, err := mustConnectNSQD(tcpAddr)
	if err != nil {
		test.Equal(t, errTooMuchClientConns.Error(), err.Error())
		return
	}
	t.Logf("3rd connection should fail")
	data := identify(t, conn3, nil, frameTypeError)
	test.Equal(t, errTooMuchClientConns.Error(), string(data))
	conn3.Close()

	ropts = *nsqd.GetOpts()
	ropts.MaxConnForClient = 3
	nsqd.SwapOpts(&ropts)

	conn4, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn4.Close()
	t.Logf("4th connection should ok after changed options")
	identify(t, conn4, nil, frameTypeResponse)
}

func TestIOLoopReturnsClientErrWhenSendFails(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return 0, errors.New("write error")
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func TestIOLoopReturnsClientErrWhenSendSucceeds(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return len(b), nil
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func testIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("INVALID_COMMAND\n")), nil
	}

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.KVEnabled = false
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
		defer os.RemoveAll(tmpDir)
	}

	nd, err := nsqdNs.New(opts)
	test.Nil(t, err)
	prot := &protocolV2{ctx: &context{nsqd: nd}}
	defer prot.ctx.nsqd.Exit()

	err = prot.IOLoop(fakeConn)

	test.NotNil(t, err)
	test.Equal(t, strings.HasPrefix(err.Error(), "E_INVALID "), true)
	test.NotNil(t, err.(*protocol.FatalClientErr))
}

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	opts.LogLevel = 0
	_ = &levellogger.SimpleLogger{}
	_, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	ctx := &context{0, nsqd, nil, nil, nil, nil, "", 0}
	p := &protocolV2{ctx}
	c := nsqdNs.NewClientV2(0, nil, ctx.getOpts(), nil)
	params := [][]byte{[]byte("NOP")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func benchmarkProtocolV2PubWithArg(b *testing.B, size int, single bool) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.SimpleLogger{}
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = msg
	}
	topicName := "bench_v2_pub" + strconv.Itoa(int(time.Now().Unix()))
	testTopic := nsqd.GetTopic(topicName, 0, false)

	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := mustConnectNSQD(tcpAddr)
			if err != nil {
				panic(err.Error())
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			num := b.N / runtime.GOMAXPROCS(0) / batchSize
			if single {
				num = b.N / runtime.GOMAXPROCS(0)
			}
			if num <= 0 {
				num = 1
			}
			for i := 0; i < num; i++ {
				var cmd *nsq.Command
				if single {
					cmd = nsq.PublishWithPart(topicName, "0", msg)
				} else {
					cmd, _ = nsq.MultiPublishWithPart(topicName, "0", batch)
				}
				_, err := cmd.WriteTo(rw)
				if err != nil {
					b.Error(err.Error())
					return
				}
				err = rw.Flush()
				if err != nil {
					b.Error(err.Error())
					return
				}
				resp, err := nsq.ReadResponse(rw)
				if err != nil {
					b.Error(err.Error())
					return
				}
				_, data, err := nsq.UnpackResponse(resp)
				if err != nil {
					b.Error(err.Error())
					return
				}

				if bytes.Equal(data, []byte("_heartbeat_")) {
					nsq.Nop().WriteTo(rw)
					rw.Flush()
					continue
				}
				if !bytes.Equal(data, []byte("OK")) {
					b.Error("response not OK :" + string(data))
					return
				}
			}
		}()
	}

	wg.Wait()

	b.StopTimer()
	b.Log(testTopic.GetDetailStats().GetPubClientStats())
	nsqdServer.Exit()

}

func benchmarkProtocolV2Pub(b *testing.B, size int) {
	benchmarkProtocolV2PubWithArg(b, size, false)
}

func benchmarkProtocolV2PubSingle(b *testing.B, size int) {
	benchmarkProtocolV2PubWithArg(b, size, true)
}

func BenchmarkProtocolV2Pub128Single(b *testing.B) { benchmarkProtocolV2PubSingle(b, 128) }
func BenchmarkProtocolV2Pub512Single(b *testing.B) { benchmarkProtocolV2PubSingle(b, 512) }

func BenchmarkProtocolV2Pub256(b *testing.B)  { benchmarkProtocolV2Pub(b, 256) }
func BenchmarkProtocolV2Pub512(b *testing.B)  { benchmarkProtocolV2Pub(b, 512) }
func BenchmarkProtocolV2Pub1k(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024) }
func BenchmarkProtocolV2Pub2k(b *testing.B)   { benchmarkProtocolV2Pub(b, 2*1024) }
func BenchmarkProtocolV2Pub4k(b *testing.B)   { benchmarkProtocolV2Pub(b, 4*1024) }
func BenchmarkProtocolV2Pub8k(b *testing.B)   { benchmarkProtocolV2Pub(b, 8*1024) }
func BenchmarkProtocolV2Pub16k(b *testing.B)  { benchmarkProtocolV2Pub(b, 16*1024) }
func BenchmarkProtocolV2Pub32k(b *testing.B)  { benchmarkProtocolV2Pub(b, 32*1024) }
func BenchmarkProtocolV2Pub64k(b *testing.B)  { benchmarkProtocolV2Pub(b, 64*1024) }
func BenchmarkProtocolV2Pub128k(b *testing.B) { benchmarkProtocolV2Pub(b, 128*1024) }
func BenchmarkProtocolV2Pub256k(b *testing.B) { benchmarkProtocolV2Pub(b, 256*1024) }
func BenchmarkProtocolV2Pub512k(b *testing.B) { benchmarkProtocolV2Pub(b, 512*1024) }
func BenchmarkProtocolV2Pub1m(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024*1024) }

func benchmarkProtocolV2Sub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.SimpleLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        false,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	for i := 0; i < b.N; i++ {
		msg := nsqdNs.NewMessage(0, msg)
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	topic.GetChannel("ch").SetTrace(false)
	b.SetBytes(int64(len(msg)))
	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan, nil, false)
			if err != nil {
				opts.Logger.Output(1, fmt.Sprintf("%v", err))
				b.Error(err.Error())
			}
		}()
		<-rdyChan
	}
	b.Logf("starting :%v", b.N)
	b.StartTimer()

	close(goChan)
	wg.Wait()
	b.Logf("done : %v", b.N)

	b.StopTimer()
	nsqdServer.Exit()
}

func BenchmarkProtocolV2SubExt256(b *testing.B)  { benchmarkProtocolV2SubExt(b, 256) }
func BenchmarkProtocolV2SubExt512(b *testing.B)  { benchmarkProtocolV2SubExt(b, 512) }
func BenchmarkProtocolV2SubExt1k(b *testing.B)   { benchmarkProtocolV2SubExt(b, 1024) }
func BenchmarkProtocolV2SubExt2k(b *testing.B)   { benchmarkProtocolV2SubExt(b, 2*1024) }
func BenchmarkProtocolV2SubExt4k(b *testing.B)   { benchmarkProtocolV2SubExt(b, 4*1024) }
func BenchmarkProtocolV2SubExt8k(b *testing.B)   { benchmarkProtocolV2SubExt(b, 8*1024) }
func BenchmarkProtocolV2SubExt16k(b *testing.B)  { benchmarkProtocolV2SubExt(b, 16*1024) }
func BenchmarkProtocolV2SubExt32k(b *testing.B)  { benchmarkProtocolV2SubExt(b, 32*1024) }
func BenchmarkProtocolV2SubExt64k(b *testing.B)  { benchmarkProtocolV2SubExt(b, 64*1024) }
func BenchmarkProtocolV2SubExt128k(b *testing.B) { benchmarkProtocolV2SubExt(b, 128*1024) }
func BenchmarkProtocolV2SubExt256k(b *testing.B) { benchmarkProtocolV2SubExt(b, 256*1024) }
func BenchmarkProtocolV2SubExt512k(b *testing.B) { benchmarkProtocolV2SubExt(b, 512*1024) }
func BenchmarkProtocolV2SubExt1m(b *testing.B)   { benchmarkProtocolV2SubExt(b, 1024*1024) }

func benchmarkProtocolV2SubExt(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.SimpleLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	workers := runtime.GOMAXPROCS(0)
	extContent := createJsonHeaderExtBenchmark(b)
	for i := 0; i < b.N; i++ {
		msg := nsqdNs.NewMessageWithExt(0, msg, extContent.ExtVersion(), extContent.GetBytes())
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	topic.GetChannel("ch").SetTrace(false)
	b.SetBytes(int64(len(msg) + len(extContent.GetBytes())))
	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//construct tag param
			clientParams := make(map[string]interface{})
			clientId := fmt.Sprintf("client_%v", j)
			clientParams["client_id"] = clientId
			clientParams["hostname"] = clientId

			err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan, clientParams, true)
			if err != nil {
				opts.Logger.Output(1, fmt.Sprintf("%v", err))
				b.Error(err.Error())
			}
		}()
		<-rdyChan
	}
	b.Logf("starting :%v", b.N)
	b.StartTimer()

	close(goChan)
	wg.Wait()
	b.Logf("done : %v", b.N)

	b.StopTimer()
	nsqdServer.Exit()
}

func BenchmarkProtocolV2SubExtTag256(b *testing.B)  { benchmarkProtocolV2SubExtTag(b, 256) }
func BenchmarkProtocolV2SubExtTag512(b *testing.B)  { benchmarkProtocolV2SubExtTag(b, 512) }
func BenchmarkProtocolV2SubExtTag1k(b *testing.B)   { benchmarkProtocolV2SubExtTag(b, 1024) }
func BenchmarkProtocolV2SubExtTag2k(b *testing.B)   { benchmarkProtocolV2SubExtTag(b, 2*1024) }
func BenchmarkProtocolV2SubExtTag4k(b *testing.B)   { benchmarkProtocolV2SubExtTag(b, 4*1024) }
func BenchmarkProtocolV2SubExtTag8k(b *testing.B)   { benchmarkProtocolV2SubExtTag(b, 8*1024) }
func BenchmarkProtocolV2SubExtTag16k(b *testing.B)  { benchmarkProtocolV2SubExtTag(b, 16*1024) }
func BenchmarkProtocolV2SubExtTag32k(b *testing.B)  { benchmarkProtocolV2SubExtTag(b, 32*1024) }
func BenchmarkProtocolV2SubExtTag64k(b *testing.B)  { benchmarkProtocolV2SubExtTag(b, 64*1024) }
func BenchmarkProtocolV2SubExtTag128k(b *testing.B) { benchmarkProtocolV2SubExtTag(b, 128*1024) }
func BenchmarkProtocolV2SubExtTag256k(b *testing.B) { benchmarkProtocolV2SubExtTag(b, 256*1024) }
func BenchmarkProtocolV2SubExtTag512k(b *testing.B) { benchmarkProtocolV2SubExtTag(b, 512*1024) }
func BenchmarkProtocolV2SubExtTag1m(b *testing.B)   { benchmarkProtocolV2SubExtTag(b, 1024*1024) }

func benchmarkProtocolV2SubExtTag(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.SimpleLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topicDynConf := nsqdNs.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil)
	topic.GetChannel("ch")

	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < b.N; i++ {
		tag := fmt.Sprintf("tag%v", i%workers)
		extContent := createJsonHeaderExtWithTagBenchmark(b, tag)
		msg := nsqdNs.NewMessageWithExt(0, msg, extContent.ExtVersion(), extContent.GetBytes())
		topic.PutMessage(msg)
	}
	topic.ForceFlush()
	topic.GetChannel("ch").SetTrace(false)
	b.SetBytes(int64(len(msg)))
	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//construct tag param
			clientParams := make(map[string]interface{})
			tag := fmt.Sprintf("tag%v", j)
			clientId := fmt.Sprintf("client_tag%v", j)
			clientParams["desired_tag"] = tag
			clientParams["client_id"] = clientId
			clientParams["hostname"] = clientId
			clientParams["extend_support"] = true

			err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan, clientParams, true)
			if err != nil {
				opts.Logger.Output(1, fmt.Sprintf("%v", err))
				b.Error(err.Error())
			}
		}()
		<-rdyChan
	}
	b.Logf("starting :%v", b.N)
	b.StartTimer()

	close(goChan)
	wg.Wait()
	b.Logf("done : %v", b.N)

	b.StopTimer()
	nsqdServer.Exit()
}

func subWorker(n int, workers int, tcpAddr *net.TCPAddr, topicName string, rdyChan chan int, goChan chan int, extra map[string]interface{}, ext bool) error {
	conn, err := mustConnectNSQD(tcpAddr)
	if err != nil {
		return err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriterSize(conn, 65536))

	identify(nil, conn, extra, frameTypeResponse)
	sub(nil, conn, topicName, "ch")

	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	//traceLog := &levellogger.SimpleLogger{}
	//traceLog.Output(1, fmt.Sprintf("begin from client: %v", conn.LocalAddr()))
	num := n / workers
	for i := 0; i < num; i++ {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			if err == io.EOF {
				return err
			} else {
				rw.Flush()
				continue
			}
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			return err
		}
		if frameType == frameTypeResponse {
			if !bytes.Equal(data, heartbeatBytes) {
				return errors.New("got response not heartbeat:" + string(data))
			}
			nsq.Nop().WriteTo(rw)
			rw.Flush()
			continue
		}
		if frameType != frameTypeMessage {
			return errors.New("got something else")
		}
		msg, err := nsq.DecodeMessageWithExt(data, ext)
		if err != nil {
			return err
		}
		nsq.Finish(msg.ID).WriteTo(rw)
		if (i+1)%rdyCount == 0 || i+1 == num {
			if i+1 == num {
				nsq.Ready(0).WriteTo(conn)
			}
			rw.Flush()
		}
	}

	rw.Flush()
	conn.Close()
	//traceLog.Output(1, fmt.Sprintf("done from client: %v", conn.LocalAddr()))
	return nil
}

func BenchmarkProtocolV2Sub256(b *testing.B)  { benchmarkProtocolV2Sub(b, 256) }
func BenchmarkProtocolV2Sub512(b *testing.B)  { benchmarkProtocolV2Sub(b, 512) }
func BenchmarkProtocolV2Sub1k(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024) }
func BenchmarkProtocolV2Sub2k(b *testing.B)   { benchmarkProtocolV2Sub(b, 2*1024) }
func BenchmarkProtocolV2Sub4k(b *testing.B)   { benchmarkProtocolV2Sub(b, 4*1024) }
func BenchmarkProtocolV2Sub8k(b *testing.B)   { benchmarkProtocolV2Sub(b, 8*1024) }
func BenchmarkProtocolV2Sub16k(b *testing.B)  { benchmarkProtocolV2Sub(b, 16*1024) }
func BenchmarkProtocolV2Sub32k(b *testing.B)  { benchmarkProtocolV2Sub(b, 32*1024) }
func BenchmarkProtocolV2Sub64k(b *testing.B)  { benchmarkProtocolV2Sub(b, 64*1024) }
func BenchmarkProtocolV2Sub128k(b *testing.B) { benchmarkProtocolV2Sub(b, 128*1024) }
func BenchmarkProtocolV2Sub256k(b *testing.B) { benchmarkProtocolV2Sub(b, 256*1024) }
func BenchmarkProtocolV2Sub512k(b *testing.B) { benchmarkProtocolV2Sub(b, 512*1024) }
func BenchmarkProtocolV2Sub1m(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024*1024) }

func benchmarkProtocolV2MultiSub(b *testing.B, num int) {
	var wg sync.WaitGroup
	b.StopTimer()

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(b)
	//opts.Logger = &levellogger.SimpleLogger{}
	//glog.SetFlags(2, "INFO", "./")
	opts.LogLevel = 0
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, 256)
	b.SetBytes(int64(len(msg) * num))

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < num; i++ {
		topicName := "bench_v2" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(int(time.Now().Unix()))
		topic := nsqd.GetTopicIgnPart(topicName)
		for i := 0; i < b.N; i++ {
			msg := nsqdNs.NewMessage(0, msg)
			topic.PutMessage(msg)
		}
		topic.ForceFlush()
		topic.GetChannel("ch")

		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan, nil, false)
				if err != nil {
					b.Error(err.Error())
				} else {
					b.Logf("sub finished ok")
				}
			}()
			<-rdyChan
		}
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqdServer.Exit()
}

func BenchmarkProtocolV2MultiSub1(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 1) }
func BenchmarkProtocolV2MultiSub2(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 2) }
func BenchmarkProtocolV2MultiSub4(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 4) }
func BenchmarkProtocolV2MultiSub8(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 8) }
func BenchmarkProtocolV2MultiSub16(b *testing.B) { benchmarkProtocolV2MultiSub(b, 16) }
