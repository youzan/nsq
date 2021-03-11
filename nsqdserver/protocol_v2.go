package nsqdserver

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	ps "github.com/prometheus/client_golang/prometheus"
	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqd"
)

const (
	E_INVALID         = "E_INVALID"
	E_TOPIC_NOT_EXIST = "E_TOPIC_NOT_EXIST"
)

const (
	maxTimeout     = time.Hour
	pubWaitTimeout = time.Second * 3
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")
var offsetSplitStr = ":"
var offsetSplitBytes = []byte(offsetSplitStr)

// no need to support this type (count message maybe changed)
//var offsetCountType = "count"

var offsetVirtualQueueType = "virtual_queue"
var offsetTimestampType = "timestamp"
var offsetSpecialType = "special"
var offsetMsgCountType = "msgcount"

var (
	ErrOrderChannelOnSampleRate = errors.New("order consume is not allowed while sample rate is not 0")
	ErrPubToWaitTimeout         = errors.New("pub to wait channel timeout")
	ErrPubPopQueueTimeout       = errors.New("pub timeout while pop wait queue")
	errTooMuchClientConns       = errors.New("too much client connections")
)

func isNetErr(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection refuse") {
		return true
	}
	if strings.Contains(errStr, "use of closed network") {
		return true
	}
	if strings.Contains(errStr, "broken pipe") {
		return true
	}
	if strings.Contains(errStr, "connection reset by peer") {
		return true
	}
	// we ignore timeout error
	return false
}

type protocolV2 struct {
	ctx *context
}

type ConsumeOffset struct {
	OffsetType  string
	OffsetValue int64
}

func (self *ConsumeOffset) ToString() string {
	return self.OffsetType + offsetSplitStr + strconv.FormatInt(self.OffsetValue, 10)
}

func (self *ConsumeOffset) FromString(s string) error {
	values := strings.Split(s, offsetSplitStr)
	if len(values) != 2 {
		return errors.New("invalid consume offset:" + s)
	}
	self.OffsetType = values[0]
	if self.OffsetType != offsetTimestampType &&
		self.OffsetType != offsetSpecialType &&
		self.OffsetType != offsetVirtualQueueType &&
		self.OffsetType != offsetMsgCountType {
		return errors.New("invalid consume offset:" + s)
	}
	v, err := strconv.ParseInt(values[1], 10, 0)
	if err != nil {
		return err
	}
	self.OffsetValue = v
	return nil
}

func (self *ConsumeOffset) FromBytes(s []byte) error {
	values := bytes.Split(s, offsetSplitBytes)
	if len(values) != 2 {
		return errors.New("invalid consume offset:" + string(s))
	}
	self.OffsetType = string(values[0])
	if self.OffsetType != offsetTimestampType &&
		self.OffsetType != offsetSpecialType &&
		self.OffsetType != offsetVirtualQueueType &&
		self.OffsetType != offsetMsgCountType {
		return errors.New("invalid consume offset:" + string(s))
	}
	v, err := strconv.ParseInt(string(values[1]), 10, 0)
	if err != nil {
		return err
	}
	self.OffsetValue = v
	return nil
}

func (p *protocolV2) IOLoop(conn net.Conn) error {
	fdn := atomic.AddInt64(&p.ctx.clientConnNum, 1)
	defer atomic.AddInt64(&p.ctx.clientConnNum, -1)
	if fdn > p.ctx.getOpts().MaxConnForClient {
		protocol.SendFramedResponse(conn, frameTypeError, []byte(errTooMuchClientConns.Error()))
		conn.Close()
		nsqd.NsqLogger().LogWarningf("PROTOCOL(V2) too much clients: %v", fdn)
		return errTooMuchClientConns
	}

	var err error
	var line []byte
	var zeroTime time.Time
	left := make([]byte, 100)
	tmpLine := make([]byte, 100)

	clientID := p.ctx.nextClientID()
	client := nsqd.NewClientV2(clientID, conn, p.ctx.getOpts(), p.ctx.GetTlsConfig())
	client.SetWriteDeadline(zeroTime)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	messagePumpStartedChan := make(chan bool)
	msgPumpStoppedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan, msgPumpStoppedChan)
	<-messagePumpStartedChan

	for {
		if client.GetHeartbeatInterval() > 0 {
			client.SetReadDeadline(time.Now().Add(client.GetHeartbeatInterval() * 3))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
				if strings.Contains(err.Error(), "timeout") {
					// force close conn to wake up conn.write if timeout since
					// the connection may be dead.
					client.Exit()
				}
			}
			break
		}

		if nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		// handle the compatible for message id.
		// Since the new message id is id+traceid. we can not
		// use \n to check line.
		// REQ, FIN, TOUCH (with message id as param) should be handled.
		// FIN 16BYTES\n
		// REQ 16bytes time\n
		// TOUCH 16bytes\n
		isSpecial := false
		params := make([][]byte, 0)
		if len(line) >= 3 {
			if bytes.Equal(line[:3], []byte("FIN")) ||
				bytes.Equal(line[:3], []byte("REQ")) {
				isSpecial = true
				if len(line) < 21 {
					// the line will be invalid after the next read in Bufio reader
					// so we need copy line to temp buffer
					// the read will overwrite the slice line,
					if len(tmpLine) < len(line) {
						tmpLine = append(tmpLine, line[:len(line)-len(tmpLine)]...)
					}
					tmpLine = tmpLine[:len(line)]
					copy(tmpLine, line)

					left = left[:20-len(tmpLine)]
					nr := 0
					nr, err = io.ReadFull(client.Reader, left)
					if err != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", err)
					}
					tmpLine = append(tmpLine, left[:nr]...)

					extra, extraErr := client.Reader.ReadSlice('\n')
					tmpLine = append(tmpLine, extra...)
					line = tmpLine
					if extraErr != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", extraErr)
					}
				}
				params = append(params, line[:3])
				if len(line) >= 21 {
					// 16bytes msg id for both fin/req
					params = append(params, line[4:20])
					if bytes.Equal(line[:3], []byte("REQ")) {
						// it must be REQ
						if len(line) >= 22 {
							params = append(params, line[21:len(line)-1])
						} else {
							nsqd.NsqLogger().LogErrorf("read invalid command line for req:%v", line)
						}
					} else {
						// it must be FIN
						if len(line) != 21 {
							nsqd.NsqLogger().LogErrorf("read invalid command line for fin :%v", line)
						}
					}
				} else {
					nsqd.NsqLogger().LogErrorf("read invalid command line :%v", line)
				}
			} else if len(line) >= 5 {
				if bytes.Equal(line[:5], []byte("TOUCH")) {
					isSpecial = true
					if len(line) < 23 {
						if len(tmpLine) < len(line) {
							tmpLine = append(tmpLine, line[:len(line)-len(tmpLine)]...)
						}
						tmpLine = tmpLine[:len(line)]
						copy(tmpLine, line)
						left = left[:23-len(tmpLine)]
						nr := 0
						nr, err = io.ReadFull(client.Reader, left)
						if err != nil {
							nsqd.NsqLogger().Logf("TOUCH param err:%v", err)
						}
						tmpLine = append(tmpLine, left[:nr]...)
						line = tmpLine
					}
					params = append(params, line[:5])
					if len(line) >= 23 {
						params = append(params, line[6:22])
					} else {
						nsqd.NsqLogger().LogErrorf("read invalid command line :%v", line)
					}
				}
			}
		}
		if p.ctx.getOpts().Verbose || nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		if !isSpecial {
			// trim the '\n'
			line = line[:len(line)-1]
			// optionally trim the '\r'
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			params = bytes.Split(line, separatorBytes)
		}

		if p.ctx.getOpts().Verbose || nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] %v, %v", client, string(params[0]), params)
		}

		var response []byte
		response, err = p.Exec(client, params)
		err = handleRequestReponseForClient(client, response, err)
		if err != nil {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) handle client command: %v failed", line)
			break
		}
	}

	if err != nil {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): client [%s] exiting ioloop with error: %v", client, err)
	}
	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): client [%s] exiting ioloop", client)
	}
	close(client.ExitChan)
	p.ctx.nsqd.CleanClientPubStats(client.String(), "tcp")
	<-msgPumpStoppedChan

	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().Logf("msg pump stopped client %v", client)
	}

	if client.Channel != nil {
		client.Channel.RequeueClientMessages(client.ID, client.String())
		client.Channel.RemoveClient(client.ID, client.GetDesiredTag())
	}
	client.FinalClose()

	return err
}

func shouldHandleAsync(params [][]byte) bool {
	return bytes.Equal(params[0], []byte("PUB")) || bytes.Equal(params[0], []byte("PUB_EXT"))
}

func handleRequestReponseForClient(client *nsqd.ClientV2, response []byte, err error) error {
	if err != nil {
		ctx := ""

		if childErr, ok := err.(protocol.ChildErr); ok {
			if parentErr := childErr.Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
		}

		nsqd.NsqLogger().LogDebugf("Error response for [%s] - %s - %s",
			client, err, ctx)

		sendErr := Send(client, frameTypeError, []byte(err.Error()))
		if sendErr != nil {
			nsqd.NsqLogger().LogWarningf("Send response error: [%s] - %s%s", client, sendErr, ctx)
			return err
		}

		// errors of type FatalClientErr should forceably close the connection
		if _, ok := err.(*protocol.FatalClientErr); ok {
			return err
		}
		return nil
	}

	if response != nil {
		sendErr := Send(client, frameTypeResponse, response)
		if sendErr != nil {
			err = fmt.Errorf("failed to send response - %s", sendErr)
		}
	}

	return err
}

func SendMessage(client *nsqd.ClientV2, msg *nsqd.Message, writeExt bool, buf *bytes.Buffer, needFlush bool) error {
	buf.Reset()
	_, err := msg.WriteToClient(buf, writeExt, client.EnableTrace)
	if err != nil {
		return err
	}

	err = internalSend(client, frameTypeMessage, buf.Bytes(), needFlush)
	if err != nil {
		return err
	}

	return nil
}

func SendNow(client *nsqd.ClientV2, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, true)
}

func internalSend(client *nsqd.ClientV2, frameType int32, data []byte, needFlush bool) error {
	client.LockWrite()
	defer client.UnlockWrite()
	if client.Writer == nil {
		return errors.New("client closed")
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	if needFlush || frameType != frameTypeMessage {
		err = client.Flush()
	}
	return err
}

func Send(client *nsqd.ClientV2, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, false)
}

func (p *protocolV2) Exec(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	// notice: any client.Reader will change the content for params since the params is returned by ReadSlice
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("PUB_TRACE")):
		return p.PUBTRACE(client, params)
	case bytes.Equal(params[0], []byte("PUB_EXT")):
		return p.PUBEXT(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB_TRACE")):
		return p.MPUBTRACE(client, params)
	case bytes.Equal(params[0], []byte("MPUB_EXT")):
		return p.MPUBEXT(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("SUB_ADVANCED")):
		return p.SUBADVANCED(client, params)
	case bytes.Equal(params[0], []byte("SUB_ORDERED")):
		return p.SUBORDERED(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	case bytes.Equal(params[0], []byte("INTERNAL_CREATE_TOPIC")):
		return p.internalCreateTopic(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %v", params))
}

func (p *protocolV2) messagePumpForAllProducerOnly(client *nsqd.ClientV2, startedChan chan bool,
	stoppedChan chan bool) {
	// TODO: for some producer clients which no need pump messages from channel, we hold in the same
	// goroutine to avoid too much unused goroutines.
	// once sub is received we start new pump goroutine.
}

func (p *protocolV2) messagePump(client *nsqd.ClientV2, startedChan chan bool,
	stoppedChan chan bool) {
	var err error
	var buf bytes.Buffer
	var clientMsgChan chan *nsqd.Message
	var subChannel *nsqd.Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	//var shortflusherChan <-chan time.Time
	//shortFlushTimer := time.NewTimer(time.Millisecond)
	var flusherChan <-chan time.Time
	var sampleRate int32

	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	// we do not need output buffer ticker for producer, since all response for producer
	// should be flushed after send. So we init and stop it by default and restart it if sub to channel
	outputBufferTicker := time.NewTicker(client.GetOutputBufferTimeout())
	outputBufferTicker.Stop()

	heartbeatTicker := time.NewTicker(client.GetHeartbeatInterval())
	heartbeatChan := heartbeatTicker.C
	heartbeatFailedCnt := 0
	msgTimeout := client.GetMsgTimeout()
	lastActiveTime := time.Now()
	var extFilter nsqd.IExtFilter
	inverseFilter := false
	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true
	extCompatible := p.ctx.getOpts().AllowSubExtCompatible
	extSupport := client.ExtendSupport()

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan)

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			clientMsgChan = nil
			flusherChan = nil
			outputBufferTicker.Stop()
			//shortflusherChan = nil
			// force flush
			client.LockWrite()
			err = client.Flush()
			client.UnlockWrite()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			clientMsgChan = client.GetTagMsgChannel()
			if clientMsgChan == nil {
				clientMsgChan = subChannel.GetClientMsgChan()
			}
			flusherChan = nil
			outputBufferTicker.Stop()
			//shortflusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = client.GetTagMsgChannel()
			if clientMsgChan == nil {
				clientMsgChan = subChannel.GetClientMsgChan()
			}
			if flusherChan == nil {
				outputBufferTicker.Stop()
				to := client.GetOutputBufferTimeout()
				if to > 0 {
					outputBufferTicker = time.NewTicker(to)
				}
			}
			flusherChan = outputBufferTicker.C
			// Outputbuffer timeout is enough for most clients who want to reduce latency.
			// Throughput is more important than latency in most case so we no need use shorter flush ticker here.

			//shortflusherChan = nil
			//if subChannel.IsWaitingMoreDiskData() {
			//	// no more data in channel, we should flush unflushed soon to reduce latency in low volume topic
			//	shortFlushTimer.Reset(time.Millisecond)
			//	shortflusherChan = shortFlushTimer.C
			//}
		}

		select {
		case <-client.ExitChan:
			goto exit
		//case <-shortflusherChan:
		//	client.LockWrite()
		//	err = client.Flush()
		//	client.UnlockWrite()
		//	if err != nil {
		//		goto exit
		//	}
		//	flushed = true
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			client.LockWrite()
			err = client.Flush()
			client.UnlockWrite()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			nsqd.NsqLogger().Logf("client %v sub to topic %v channel: %v", client,
				subChannel.GetTopicName(),
				subChannel.GetName())
			subEventChan = nil
			tag := client.GetDesiredTag()
			if tag != "" {
				client.SetTagMsgChannel(subChannel.GetOrCreateClientMsgChannel(tag))
			}
			err = client.SwitchToConsumer(subChannel.IsEphemeral())
			if err != nil {
				goto exit
			}
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			identifyEventChan = nil

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}
			if identifyData.ExtFilter.Type != 0 {
				extFilter, err = nsqd.NewExtFilter(identifyData.ExtFilter)
				if err != nil {
					nsqd.NsqLogger().Infof("channel filter %v init failed: %v", identifyData.ExtFilter, err)
				} else {
					nsqd.NsqLogger().Infof("channel filter %v init for client: %v ", identifyData.ExtFilter, client.String())
					inverseFilter = identifyData.ExtFilter.Inverse
				}
			}

			msgTimeout = identifyData.MsgTimeout
			extSupport = client.ExtendSupport()
		case <-heartbeatChan:
			if subChannel != nil && client.IsReadyForMessages() {
				// try wake up the channel
				subChannel.TryWakeupRead()
			}

			if subChannel != nil && subChannel.Depth() <= 0 {
				lastActiveTime = time.Now()
			}
			// close this client if depth is large and the active is long ago.
			// maybe some bug blocking this client, reconnect can solve bug.
			// ignore the tagged client since it can be idle while no tagged messages
			if subChannel != nil && client.GetTagMsgChannel() == nil &&
				!subChannel.IsOrdered() && subChannel.Depth() > 10 &&
				client.IsReadyForMessages() &&
				time.Since(lastActiveTime) > (msgTimeout*10+client.GetHeartbeatInterval()) &&
				subChannel.GetInflightNum() <= 0 && !subChannel.IsPaused() && !subChannel.IsSkipped() {
				// if all are memory delayed, it means no any need to be send to client until
				// delayed timeouted
				nsqd.NsqLogger().Warningf("client %s not active since %v, current : %v, %v, %v", client, lastActiveTime,
					subChannel.Depth(), subChannel.DepthTimestamp(), subChannel.GetChannelDebugStats())
				goto exit
			}
			err = Send(client, frameTypeResponse, heartbeatBytes)
			nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] send heartbeat", client)
			if err != nil {
				heartbeatFailedCnt++
				nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] send heartbeat failed %v times, %v", client, heartbeatFailedCnt, err)
				if heartbeatFailedCnt > 2 || isNetErr(err) {
					goto exit
				}
			} else {
				heartbeatFailedCnt = 0
			}
		case msg, ok := <-clientMsgChan:
			if !ok {
				goto exit
			}

			// ordered channel sample is not allowed
			if sampleRate > 0 && rand.Int31n(100) > sampleRate && msg.DelayedType != nsqd.ChannelDelayed {
				// FIN automatically, all message will not wait to confirm if not sending,
				// and the reader keep moving forward.
				offset, confirmedCnt, changed := subChannel.ConfirmMsgWithoutGoInflight(msg)
				if changed && p.ctx.nsqdCoord != nil {
					p.ctx.nsqdCoord.SetChannelConsumeOffsetToCluster(subChannel, int64(offset), confirmedCnt, true)
				}
				continue
			}
			if extFilter != nil && subChannel.IsExt() {
				matched := extFilter.Match(msg)
				if inverseFilter {
					matched = !matched
				}
				if !matched {
					if nsqd.NsqLogger().Level() >= levellogger.LOG_DETAIL {
						nsqd.NsqLogger().Debugf("channel %v filtered message %v", subChannel.GetName(), nsqd.PrintMessageNoBody(msg))
					}
					subChannel.ConfirmMsgWithoutGoInflight(msg)
					continue
				}
			}
			// ordered channel will never delayed
			if subChannel.ShouldWaitDelayed(msg) {
				subChannel.ConfirmMsgWithoutGoInflight(msg)
				continue
			}
			// avoid re-send some confirmed message,
			// this may happen while the channel reader is reset to old position
			// due to some retry or leader change.
			if subChannel.IsConfirmed(msg) {
				subChannel.ConfirmMsgWithoutGoInflight(msg)
				continue
			}

			shouldSend, err := subChannel.StartInFlightTimeout(msg, client, client.String(), msgTimeout)
			if !shouldSend || err != nil {
				continue
			}

			lastActiveTime = time.Now()
			client.SendingMessage()
			if subChannel.IsExt() && !extSupport && !extCompatible {
				// while the topic upgraded to the ext, we should close all the old client
				// which not support the ext.
				err = errors.New("client should reconnect with extend support since the topic is upgraded to ext")
				goto exit
			}
			err = SendMessage(client, msg, extSupport && subChannel.IsExt(), &buf, subChannel.IsOrdered())
			if err != nil {
				goto exit
			}
			subChannel.CheckIfTimeoutToomuch(msg, msgTimeout)
			flushed = false
		}
	}

exit:
	if nsqd.NsqLogger().Level() > levellogger.LOG_DEBUG {
		nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] exiting messagePump", client)
	}
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
	close(stoppedChan)
}

func (p *protocolV2) IDENTIFY(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var err error

	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData nsqd.IdentifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] %+v", client, identifyData)

	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.GetTlsConfig() != nil && identifyData.TLSv1
	deflate := p.ctx.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(identifyData.DeflateLevel), float64(p.ctx.getOpts().MaxDeflateLevel)))
	}
	snappy := p.ctx.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
		DesiredTag          string `json:"desired_tag,omitempty"`
	}{
		MaxRdyCount:         p.ctx.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.GetMsgTimeout() / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.isAuthEnabled(),
		OutputBufferSize:    int(client.GetOutputBufferSize()),
		OutputBufferTimeout: int64(client.GetOutputBufferTimeout() / time.Millisecond),
		DesiredTag:          client.GetDesiredTag(),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocolV2) AUTH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.ctx.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "AUTH Already set")
	}

	if !p.ctx.isAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH Disabled")
	}

	if err = client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH No authorizations found")
	}

	var resp []byte
	resp, err = json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *nsqd.ClientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if p.ctx.isAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// params: [command topic channel partition consume_start]
func (p *protocolV2) SUBADVANCED(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var consumeStart *ConsumeOffset
	if len(params) > 4 && len(params[4]) > 0 {
		consumeStart = &ConsumeOffset{}
		err := consumeStart.FromBytes(params[4])
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
		}
	}
	return p.internalSUB(client, params, true, false, consumeStart)
}

//params: [command topic channel partition]
func (p *protocolV2) SUBORDERED(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalSUB(client, params, true, true, nil)
}

func (p *protocolV2) SUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalSUB(client, params, false, false, nil)
}

func (p *protocolV2) internalSUB(client *nsqd.ClientV2, params [][]byte, enableTrace bool,
	ordered bool, startFrom *ConsumeOffset) ([]byte, error) {

	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot SUB in current state")
	}

	if client.GetHeartbeatInterval() <= 0 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	partition := -1
	channelName := ""
	var err error
	channelName = string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if len(params) == 4 {
		partition, err = strconv.Atoi(string(params[3]))
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	if err = p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("sub to not existing topic: %v, err:%v", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}
	if topic.IsOrdered() && !ordered {
		return nil, protocol.NewFatalClientErr(nil, "E_SUB_ORDER_IS_MUST", "this topic is configured only allow ordered sub")
	}
	if topic.IsExt() {
		if !p.ctx.getOpts().AllowSubExtCompatible && !client.ExtendSupport() {
			nsqd.NsqLogger().Logf("sub failed on extend topic: %v-%v, %v", topicName, channelName, client.String())
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_EXTEND_NEED", "this topic is extended and should identify as extend support.")
		}
	} else {
		if client.ExtendSupport() {
			nsqd.NsqLogger().Logf("sub failed on non-extend topic: %v-%v, %v", topicName, channelName, client.String())
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_EXTEND_FORBIDDON", "this topic is not extended and should not identify as extend support.")
		}
	}
	if !p.ctx.checkConsumeForMasterWrite(topicName, partition) {
		nsqd.NsqLogger().Logf("sub failed on not leader: %v-%v, remote is : %v", topicName, partition, client.String())
		// we need disable topic here to trigger a notify, maybe we failed to notify lookup last time.
		topic.DisableForSlave(false)
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}
	var channel *nsqd.Channel
	if topic.IsChannelAutoCreateDisabled() {
		var err error
		channel, err = topic.GetExistingChannel(channelName)
		if err != nil {
			nsqd.NsqLogger().Logf("sub failed on not registered: %v-%v, channel: %s", topicName, partition, channelName)
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_CHANNEL_NOT_REGISTERED", "channel is not registered under topic.")
		}
	} else {
		channel = topic.GetChannel(channelName)
	}
	// need sync channel after created
	p.ctx.SyncChannels(topic)
	// client with tag is subscribe to topic not support tag, remove client's tag and treat it like untaged consumer
	if !topic.IsExt() && client.GetDesiredTag() != "" {
		nsqd.NsqLogger().Logf("[%v] IDENTIFY before subscribe has a tag %v to topic %v not support tag. Remove client's tag.", client, client.GetDesiredTag(), topicName)
		client.UnsetDesiredTag()
	}

	err = channel.AddClient(client.ID, client)
	if err != nil {
		nsqd.NsqLogger().Logf("sub failed to add client: %v, %v", client, err)
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotWritable, "")
	}

	//if client.Tag != nil {
	//	client.SetTagMsgChannel(channel.GetOrCreateClientMsgChannel(client.Tag))
	//}

	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	if enableTrace {
		nsqd.NsqLogger().Logf("sub channel %v with trace enabled, remote is : %v", channelName, client.String())
	}
	if ordered {
		if atomic.LoadInt32(&client.SampleRate) != 0 {
			nsqd.NsqLogger().Errorf("%v", ErrOrderChannelOnSampleRate)
			return nil, protocol.NewFatalClientErr(nil, E_INVALID, ErrOrderChannelOnSampleRate.Error())
		}
		channel.SetOrdered(true)
	} else {
		if !topic.IsOrdered() && channel.IsOrdered() {
			nsqd.NsqLogger().Infof("channel %v is in ordered state on non-order topic %v but with normal sub command, remote is : %v, should convert state to non-ordered with http api.",
				channelName, topicName, client.String())
		}
	}

	if startFrom != nil {
		cnt := channel.GetClientsCount()
		if cnt > 1 {
			nsqd.NsqLogger().LogDebugf("the consume offset: %v can only be set by the first client: %v", startFrom, cnt)
		} else {
			queueOffset, cnt, err := p.ctx.SetChannelOffset(channel, startFrom, false)
			if err != nil {
				return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
			}
			nsqd.NsqLogger().Logf("set the channel offset: %v (actual set : %v:%v), by client:%v, %v",
				startFrom, queueOffset, cnt, client.String(), client.UserAgent)
		}
	}
	client.EnableTrace = enableTrace
	// update message pump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *protocolV2) RDY(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		nsqd.NsqLogger().Logf(
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, E_INVALID,
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ctx.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, E_INVALID,
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *protocolV2) FIN(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot FIN in current state")
	}

	if len(params) < 2 {
		nsqd.NsqLogger().LogDebugf("FIN error params: %v", params)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "FIN insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		nsqd.NsqLogger().LogDebugf("FIN error: %v, %v", params[1], err)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}
	msgID := nsqd.GetMessageIDFromFullMsgID(*id)
	if int64(msgID) <= 0 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "Invalid Message ID")
	}

	if client.Channel == nil {
		nsqd.NsqLogger().LogDebugf("FIN error no channel: %v", msgID)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}

	if !p.ctx.checkConsumeForMasterWrite(client.Channel.GetTopicName(), client.Channel.GetTopicPart()) {
		nsqd.NsqLogger().Logf("topic %v fin message failed for not leader", client.Channel.GetTopicName())
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}

	err = p.ctx.FinishMessage(client.Channel, client.ID, client.String(), msgID)
	if err != nil {
		client.IncrSubError(int64(1))
		nsqd.NsqLogger().LogDebugf("FIN error : %v, err: %v, channel: %v, topic: %v", msgID,
			err, client.Channel.GetName(), client.Channel.GetTopicName())
		if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, protocol.NewFatalClientErr(err, FailedOnNotWritable, "")
			}
		}
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func (p *protocolV2) requeueToEnd(client *nsqd.ClientV2, oldMsg *nsqd.Message,
	timeoutDuration time.Duration) error {
	err := p.ctx.internalRequeueToEnd(client.Channel, oldMsg, timeoutDuration)
	if err != nil {
		nsqd.NsqLogger().LogWarningf("[%s] req channel %v(%v) failed: %v", client,
			client.Channel.GetName(), client.Channel.GetTopicName(), err)
		return err
	}
	return nil
}

func (p *protocolV2) REQ(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "REQ insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, E_INVALID,
			fmt.Sprintf("REQ could not parse timeout %s, %s", params[1], params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.ctx.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		nsqd.NsqLogger().Logf("[%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}
	if client.Channel == nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}
	// in the queue, we confirm the message as a fifo-alike queue,
	// Too much req messages in memory will block the queue read from disk until the requeued message confirmed.
	// To avoid block by req, we put some of the req messages to the end of queue of some conditions meet
	// 1. the req delay time is large than 10 mins (this delay means latency is trival)
	// 2. this message has been req for more than 10 times
	// 3. this message is blocking confirm queue for 10 mins
	// to avoid delivery the delayed message early than required, we
	// can update the inflight message to the new message put backed at the queue

	msgID := nsqd.GetMessageIDFromFullMsgID(*id)
	topic, _ := p.ctx.getExistingTopic(client.Channel.GetTopicName(), client.Channel.GetTopicPart())
	oldMsg, toEnd := client.Channel.ShouldRequeueToEnd(client.ID, client.String(),
		msgID, timeoutDuration, true)
	// the channel under non-order topic may also sub with ordered
	isOrderedCh := client.Channel.IsOrdered()
	if topic != nil && topic.IsOrdered() {
		isOrderedCh = true
	}
	if isOrderedCh {
		toEnd = false
		// for ordered topic, disable defer since it may block the consume
		if timeoutDuration > 0 {
			nsqd.NsqLogger().Logf("ignore delay for ordered topic: %v, %v, %v, %v",
				client, client.Channel.GetTopicName(), client.Channel.GetName(), timeoutDuration)
			return nil, nil
		}
	}
	if toEnd {
		err = p.requeueToEnd(client, oldMsg, timeoutDuration)
		if err != nil {
			// try to reduce timeout to requeue to memory if failed to requeue to end
			if timeoutDuration > p.ctx.getOpts().ReqToEndThreshold {
				timeoutDuration = p.ctx.getOpts().ReqToEndThreshold
			}
		}
	}
	if !toEnd || err != nil {
		err = client.Channel.RequeueMessage(client.ID, client.String(), msgID, timeoutDuration, true)
	}
	if err != nil {
		client.IncrSubError(int64(1))

		nsqd.NsqLogger().Logf("client %v req failed %v for topic: %v, %v, %v, %v",
			client, err.Error(), client.Channel.GetTopicName(), client.Channel.GetName(), msgID, timeoutDuration)
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func (p *protocolV2) CLS(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed {
		nsqd.NsqLogger().Logf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *protocolV2) NOP(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *protocolV2) internalCreateTopic(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "CREATE_TOPIC insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("topic name %q is not valid", topicName))
	}

	partition, err := strconv.Atoi(string(params[2]))
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
			fmt.Sprintf("topic partition is not valid: %v", err))
	}
	if err = p.CheckAuth(client, "CREATE_TOPIC", topicName, ""); err != nil {
		return nil, err
	}

	if partition < 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION", "partition should not less than 0")
	}

	var ext bool
	if len(params) >= 4 {
		ext, err = strconv.ParseBool(string(params[3]))
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_EXT",
				fmt.Sprintf("topic ext is not valid: %v", err))
		}
	}

	if p.ctx.nsqdCoord != nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC is not allowed here while cluster feature enabled."))
	}

	topic := p.ctx.getTopic(topicName, partition, ext, false)
	if topic == nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC %v failed", topicName))
	}
	return okBytes, nil
}

//if target topic is not configured as extendable and there is a tag, pub request should be stopped here
func (p *protocolV2) preparePub(client *nsqd.ClientV2, params [][]byte, maxBody int64, isMpub bool) (int32, *nsqd.Topic, error) {
	var err error

	if len(params) < 2 {
		return 0, nil, protocol.NewFatalClientErr(nil, E_INVALID, "insufficient number of parameters")
	}

	topicName := string(params[1])
	partition := -1
	if len(params) == 3 {
		partition, err = strconv.Atoi(string(params[2]))
		if err != nil {
			return 0, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	origPart := partition
	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return 0, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "failed to read body size")
	}

	if bodyLen <= 0 {
		return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > maxBody {
		nsqd.NsqLogger().Logf("topic: %v message body too large %v vs %v ", topicName, bodyLen, maxBody)
		if isMpub {
			return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
				fmt.Sprintf("body too big %d > %d", bodyLen, maxBody))
		} else {
			return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("message too big %d > %d", bodyLen, maxBody))
		}
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("not existing topic: %v-%v, err:%v", topicName, partition, err.Error())
		return bodyLen, nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}

	if origPart == -1 && topic.IsOrdered() {
		return 0, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
			fmt.Sprintf("topic partition is not valid for multi partition: %v", origPart))
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return bodyLen, nil, err
	}
	return bodyLen, topic, nil
}

// PUB TRACE data format
// 4 bytes length + 8bytes trace id + binary data
func (p *protocolV2) PUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, true)
}

func (p *protocolV2) PUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, false)
}

func (p *protocolV2) PUBEXT(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubExtAndTrace(client, params, true, false)
}

func (p *protocolV2) MPUBEXT(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBEXTAndTrace(client, params, true, false)
}

func (p *protocolV2) MPUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBEXTAndTrace(client, params, false, true)
}

func (p *protocolV2) MPUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBEXTAndTrace(client, params, false, false)
}

func getTracedReponse(id nsqd.MessageID, traceID uint64, offset nsqd.BackendOffset, rawSize int32) ([]byte, error) {
	// pub with trace will return OK+16BYTES ID+8bytes offset of the disk queue + 4bytes raw size of disk queue data.
	retLen := 2 + 16 + 8 + 4
	buf := make([]byte, retLen)
	copy(buf[:2], okBytes)
	pos := 2
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(id))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+nsqd.MsgTraceIDLength], uint64(traceID))
	pos += nsqd.MsgTraceIDLength
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(offset))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(rawSize))

	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().Logf("pub traced %v (%v, %v) response : %v", id, offset, rawSize, buf)
	}
	return buf, nil
}

func internalMPubAsync(topic *nsqd.Topic, msgs []*nsqd.Message) error {
	if topic.Exiting() {
		return nsqd.ErrExiting
	}
	info := &nsqd.MPubInfo{
		Done:     make(chan struct{}),
		Msgs:     msgs,
		StartPub: time.Now(),
	}

	select {
	case topic.GetMWaitChan() <- info:
	default:
		clientTimer := time.NewTimer(pubWaitTimeout)
		defer clientTimer.Stop()
		select {
		case topic.GetMWaitChan() <- info:
		case <-topic.QuitChan():
			nsqd.NsqLogger().Infof("topic %v put messages failed at exiting", topic.GetFullName())
			return nsqd.ErrExiting
		case <-clientTimer.C:
			nsqd.NsqLogger().Infof("topic %v put messages timeout ", topic.GetFullName())
			topic.IncrPubFailed()
			incrServerPubFailed()
			return ErrPubToWaitTimeout
		}
	}
	<-info.Done
	return info.Err
}

func internalPubAsync(clientTimer *time.Timer, msgBody []byte, topic *nsqd.Topic, extContent ext.IExtContent) error {
	if topic.Exiting() {
		return nsqd.ErrExiting
	}
	info := &nsqd.PubInfo{
		Done:       make(chan struct{}),
		MsgBody:    msgBody,
		ExtContent: extContent,
		StartPub:   time.Now(),
	}

	select {
	case topic.GetWaitChan() <- info:
	default:
		if clientTimer == nil {
			clientTimer = time.NewTimer(pubWaitTimeout)
		} else {
			if !clientTimer.Stop() {
				select {
				case <-clientTimer.C:
				default:
				}
			}
			clientTimer.Reset(pubWaitTimeout)
		}
		defer clientTimer.Stop()
		select {
		case topic.GetWaitChan() <- info:
		case <-topic.QuitChan():
			nsqd.NsqLogger().Infof("topic %v put messages failed at exiting", topic.GetFullName())
			return nsqd.ErrExiting
		case <-clientTimer.C:
			nsqd.NsqLogger().Infof("topic %v put messages timeout ", topic.GetFullName())
			topic.IncrPubFailed()
			incrServerPubFailed()
			return ErrPubToWaitTimeout
		}
	}
	<-info.Done
	return info.Err
}

func isPubExt(pubCmdName []byte) bool {
	return bytes.Equal(pubCmdName, []byte("PUB_EXT"))
}

func (p *protocolV2) internalPubAndTrace(client *nsqd.ClientV2, params [][]byte, traceEnable bool) ([]byte, error) {
	return p.internalPubExtAndTrace(client, params, false, traceEnable)
}

/**
pub ext or pub trace or pub, if pubExt is true, traceEnable is ignored.
*/
func (p *protocolV2) internalPubExtAndTrace(client *nsqd.ClientV2, params [][]byte, pubExt bool, traceEnable bool) ([]byte, error) {
	startPub := time.Now().UnixNano()
	asyncAction := shouldHandleAsync(params)
	// notice: any client.Reader will change the content for params since the params is returned by ReadSlice
	bodyLen, topic, err := p.preparePub(client, params, p.ctx.getOpts().MaxMsgSize, false)
	if err != nil {
		return nil, err
	}

	if traceEnable && bodyLen <= nsqd.MsgTraceIDLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with trace id enabled", bodyLen))
	}

	if pubExt && bodyLen <= nsqd.MsgJsonHeaderLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with ext json header enabled", bodyLen))
	}
	wb := topic.IncrPubWaitingBytes(int64(bodyLen))
	defer topic.IncrPubWaitingBytes(-1 * int64(bodyLen))
	if wb > p.ctx.getOpts().MaxPubWaitingSize ||
		(topic.IsWaitChanFull() && wb > p.ctx.getOpts().MaxPubWaitingSize/10) {
		nsqd.TopicPubRefusedByLimitedCnt.With(ps.Labels{
			"topic": topic.GetTopicName(),
		}).Inc()
		return nil, protocol.NewFatalClientErr(nil, "E_PUB_TOO_MUCH_WAITING", fmt.Sprintf("pub too much waiting in the queue: %d", wb))
	}

	messageBodyBuffer := topic.BufferPoolGet(int(bodyLen))
	defer topic.BufferPoolPut(messageBodyBuffer)

	topicName := topic.GetTopicName()
	_, err = io.CopyN(messageBodyBuffer, client.Reader, int64(bodyLen))
	if err != nil {
		nsqd.NsqLogger().Logf("topic: %v message body read error %v ", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "failed to read message body")
	}
	messageBody := messageBodyBuffer.Bytes()[:bodyLen]

	partition := topic.GetTopicPart()
	var extJsonLen uint16
	var traceID uint64
	var needTraceRsp bool
	var realBody []byte
	var extContent ext.IExtContent
	var jsonHeader nsqd.IJsonExt
	extContent = ext.NewNoExt()
	if traceEnable && !pubExt {
		traceID = binary.BigEndian.Uint64(messageBody[:nsqd.MsgTraceIDLength])
		needTraceRsp = true
		realBody = messageBody[nsqd.MsgTraceIDLength:]
	} else if pubExt {
		//read two byte header length
		extJsonLen = binary.BigEndian.Uint16(messageBody[:nsqd.MsgJsonHeaderLength])
		//check json length, make sure it does not exceed slice length
		if bodyLen <= nsqd.MsgJsonHeaderLength+int32(extJsonLen) {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
				fmt.Sprintf("invalid body size %d in ext json header content length", bodyLen))
		}
		extJsonBytes := messageBody[nsqd.MsgJsonHeaderLength : nsqd.MsgJsonHeaderLength+extJsonLen]
		jsonHeader, err = nsqd.NewJsonExt(extJsonBytes)
		if err != nil {
			return nil, protocol.NewClientErr(err, ext.E_INVALID_JSON_HEADER, "fail to parse json header:"+err.Error())
		}
		//parse traceID, if there is any
		traceIDStr, jerr := jsonHeader.GetString(ext.TRACE_ID_KEY)
		if jerr != nil && !nsqd.IsNotFoundJsonKey(jerr) {
			return nil, protocol.NewClientErr(nil, "INVALID_TRACE_ID", "passin trace id should be string")
		}
		if jerr == nil {
			traceID, err = strconv.ParseUint(traceIDStr, 10, 0)
			if err != nil {
				return nil, protocol.NewClientErr(err, "INVALID_TRACE_ID", "invalid trace id")
			}
			needTraceRsp = true
		}

		jhe := ext.NewJsonHeaderExt()
		jhe.SetJsonHeaderBytes(extJsonBytes)
		extContent = jhe
		realBody = messageBody[nsqd.MsgJsonHeaderLength+extJsonLen:]
	} else {
		realBody = messageBody
	}
	if needTraceRsp || atomic.LoadInt32(&topic.EnableTrace) == 1 {
		asyncAction = false
	}
	if !topic.IsExt() && extContent.ExtVersion() != ext.NO_EXT_VER {
		notOK := true
		if p.ctx.getOpts().AllowExtCompatible {
			canIgnoreExt := canIgnoreJsonHeader(topicName, jsonHeader)
			if canIgnoreExt {
				extContent = ext.NewNoExt()
				nsqd.NsqLogger().Debugf("ext content ignored in topic: %v", topicName)
				notOK = false
			}
		}
		if notOK {
			nsqd.NsqLogger().Infof("ext content not supported in topic: %v", topicName)
			return nil, protocol.NewClientErr(nil, ext.E_EXT_NOT_SUPPORT,
				fmt.Sprintf("ext content not supported in topic %v", topicName))
		}
	}
	id := nsqd.MessageID(0)
	offset := nsqd.BackendOffset(0)
	rawSize := int32(0)
	if asyncAction {
		err = internalPubAsync(nil, realBody, topic, extContent)
	} else {
		id, offset, rawSize, _, err = p.ctx.PutMessage(topic, realBody, extContent, traceID)
	}
	//p.ctx.setHealth(err)
	if err != nil {
		pstat := client.GetTcpPubStats(topic)
		if pstat != nil {
			pstat.IncrCounter(1, true)
		}
		if !asyncAction {
			// async will add failed counter in loop
			topic.IncrPubFailed()
			incrServerPubFailed()
		}
		nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v, from: %v", topic.GetFullName(), err, client.String())
		if !p.ctx.checkForMasterWrite(topicName, partition) {
			return nil, protocol.NewClientErr(err, FailedOnNotLeader, "")
		}
		if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
			}
		}
		return nil, protocol.NewClientErr(err, "E_PUB_FAILED", err.Error())
	}
	pstat := client.GetTcpPubStats(topic)
	if pstat != nil {
		pstat.IncrCounter(1, false)
	}
	cost := time.Now().UnixNano() - startPub
	topic.GetDetailStats().UpdateTopicMsgStats(int64(len(realBody)), cost/1000)

	if traceID != 0 || atomic.LoadInt32(&topic.EnableTrace) == 1 || nsqd.NsqLogger().Level() >= levellogger.LOG_DETAIL {
		nsqd.GetMsgTracer().TracePubClient(topic.GetTopicName(), topic.GetTopicPart(), traceID, id, offset, client.String())
	}
	if needTraceRsp {
		return getTracedReponse(id, traceID, offset, rawSize)
	}
	return okBytes, nil
}

func (p *protocolV2) internalMPUBEXTAndTrace(client *nsqd.ClientV2, params [][]byte, mpubExt bool, traceEnable bool) ([]byte, error) {
	startPub := time.Now().UnixNano()
	bodyLen, topic, preErr := p.preparePub(client, params, p.ctx.getOpts().MaxBodySize, true)
	if preErr != nil {
		return nil, preErr
	}
	wb := topic.IncrPubWaitingBytes(int64(bodyLen))
	defer topic.IncrPubWaitingBytes(-1 * int64(bodyLen))
	if wb > p.ctx.getOpts().MaxPubWaitingSize ||
		(topic.IsMWaitChanFull() && wb > p.ctx.getOpts().MaxPubWaitingSize/10) {
		nsqd.TopicPubRefusedByLimitedCnt.With(ps.Labels{
			"topic": topic.GetTopicName(),
		}).Inc()
		return nil, protocol.NewFatalClientErr(nil, "E_PUB_TOO_MUCH_WAITING", fmt.Sprintf("pub too much waiting in the queue: %d", wb))
	}

	messages, buffers, preErr := readMPUBEXT(client.Reader, client.LenSlice, topic,
		p.ctx.getOpts().MaxMsgSize, p.ctx.getOpts().MaxBodySize, traceEnable, mpubExt, p.ctx.getOpts().AllowExtCompatible)

	defer func() {
		for _, b := range buffers {
			topic.BufferPoolPut(b)
		}
	}()
	if preErr != nil {
		return nil, preErr
	}

	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	if !p.ctx.checkForMasterWrite(topicName, partition) {
		topic.IncrPubFailed()
		incrServerPubFailed()
		pstat := client.GetTcpPubStats(topic)
		if pstat != nil {
			pstat.IncrCounter(int64(len(messages)), true)
		}
		//forward to master of topic
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), client.String())
		return nil, protocol.NewClientErr(preErr, FailedOnNotLeader, "")
	}
	id := nsqd.MessageID(0)
	offset := nsqd.BackendOffset(0)
	rawSize := int32(0)
	var err error
	asyncAction := !traceEnable && (atomic.LoadInt32(&topic.EnableTrace) != 1)

	if asyncAction {
		err = internalMPubAsync(topic, messages)
	} else {
		id, offset, rawSize, err = p.ctx.PutMessages(topic, messages)
	}
	pstat := client.GetTcpPubStats(topic)
	if pstat != nil {
		pstat.IncrCounter(int64(len(messages)), err != nil)
	}
	if err != nil {
		if !asyncAction {
			topic.IncrPubFailed()
			incrServerPubFailed()
		}
		nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)

		if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
			}
		}
		return nil, protocol.NewClientErr(err, "E_MPUB_FAILED", err.Error())
	}
	cost := time.Now().UnixNano() - startPub
	topic.GetDetailStats().BatchUpdateTopicLatencyStats(cost/int64(time.Microsecond), int64(len(messages)))
	if !traceEnable {
		return okBytes, nil
	}
	return getTracedReponse(id, 0, offset, rawSize)
}

func (p *protocolV2) TOUCH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "TOUCH insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}

	msgTimeout := client.GetMsgTimeout()

	if client.Channel == nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}
	err = client.Channel.TouchMessage(client.ID, nsqd.GetMessageIDFromFullMsgID(*id), msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, topic *nsqd.Topic, maxMessageSize int64,
	maxBodySize int64, traceEnable bool) ([]*nsqd.Message, []*bytes.Buffer, error) {
	return readMPUBEXT(r, tmp, topic, maxMessageSize, maxBodySize, traceEnable, false, false)
}

func readMPUBEXT(r io.Reader, tmp []byte, topic *nsqd.Topic, maxMessageSize int64,
	maxBodySize int64, traceEnable bool, mpubExt bool, allowExtCompatible bool) ([]*nsqd.Message, []*bytes.Buffer, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5

	if numMessages <= 0 || int64(numMessages) > maxMessages {
		return nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*nsqd.Message, 0, numMessages)
	buffers := make([]*bytes.Buffer, 0, numMessages)
	topicName := topic.GetTopicName()
	topicExt := topic.IsExt()
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, buffers, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		b := topic.BufferPoolGet(int(messageSize))
		buffers = append(buffers, b)
		_, err = io.CopyN(b, r, int64(messageSize))
		if err != nil {
			return nil, buffers, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		//parse ext header or trace
		msgBody := b.Bytes()[:messageSize]

		traceID := uint64(0)
		var extJsonBytes []byte
		var realBody []byte
		var extJsonLen uint16
		var canIgnoreExt bool
		if traceEnable && !mpubExt {
			if messageSize <= nsqd.MsgTraceIDLength {
				return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
					fmt.Sprintf("MPUB invalid message(%d) body size %d for tracing", i, messageSize))
			}
			traceID = binary.BigEndian.Uint64(msgBody[:nsqd.MsgTraceIDLength])
			realBody = msgBody[nsqd.MsgTraceIDLength:]
		} else if mpubExt {
			//read two byte header length
			extJsonLen = binary.BigEndian.Uint16(msgBody[:nsqd.MsgJsonHeaderLength])
			//check json length, make sure it does not exceed slice length
			if messageSize <= nsqd.MsgJsonHeaderLength+int32(extJsonLen) {
				return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
					fmt.Sprintf("invalid body size %d in ext json header content length", messageSize))
			}
			extJsonBytes = msgBody[nsqd.MsgJsonHeaderLength : nsqd.MsgJsonHeaderLength+extJsonLen]
			//parse trace id, if there is a json ext
			if extJsonLen > 0 {
				if !topicExt && !allowExtCompatible {
					nsqd.NsqLogger().Infof("ext content not supported in topic: %v", topicName)
					return nil, buffers, protocol.NewClientErr(nil, ext.E_EXT_NOT_SUPPORT,
						fmt.Sprintf("ext content not supported in topic %v", topicName))
				}
				jsonHeader, jerr := nsqd.NewJsonExt(extJsonBytes)
				if jerr != nil {
					return nil, buffers, protocol.NewClientErr(jerr, ext.E_INVALID_JSON_HEADER, "fail to parse json header:"+jerr.Error())
				}

				//parse traceID, if there is any
				traceIDStr, jerr := jsonHeader.GetString(ext.TRACE_ID_KEY)
				if jerr != nil && !nsqd.IsNotFoundJsonKey(jerr) {
					return nil, buffers, protocol.NewClientErr(err, "INVALID_TRACE_ID", "passin trace id should be string")
				}
				if jerr == nil {
					traceID, err = strconv.ParseUint(traceIDStr, 10, 0)
					if err != nil {
						return nil, buffers, protocol.NewClientErr(err, "INVALID_TRACE_ID", "invalid trace id")
					}
				}
				//check compatibility when topic does not support ext
				if !topicExt {
					canIgnoreExt = canIgnoreJsonHeader(topicName, jsonHeader)
					if canIgnoreExt {
						nsqd.NsqLogger().Debugf("ext content ignored in topic: %v", topicName)
					} else {
						nsqd.NsqLogger().Infof("ext content not supported in topic: %v", topicName)
						return nil, buffers, protocol.NewClientErr(nil, ext.E_EXT_NOT_SUPPORT,
							fmt.Sprintf("ext content not supported in topic %v", topicName))
					}
				}

			}
			realBody = msgBody[nsqd.MsgJsonHeaderLength+extJsonLen:]
		} else {
			realBody = msgBody
		}

		var msg *nsqd.Message
		if mpubExt && extJsonLen > 0 && topicExt {
			msg = nsqd.NewMessageWithExt(0, realBody, ext.JSON_HEADER_EXT_VER, extJsonBytes)
		} else {
			msg = nsqd.NewMessage(0, realBody)
		}
		msg.TraceID = traceID
		messages = append(messages, msg)
		topic.GetDetailStats().UpdateTopicMsgStats(int64(len(realBody)), 0)
	}

	return messages, buffers, nil
}

//return true when there are only preserved kv in json header, and false otherwise
func canIgnoreJsonHeader(topicName string, jsonHeader nsqd.IJsonExt) bool {
	canIgnoreExt := true
	if jsonHeader != nil {
		// if only internal header, we can ignore
		jsonHeader.KeysCheck(func(k string) bool {
			// for future, if any internal header can not be ignored, we should check here
			if k == ext.ZAN_TEST_KEY {
				nsqd.NsqLogger().Debugf("illegal zan test header ignored in topic: %v", topicName)
				return true
			}
			if !strings.HasPrefix(k, "##") {
				canIgnoreExt = false
				nsqd.NsqLogger().Debugf("custom ext content can not be ignored in topic: %v, %v", topicName, k)
				// stop early
				return false
			}
			return true
		})
	}
	return canIgnoreExt
}

// validate and cast the bytes on the wire to a message ID
func getFullMessageID(p []byte) (*nsqd.FullMessageID, error) {
	if len(p) != nsqd.MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*nsqd.FullMessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *nsqd.ClientV2, p *protocolV2, command []byte) error {
	if p.ctx.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, E_INVALID,
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
