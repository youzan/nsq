package nsq_sync

import (
	"bytes"
	"github.com/youzan/nsq/internal/protocol"
	"fmt"
	"net"
	"io"
	"encoding/binary"
	"github.com/youzan/nsq/internal/levellogger"
	"errors"
	"time"
)

const (
	E_INVALID         = "E_INVALID"
	E_TOPIC_NOT_EXIST = "E_TOPIC_NOT_EXIST"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)


var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")
var offsetSplitStr = ":"
var offsetSplitBytes = []byte(offsetSplitStr)

var (
	ErrOrderChannelOnSampleRate = errors.New("order consume is not allowed while sample rate is not 0")
	ErrPubToWaitTimeout         = errors.New("pub to wait channel timeout")
)

type protocolSync struct {
	ctx *context
}

//not heart beat as conenction healthy check is delegated to producer manager
//func (p *protocolProxyForward) heartbeatLoopPump(client *ProxyClient, startedChan chan bool, stoppedChan chan bool, heartbeatStopChan chan bool)

func (p *protocolSync) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time
	left := make([]byte, 100)
	tmpLine := make([]byte, 100)
	clientID := p.ctx.nextClientID()
	client := NewProxyClient(clientID, conn, p.ctx.getOpts(), p.ctx.GetTlsConfig())
	client.SetWriteDeadline(zeroTime)

	startedChan := make(chan bool)
	stoppedChan := make(chan bool)
	heartbeatStoppedChan := make(chan bool)
	go p.heartbeatLoopPump(client, startedChan, stoppedChan, heartbeatStoppedChan)
	<-startedChan

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
					//TODO client.Exit()
				}
			}
			break
		}

		if nsqproxyLog.Level() > levellogger.LOG_DETAIL {
			nsqproxyLog.Logf("PROTOCOL(V2) got client command: %v ", line)
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
					left = left[:20-len(line)]
					nr := 0
					nr, err = io.ReadFull(client.Reader, left)
					if err != nil {
						nsqproxyLog.LogErrorf("read param err:%v", err)
					}
					line = append(line, left[:nr]...)
					tmpLine = tmpLine[:len(line)]
					copy(tmpLine, line)
					// the readslice will overwrite the slice line,
					// so we should copy it and copy back.
					extra, extraErr := client.Reader.ReadSlice('\n')
					tmpLine = append(tmpLine, extra...)
					line = append(line[:0], tmpLine...)
					if extraErr != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", extraErr)
					}
				}
				params = append(params, line[:3])
				if len(line) >= 21 {
					params = append(params, line[4:20])
					// it must be REQ
					if bytes.Equal(line[:3], []byte("REQ")) {
						if len(line) >= 22 {
							params = append(params, line[21:len(line)-1])
						}
					} else {
						params = append(params, line[20:])
					}
				} else {
					params = append(params, []byte(""))
				}

			} else if len(line) >= 5 {
				if bytes.Equal(line[:5], []byte("TOUCH")) {
					isSpecial = true
					if len(line) < 23 {
						left = left[:23-len(line)]
						nr := 0
						nr, err = io.ReadFull(client.Reader, left)
						if err != nil {
							nsqproxyLog.Logf("TOUCH param err:%v", err)
						}
						line = append(line, left[:nr]...)
					}
					params = append(params, line[:5])
					if len(line) >= 23 {
						params = append(params, line[6:22])
					} else {
						params = append(params, []byte(""))
					}
				}
			}
		}
		if p.ctx.getOpts().Verbose || nsqproxyLog.Level() > levellogger.LOG_DETAIL {
			nsqproxyLog.Logf("PROTOCOL(V2) got client command: %v ", line)
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

		if p.ctx.getOpts().Verbose || nsqproxyLog.Level() > levellogger.LOG_DETAIL {
			nsqproxyLog.Logf("PROTOCOL(V2): [%s] %v, %v", client, string(params[0]), params)
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
	if nsqproxyLog.Level() >= levellogger.LOG_DEBUG {
		nsqproxyLog.LogDebugf("PROTOCOL(V2): client [%s] exiting ioloop", client)
	}
	close(client.ExitChan)
	//p.ctx.nsqd.CleanClientPubStats(client.String(), "tcp")
	<-stoppedChan

	if nsqproxyLog.Level() >= levellogger.LOG_DEBUG {
		nsqproxyLog.Logf("msg pump stopped client %v", client)
	}

	//if client.Channel != nil {
	//	client.Channel.RequeueClientMessages(client.ID, client.String())
	//	client.Channel.RemoveClient(client.ID, client.GetDesiredTag())
	//}
	client.FinalClose()

	return err
}

func handleRequestReponseForClient(client *ProxyClient, response []byte, err error) error {
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
			nsqd.NsqLogger().LogErrorf("Send response error: [%s] - %s%s", client, sendErr, ctx)
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

/**
TODO: AUTH handle authentication request from peer nsqproxy, and use auth server in configuration for authentication.
Authentication server configured need to be the same one configured in forward nsqd.
 */


func (p *protocolSync) Exec(client *ProxyClient, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	//err := enforceTLSPolicy(client, p, params[0])
	//if err != nil {
	//	return nil, err
	//}
	switch {
	//case bytes.Equal(params[0], []byte("FIN")):
	//	return p.FIN(client, params)
	//case bytes.Equal(params[0], []byte("RDY")):
	//	return p.RDY(client, params)
	//case bytes.Equal(params[0], []byte("REQ")):
	//	return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	//case bytes.Equal(params[0], []byte("PUB_TRACE")):
	//	return p.PUBTRACE(client, params)
	//case bytes.Equal(params[0], []byte("PUB_EXT")):
	//	return p.PUBEXT(client, params)
	//case bytes.Equal(params[0], []byte("MPUB")):
	//	return p.MPUB(client, params)
	//case bytes.Equal(params[0], []byte("MPUB_TRACE")):
	//	return p.MPUBTRACE(client, params)
	//case bytes.Equal(params[0], []byte("NOP")):
	//	return p.NOP(client, params)
	//case bytes.Equal(params[0], []byte("TOUCH")):
	//	return p.TOUCH(client, params)
	//case bytes.Equal(params[0], []byte("SUB_ADVANCED")):
	//	return p.SUBADVANCED(client, params)
	//case bytes.Equal(params[0], []byte("SUB_ORDERED")):
	//	return p.SUBORDERED(client, params)
	//case bytes.Equal(params[0], []byte("CLS")):
	//	return p.CLS(client, params)
	//case bytes.Equal(params[0], []byte("AUTH")):
	//	return p.AUTH(client, params)
	//case bytes.Equal(params[0], []byte("INTERNAL_CREATE_TOPIC")):
	//	return p.internalCreateTopic(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %v", params))
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func Send(client *ProxyClient, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, false)
}

func internalSend(client *ProxyClient, frameType int32, data []byte, needFlush bool) error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()
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