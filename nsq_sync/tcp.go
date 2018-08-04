package nsq_sync

import (
	"net"
	"io"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"time"
)

type tcpServer struct {
	ctx *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	clientConn.SetReadDeadline(time.Now().Add(p.ctx.getOpts().ReadTimeout))
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		nsqSyncLog.Logf(" failed to read protocol version - %s from client: %v", err, clientConn.RemoteAddr())
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)

	if nsqSyncLog.Level() >= levellogger.LOG_DEBUG {
		nsqSyncLog.LogDebugf("new CLIENT(%s): desired protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
	}

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolSync{ctx: p.ctx}
	default:
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		nsqSyncLog.LogErrorf("client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		nsqSyncLog.Logf("client(%s) error - %s", clientConn.RemoteAddr(), err)
		return
	}
}
