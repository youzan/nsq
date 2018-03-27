package nsq_sync

import (
	"github.com/youzan/nsq/internal/http_api"
	"net"
	"os"
	"github.com/youzan/nsq/internal/protocol"
	"sync"
	"github.com/youzan/nsq/internal/util"
)

type NsqSync struct {
	sync.RWMutex
	opts         *Options
	TcpListener net.Listener
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
}


func NewNsqSync(opts *Options) *NsqSync {
	n := &NsqSync{
		opts: opts,
	}
	return n
}

func getIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		nsqSyncLog.Logf("found interface: %s", inter.Name)
		if inter.Name == ifname {
			if addrs, err := inter.Addrs(); err == nil {
				for _, addr := range addrs {
					switch ip := addr.(type) {
					case *net.IPNet:
						if ip.IP.DefaultMask() != nil {
							return ip.IP.String()
						}
					}
				}
			}
		}
	}
	return ""
}

func (l *NsqSync) Main() {
	ctx := &context{
		nsqsync: l,
	}
	var err error

	ctx.tcpAddr, err = net.ResolveTCPAddr("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy TCP address %v", l.opts.TCPAddress)
		return;
	}
	nsqSyncLog.Infof("nsq proxy TCP address: %v", ctx.tcpAddr.String())

	ctx.peerTcpAddr, err = net.ResolveTCPAddr("tcp", l.opts.PeerNSQSyncTCPAddr)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy forward TCP address %v", l.opts.PeerNSQSyncTCPAddr)
		return;
	}
	nsqSyncLog.Infof("nsq proxy forward TCP address: %v", ctx.peerTcpAddr.String())

	ctx.httpAddr, err = net.ResolveTCPAddr("tcp", l.opts.HTTPAddress)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse nsqproxy HTTP address %v", l.opts.HTTPAddress)
		return;
	}
	nsqSyncLog.Infof("nsq proxy HTTP address: %v", ctx.httpAddr.String())

	ctx.peerHttpAddr, err = net.ResolveTCPAddr("tcp", l.opts.PeerNSQSyncHTTPAddr)
	if err != nil {
		nsqSyncLog.Errorf("fail to parse forward nsqproxy TCP address %v", l.opts.PeerNSQSyncHTTPAddr)
		return;
	}
	nsqSyncLog.Infof("peer nsq proxy HTTP address: %v", ctx.peerHttpAddr.String())

	//TODO forward tcp server
	TcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqSyncLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.TcpListener = TcpListener
	ctx.tcpAddr = TcpListener.Addr().(*net.TCPAddr)
	l.Unlock()
	nsqSyncLog.Logf("TCP: listening on %s", TcpListener.Addr())
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(forwardTcpListener, tcpServer)
		nsqSyncLog.Logf("TCP: closing %s", forwardTcpListener.Addr())
	})

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqSyncLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	ctx.tcpAddr = tcpListener.Addr().(*net.TCPAddr)
	l.Unlock()
	nsqSyncLog.Logf("TCP: listening on %s", tcpListener.Addr())
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer)
		nsqSyncLog.Logf("TCP: closing %s", tcpListener.Addr())
	})



	//HTTP server
	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	ctx.httpAddr = httpListener.Addr().(*net.TCPAddr)
	if err != nil {
		nsqSyncLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx, false, l.opts.TLSRequired == nsqd.TLSRequired)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})

	//TODO: listen forward(HTTPS)
}

func (l *NsqSync) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.tcpListener == nil || l.tcpListener.Addr() == nil {
		return nil
	}
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NsqSync) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.httpListener == nil || l.httpListener.Addr() == nil {
		return nil
	}
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NsqSync) Exit() {
	nsqSyncLog.Logf("nsqproxy server stopping.")

	if l.tcpListener != nil {
		l.tcpListener.Close()
	}
	if l.forwardTcpListener != nil {
		l.forwardTcpListener.Close()
	}
	if l.httpListener != nil {
		l.httpListener.Close()
	}

	l.waitGroup.Wait()
	nsqSyncLog.Logf("nsqproxy stopped.")
}

func (l *NsqSync) IsAuthEnabled() bool {
	return len(l.opts.AuthHTTPAddresses) != 0
}