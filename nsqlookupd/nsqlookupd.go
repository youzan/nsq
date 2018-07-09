package nsqlookupd

import (
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/youzan/nsq/consistence"

	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/util"
	"github.com/youzan/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
	coordinator  *consistence.NsqLookupCoordinator
}

func New(opts *Options) *NSQLookupd {
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	return n
}

func getIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		nsqlookupLog.Logf("found interface: %s", inter.Name)
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

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	nsqlookupLog.Logf(version.String("nsqlookupd"))
	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqlookupLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	l.Unlock()
	nsqlookupLog.Logf("TCP: listening on %s", tcpListener.Addr())
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer)
		nsqlookupLog.Logf("TCP: closing %s", tcpListener.Addr())
	})

	var node consistence.NsqLookupdNodeInfo
	_, node.HttpPort, _ = net.SplitHostPort(l.opts.HTTPAddress)
	if l.opts.ReverseProxyPort != "" {
		node.HttpPort = l.opts.ReverseProxyPort
	}
	node.NodeIP, node.TcpPort, _ = net.SplitHostPort(l.opts.TCPAddress)
	if l.opts.RPCPort != "" {
		nsqlookupLog.Logf("broadcast option: %s, %s", l.opts.BroadcastAddress, l.opts.BroadcastInterface)
		if l.opts.BroadcastInterface != "" {
			node.NodeIP = getIPv4ForInterfaceName(l.opts.BroadcastInterface)
		}
		if node.NodeIP == "" {
			node.NodeIP = l.opts.BroadcastAddress
		} else {
			l.opts.BroadcastAddress = node.NodeIP
		}
		if node.NodeIP == "0.0.0.0" || node.NodeIP == "" {
			nsqlookupLog.LogErrorf("can not decide the broadcast ip: %v", node.NodeIP)
			os.Exit(1)
		}
		nsqlookupLog.Logf("Start with broadcast ip:%s", node.NodeIP)
		node.RpcPort = l.opts.RPCPort
		node.ID = consistence.GenNsqLookupNodeID(&node, "nsqlookup")

		nsqlookupLog.Logf("balance interval is: %v", l.opts.BalanceInterval)
		coordOpts := &consistence.Options{}

		if len(l.opts.BalanceInterval) == 2 {
			coordOpts.BalanceStart, err = strconv.Atoi(l.opts.BalanceInterval[0])
			if err != nil {
				nsqlookupLog.LogErrorf("invalid balance interval: %v", err)
				os.Exit(1)
			}
			coordOpts.BalanceEnd, err = strconv.Atoi(l.opts.BalanceInterval[1])
			if err != nil {
				nsqlookupLog.LogErrorf("invalid balance interval: %v", err)
				os.Exit(1)
			}
		}

		l.Lock()
		consistence.SetCoordLogger(l.opts.Logger, l.opts.LogLevel)
		l.coordinator = consistence.NewNsqLookupCoordinator(l.opts.ClusterID, &node, coordOpts)
		l.Unlock()
		// set etcd leader manager here
		leadership, err := consistence.NewNsqLookupdEtcdMgr(l.opts.ClusterLeadershipAddresses)
		if err != nil {
			nsqlookupLog.LogErrorf("FATAL: start coordinator failed - %s", err)
			os.Exit(1)
		}
		l.coordinator.SetLeadershipMgr(leadership)
		err = l.coordinator.Start()
		if err != nil {
			nsqlookupLog.LogErrorf("FATAL: start coordinator failed - %s", err)
			os.Exit(1)
		}
	} else {
		nsqlookupLog.Logf("lookup start without the coordinator enabled.")
		l.Lock()
		l.coordinator = nil
		l.Unlock()
	}

	// wait coordinator ready
	time.Sleep(time.Millisecond * 500)
	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		nsqlookupLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.tcpListener == nil || l.tcpListener.Addr() == nil {
		return nil
	}
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	if l.httpListener == nil || l.httpListener.Addr() == nil {
		return nil
	}
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}
	if l.coordinator != nil {
		l.coordinator.Stop()
	}
	if l.httpListener != nil {
		l.httpListener.Close()
	}

	l.waitGroup.Wait()
	nsqlookupLog.Logf("lookup stopped.")
}
