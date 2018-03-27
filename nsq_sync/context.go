package nsq_sync

import (
	"crypto/tls"
	"net"
	"sync/atomic"
)

type context struct {
	clientIDSequence int64
	nsqsync             *NsqSync
	tlsConfig        *tls.Config
	tlsEnable	bool
	tlsRequired	bool
	reverseProxyPort string

	tcpAddr 	*net.TCPAddr
	httpAddr 	*net.TCPAddr
	peerHttpAddr *net.TCPAddr
	peerTcpAddr  *net.TCPAddr
}

func (c *context) getOpts() *Options {
	return c.nsqsync.opts
}

func (c *context) GetTlsConfig() *tls.Config {
	return c.tlsConfig
}

func (c *context) nextClientID() int64 {
	return atomic.AddInt64(&c.clientIDSequence, 1)
}

func (c *context) realHTTPAddr() *net.TCPAddr {
	return c.httpAddr
}

func (c *context) realTCPAddr() *net.TCPAddr {
	return c.tcpAddr
}