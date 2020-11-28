package consistence

import (
	"sync"
	"time"

	"github.com/absolute8511/gorpc"
)

type INsqlookupRemoteProxy interface {
	RemoteAddr() string
	Reconnect() error
	Close()
	RequestJoinCatchup(topic string, partition int, nid string) *CoordErr
	RequestJoinTopicISR(topic string, partition int, nid string) *CoordErr
	ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, joinISRSession string) *CoordErr
	RequestLeaveFromISR(topic string, partition int, nid string) *CoordErr
	RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr
	RequestNotifyNewTopicInfo(topic string, partition int, nid string)
	RequestCheckTopicConsistence(topic string, partition int)
}

type nsqlookupRemoteProxyCreateFunc func(string, time.Duration) (INsqlookupRemoteProxy, error)

var lookupClientDispatcher *gorpc.Dispatcher

func init() {
	lookupClientDispatcher = gorpc.NewDispatcher()
	lookupClientDispatcher.AddService("NsqLookupCoordRpcServer", &NsqLookupCoordRpcServer{})
}

type NsqLookupRpcClient struct {
	sync.Mutex
	remote  string
	timeout time.Duration
	c       *gorpc.Client
	dc      *gorpc.DispatcherClient
}

func NewNsqLookupRpcClient(addr string, timeout time.Duration) (INsqlookupRemoteProxy, error) {
	c := gorpc.NewTCPClient(addr)
	c.RequestTimeout = timeout
	c.DisableCompression = true
	c.Start()

	return &NsqLookupRpcClient{
		remote:  addr,
		timeout: timeout,
		c:       c,
		dc:      lookupClientDispatcher.NewServiceClient("NsqLookupCoordRpcServer", c),
	}, nil
}

func (nlrpc *NsqLookupRpcClient) Close() {
	nlrpc.Lock()
	if nlrpc.c != nil {
		nlrpc.c.Stop()
		nlrpc.c = nil
	}
	nlrpc.Unlock()
}

func (nlrpc *NsqLookupRpcClient) ShouldRemoved() bool {
	r := false
	nlrpc.Lock()
	if nlrpc.c != nil {
		r = nlrpc.c.ShouldRemoved()
	}
	nlrpc.Unlock()
	return r
}

func (nlrpc *NsqLookupRpcClient) RemoteAddr() string {
	return nlrpc.remote
}

func (nlrpc *NsqLookupRpcClient) Reconnect() error {
	nlrpc.Lock()
	if nlrpc.c != nil {
		nlrpc.c.Stop()
	}
	nlrpc.c = gorpc.NewTCPClient(nlrpc.remote)
	nlrpc.c.RequestTimeout = nlrpc.timeout
	nlrpc.c.DisableCompression = true
	nlrpc.c.Start()
	nlrpc.dc = lookupClientDispatcher.NewServiceClient("NsqLookupCoordRpcServer", nlrpc.c)
	nlrpc.Unlock()
	return nil
}

func (nlrpc *NsqLookupRpcClient) CallFast(method string, arg interface{}) (interface{}, error) {
	reply, err := nlrpc.dc.CallTimeout(method, arg, time.Millisecond*5)
	return reply, err
}

func (nlrpc *NsqLookupRpcClient) CallWithRetry(method string, arg interface{}) (interface{}, error) {
	retry := 0
	var err error
	var reply interface{}
	for retry < 5 {
		retry++
		reply, err = nlrpc.dc.Call(method, arg)
		if err != nil {
			cerr, ok := err.(*gorpc.ClientError)
			if (ok && cerr.Connection) || nlrpc.ShouldRemoved() {
				coordLog.Infof("rpc connection closed, error: %v", err)
				connErr := nlrpc.Reconnect()
				if connErr != nil {
					return reply, err
				}
			} else {
				return reply, err
			}
		} else {
			if err != nil {
				coordLog.Infof("rpc call %v error: %v", method, err)
			}
			return reply, err
		}
	}
	return reply, err
}

func (nlrpc *NsqLookupRpcClient) RequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	var req RpcReqJoinCatchup
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	ret, err := nlrpc.CallWithRetry("RequestJoinCatchup", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) RequestNotifyNewTopicInfo(topic string, partition int, nid string) {
	var req RpcReqNewTopicInfo
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	nlrpc.CallWithRetry("RequestNotifyNewTopicInfo", &req)
}

func (nlrpc *NsqLookupRpcClient) RequestJoinTopicISR(topic string, partition int, nid string) *CoordErr {
	var req RpcReqJoinISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	ret, err := nlrpc.CallWithRetry("RequestJoinTopicISR", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, joinISRSession string) *CoordErr {
	var req RpcReadyForISR
	req.NodeID = nid
	if leaderSession != nil {
		req.LeaderSession = *leaderSession
	}
	req.JoinISRSession = joinISRSession
	req.TopicName = topic
	req.TopicPartition = partition
	ret, err := nlrpc.CallWithRetry("ReadyForTopicISR", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) RequestLeaveFromISR(topic string, partition int, nid string) *CoordErr {
	var req RpcReqLeaveFromISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	ret, err := nlrpc.CallWithRetry("RequestLeaveFromISR", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) RequestLeaveFromISRFast(topic string, partition int, nid string) *CoordErr {
	var req RpcReqLeaveFromISR
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	ret, err := nlrpc.CallFast("RequestLeaveFromISR", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr {
	var req RpcReqLeaveFromISRByLeader
	req.NodeID = nid
	req.TopicName = topic
	req.TopicPartition = partition
	req.LeaderSession = *leaderSession
	ret, err := nlrpc.CallWithRetry("RequestLeaveFromISRByLeader", &req)
	return convertRpcError(err, ret)
}

func (nlrpc *NsqLookupRpcClient) RequestCheckTopicConsistence(topic string, partition int) {
	var req RpcReqCheckTopic
	req.TopicName = topic
	req.TopicPartition = partition
	nlrpc.CallWithRetry("RequestCheckTopicConsistence", &req)
}
