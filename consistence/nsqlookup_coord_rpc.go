package consistence

import (
	"net"
	"time"

	"github.com/absolute8511/gorpc"
)

type RpcLookupReqBase struct {
	TopicName      string
	TopicPartition int
	NodeID         string
}

type RpcReqJoinCatchup struct {
	RpcLookupReqBase
}

type RpcReqJoinISR struct {
	RpcLookupReqBase
}

type RpcReqCheckTopic struct {
	RpcLookupReqBase
}

type RpcReqNewTopicInfo struct {
	RpcLookupReqBase
}

type RpcReadyForISR struct {
	RpcLookupReqBase
	LeaderSession  TopicLeaderSession
	JoinISRSession string
}

type RpcRspJoinISR struct {
	CoordErr
	JoinISRSession string
}

type RpcReqLeaveFromISR struct {
	RpcLookupReqBase
}

type RpcReqLeaveFromISRByLeader struct {
	RpcLookupReqBase
	LeaderSession TopicLeaderSession
}

type NsqLookupCoordRpcServer struct {
	nsqLookupCoord *NsqLookupCoordinator
	rpcDispatcher  *gorpc.Dispatcher
	rpcServer      *gorpc.Server
	lastNotify     time.Time
}

func NewNsqLookupCoordRpcServer(coord *NsqLookupCoordinator) *NsqLookupCoordRpcServer {
	return &NsqLookupCoordRpcServer{
		nsqLookupCoord: coord,
		rpcDispatcher:  gorpc.NewDispatcher(),
		lastNotify:     time.Now(),
	}
}

func (nlcoord *NsqLookupCoordRpcServer) start(ip, port string) error {
	nlcoord.rpcDispatcher.AddService("NsqLookupCoordRpcServer", nlcoord)
	nlcoord.rpcServer = gorpc.NewTCPServer(net.JoinHostPort(ip, port), nlcoord.rpcDispatcher.NewHandlerFunc())
	e := nlcoord.rpcServer.Start()
	if e != nil {
		coordLog.Errorf("listen rpc error : %v", e)
		panic(e)
	}

	coordLog.Infof("nsqlookup coordinator rpc listen at : %v", nlcoord.rpcServer.Listener.ListenAddr())
	return nil
}

func (nlcoord *NsqLookupCoordRpcServer) stop() {
	if nlcoord.rpcServer != nil {
		nlcoord.rpcServer.Stop()
	}
}

func (nlcoord *NsqLookupCoordRpcServer) RequestJoinCatchup(req *RpcReqJoinCatchup) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()
	var ret CoordErr
	err := nlcoord.nsqLookupCoord.handleRequestJoinCatchup(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (nlcoord *NsqLookupCoordRpcServer) RequestJoinTopicISR(req *RpcReqJoinISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := nlcoord.nsqLookupCoord.handleRequestJoinISR(req.TopicName, req.TopicPartition, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (nlcoord *NsqLookupCoordRpcServer) ReadyForTopicISR(req *RpcReadyForISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := nlcoord.nsqLookupCoord.handleReadyForISR(req.TopicName, req.TopicPartition, req.NodeID, req.LeaderSession, req.JoinISRSession)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (nlcoord *NsqLookupCoordRpcServer) RequestLeaveFromISR(req *RpcReqLeaveFromISR) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := nlcoord.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, nil, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (nlcoord *NsqLookupCoordRpcServer) RequestLeaveFromISRByLeader(req *RpcReqLeaveFromISRByLeader) *CoordErr {
	s := time.Now().Unix()
	defer func() {
		e := time.Now().Unix()
		if e-s > int64(RPC_TIMEOUT/2) {
			coordLog.Infof("rpc call used: %v", e-s)
		}
	}()

	var ret CoordErr
	err := nlcoord.nsqLookupCoord.handleLeaveFromISR(req.TopicName, req.TopicPartition, &req.LeaderSession, req.NodeID)
	if err != nil {
		ret = *err
		return &ret
	}
	return &ret
}

func (nlcoord *NsqLookupCoordRpcServer) RequestNotifyNewTopicInfo(req *RpcReqNewTopicInfo) *CoordErr {
	var coordErr CoordErr
	if time.Since(nlcoord.lastNotify) < time.Millisecond*10 {
		return &coordErr
	}
	nlcoord.nsqLookupCoord.handleRequestNewTopicInfo(req.TopicName, req.TopicPartition, req.NodeID)
	return &coordErr
}

func (nlcoord *NsqLookupCoordRpcServer) RequestCheckTopicConsistence(req *RpcReqCheckTopic) *CoordErr {
	var coordErr CoordErr
	nlcoord.nsqLookupCoord.handleRequestCheckTopicConsistence(req.TopicName, req.TopicPartition)
	return &coordErr
}
