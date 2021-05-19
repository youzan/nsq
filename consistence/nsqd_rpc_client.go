package consistence

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/absolute8511/gorpc"
	pb "github.com/youzan/nsq/consistence/coordgrpc"
	"github.com/youzan/nsq/nsqd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var testRPCTimeoutAndWait bool

const (
	RPC_TIMEOUT            = time.Duration(time.Second * 5)
	RPC_TIMEOUT_SHORT      = time.Duration(time.Second * 3)
	RPC_TIMEOUT_FOR_LOOKUP = time.Duration(time.Second * 3)
)

var nsqdClientDispatcher *gorpc.Dispatcher

func init() {
	nsqdClientDispatcher = gorpc.NewDispatcher()
	nsqdClientDispatcher.AddService("NsqdCoordRpcServer", &NsqdCoordRpcServer{})
}

type NsqdRpcClient struct {
	sync.Mutex
	remote     string
	timeout    time.Duration
	c          *gorpc.Client
	dc         *gorpc.DispatcherClient
	grpcClient pb.NsqdCoordRpcV2Client
	grpcConn   *grpc.ClientConn
}

func convertRpcError(err error, errInterface interface{}) *CoordErr {
	if err != nil {
		return NewCoordErr(err.Error(), CoordNetErr)
	}
	if errInterface == nil {
		return nil
	}
	coordErr, ok := errInterface.(*CoordErr)
	if ok {
		if coordErr != nil && coordErr.HasError() {
			return coordErr
		}
	} else if pbErr, ok := errInterface.(*pb.CoordErr); ok {
		if pbErr != nil && (pbErr.ErrType != 0 || pbErr.ErrCode != 0) {
			return &CoordErr{
				ErrMsg:  pbErr.ErrMsg,
				ErrCode: ErrRPCRetCode(pbErr.ErrCode),
				ErrType: CoordErrType(pbErr.ErrType),
			}
		}
	} else {
		return NewCoordErr("Not an Invalid CoordErr", CoordCommonErr)
	}
	return nil
}

func NewNsqdRpcClient(addr string, timeout time.Duration) (*NsqdRpcClient, error) {
	c := gorpc.NewTCPClient(addr)
	c.RequestTimeout = timeout
	c.DisableCompression = true
	c.Start()
	var grpcClient pb.NsqdCoordRpcV2Client
	//ip, port, _ := net.SplitHostPort(addr)
	//portNum, _ := strconv.Atoi(port)
	//grpcAddr := ip + ":" + strconv.Itoa(portNum+1)
	//grpcConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	//if err != nil {
	//	coordLog.Warningf("failed to connect to grpc server %v: %v", grpcAddr, err)
	//	grpcClient = nil
	//	grpcConn = nil
	//} else {
	//	grpcClient = pb.NewNsqdCoordRpcV2Client(grpcConn)
	//}
	coordLog.Infof("connected to rpc server %v", addr)

	return &NsqdRpcClient{
		remote:     addr,
		timeout:    timeout,
		c:          c,
		dc:         nsqdClientDispatcher.NewServiceClient("NsqdCoordRpcServer", c),
		grpcClient: grpcClient,
		grpcConn:   nil,
	}, nil
}

func (nrpc *NsqdRpcClient) Close() {
	nrpc.Lock()
	if nrpc.c != nil {
		nrpc.c.Stop()
		nrpc.c = nil
	}
	if nrpc.grpcConn != nil {
		nrpc.grpcConn.Close()
		nrpc.grpcConn = nil
	}
	nrpc.Unlock()
}

func (nrpc *NsqdRpcClient) ShouldRemoved() bool {
	r := true
	nrpc.Lock()
	if nrpc.c != nil {
		r = nrpc.c.ShouldRemoved()
	}
	nrpc.Unlock()
	return r
}

func (nrpc *NsqdRpcClient) Reconnect() error {
	nrpc.Lock()
	if nrpc.c != nil {
		nrpc.c.Stop()
	}
	if nrpc.grpcConn != nil {
		nrpc.grpcConn.Close()
	}
	nrpc.c = gorpc.NewTCPClient(nrpc.remote)
	nrpc.c.RequestTimeout = nrpc.timeout
	nrpc.c.DisableCompression = true
	nrpc.dc = nsqdClientDispatcher.NewServiceClient("NsqdCoordRpcServer", nrpc.c)
	nrpc.c.Start()

	//ip, port, _ := net.SplitHostPort(nrpc.remote)
	//portNum, _ := strconv.Atoi(port)
	//grpcAddr := ip + ":" + strconv.Itoa(portNum+1)
	//grpcConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(nrpc.timeout))
	//if err != nil {
	//	coordLog.Warningf("failed to connect to grpc server %v: %v", grpcAddr, err)
	//	nrpc.grpcConn = nil
	//	nrpc.grpcClient = nil
	//} else {
	//	nrpc.grpcConn = grpcConn
	//	nrpc.grpcClient = pb.NewNsqdCoordRpcV2Client(grpcConn)
	//}
	coordLog.Infof("reconnected to rpc server %v", nrpc.remote)

	nrpc.Unlock()
	return nil
}

func (nrpc *NsqdRpcClient) CallFast(method string, arg interface{}) (interface{}, error) {
	reply, err := nrpc.dc.CallTimeout(method, arg, time.Second)
	return reply, err
}

func (nrpc *NsqdRpcClient) CallWithRetry(method string, arg interface{}) (interface{}, error) {
	retry := 0
	var err error
	var reply interface{}
	for retry < 5 {
		retry++
		reply, err = nrpc.dc.Call(method, arg)
		if testRPCTimeoutAndWait {
			time.Sleep(maxWriteWaitTimeout)
			e := *gorpc.ErrCanceled
			e.Timeout = true
			e.Connection = true
			err = &e
		}
		if err != nil {
			cerr, ok := err.(*gorpc.ClientError)
			if (ok && cerr.Connection) || nrpc.ShouldRemoved() {
				coordLog.Warningf("rpc connection %v closed, error: %v", nrpc.remote, err)
				connErr := nrpc.Reconnect()
				if connErr != nil {
					return reply, err
				}
			} else {
				return reply, err
			}
		} else {
			if err != nil {
				coordLog.Debugf("rpc call %v error: %v", method, err)
			}
			return reply, err
		}
	}
	return nil, err
}

func (nrpc *NsqdRpcClient) NotifyTopicLeaderSession(epoch EpochType, topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, joinSession string) *CoordErr {
	var rpcInfo RpcTopicLeaderSession
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicLeaderSession = leaderSession.Session
	rpcInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	if leaderSession.LeaderNode != nil {
		rpcInfo.LeaderNode = *leaderSession.LeaderNode
	}
	rpcInfo.JoinSession = joinSession
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	retErr, err := nrpc.CallWithRetry("NotifyTopicLeaderSession", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) NotifyAcquireTopicLeader(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAcquireTopicLeaderReq
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	rpcInfo.TopicWriteEpoch = topicInfo.EpochForWrite
	rpcInfo.Epoch = topicInfo.Epoch
	rpcInfo.LeaderNodeID = topicInfo.Leader
	retErr, err := nrpc.CallWithRetry("NotifyAcquireTopicLeader", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) NotifyReleaseTopicLeader(epoch EpochType, topicInfo *TopicPartitionMetaInfo,
	leaderSessionEpoch EpochType, leaderSession string) *CoordErr {
	var rpcInfo RpcReleaseTopicLeaderReq
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicName = topicInfo.Name
	rpcInfo.TopicPartition = topicInfo.Partition
	rpcInfo.TopicWriteEpoch = topicInfo.EpochForWrite
	rpcInfo.Epoch = topicInfo.Epoch
	rpcInfo.LeaderNodeID = topicInfo.Leader
	rpcInfo.TopicLeaderSessionEpoch = leaderSessionEpoch
	rpcInfo.TopicLeaderSession = leaderSession
	retErr, err := nrpc.CallWithRetry("NotifyReleaseTopicLeader", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) UpdateTopicInfo(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := nrpc.CallWithRetry("UpdateTopicInfo", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) EnableTopicWrite(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := nrpc.CallWithRetry("EnableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) DisableTopicWriteFast(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := nrpc.CallFast("DisableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) DisableTopicWrite(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := nrpc.CallWithRetry("DisableTopicWrite", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) DeleteNsqdTopic(epoch EpochType, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.LookupdEpoch = epoch
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	retErr, err := nrpc.CallFast("DeleteNsqdTopic", &rpcInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) IsTopicWriteDisabled(topicInfo *TopicPartitionMetaInfo) bool {
	var rpcInfo RpcAdminTopicInfo
	rpcInfo.TopicPartitionMetaInfo = *topicInfo
	ret, err := nrpc.CallFast("IsTopicWriteDisabled", &rpcInfo)
	if err != nil {
		return false
	}
	return ret.(bool)
}

func (nrpc *NsqdRpcClient) GetTopicStats(topic string) (*NodeTopicStats, error) {
	stat, err := nrpc.CallWithRetry("GetTopicStats", topic)
	if err != nil {
		return nil, err
	}
	return stat.(*NodeTopicStats), err
}

func (nrpc *NsqdRpcClient) TriggerLookupChanged() error {
	_, err := nrpc.CallFast("TriggerLookupChanged", "")
	return err
}

func (nrpc *NsqdRpcClient) UpdateChannelList(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, chList []string) *CoordErr {
	var updateInfo RpcChannelListArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.ChannelList = chList
	retErr, err := nrpc.CallFast("UpdateChannelList", &updateInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) NotifyChannelList(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, chList []string) *CoordErr {
	var updateInfo RpcChannelListArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.ChannelList = chList
	err := nrpc.dc.Send("UpdateChannelList", &updateInfo)
	return convertRpcError(err, nil)
}

func (nrpc *NsqdRpcClient) DeleteChannel(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string) *CoordErr {
	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.Channel = channel
	retErr, err := nrpc.CallWithRetry("DeleteChannel", &updateInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) UpdateChannelState(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string, paused int, skipped int, zanTestSkipped int) *CoordErr {
	var channelState RpcChannelState
	channelState.TopicName = info.Name
	channelState.TopicPartition = info.Partition
	channelState.TopicWriteEpoch = info.EpochForWrite
	channelState.Epoch = info.Epoch
	channelState.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	channelState.TopicLeaderSession = leaderSession.Session
	channelState.Channel = channel
	channelState.Paused = paused
	channelState.Skipped = skipped
	channelState.ZanTestSkipped = zanTestSkipped

	retErr, err := nrpc.CallWithRetry("UpdateChannelState", &channelState)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) UpdateChannelOffset(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string, offset ChannelConsumerOffset) *CoordErr {
	// it seems grpc is slower, so disable it.
	if nrpc.grpcClient != nil && false {
		var req pb.RpcChannelOffsetArg
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session
		req.TopicData = &rpcData
		req.Channel = channel
		req.ChannelOffset.Voffset = offset.VOffset
		req.ChannelOffset.Vcnt = offset.VCnt
		req.ChannelOffset.Flush = offset.Flush
		req.ChannelOffset.AllowBackward = offset.AllowBackward
		if offset.NeedUpdateConfirmed {
			req.ChannelOffset.NeedUpdateConfirmed = offset.NeedUpdateConfirmed
			for _, interval := range offset.ConfirmedInterval {
				req.ChannelOffset.ConfirmedIntervals = append(req.ChannelOffset.ConfirmedIntervals, pb.MsgQueueInterval{
					Start:  interval.Start,
					End:    interval.End,
					EndCnt: interval.EndCnt,
				})
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		retErr, err := nrpc.grpcClient.UpdateChannelOffset(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}

	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	retErr, err := nrpc.CallFast("UpdateChannelOffset", &updateInfo)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) NotifyUpdateChannelOffset(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, channel string, offset ChannelConsumerOffset) *CoordErr {
	var updateInfo RpcChannelOffsetArg
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.Channel = channel
	updateInfo.ChannelOffset = offset
	err := nrpc.dc.Send("UpdateChannelOffset", &updateInfo)
	return convertRpcError(err, nil)
}

func (nrpc *NsqdRpcClient) UpdateDelayedQueueState(leaderSession *TopicLeaderSession,
	info *TopicPartitionMetaInfo, ch string, ts int64, cursorList [][]byte,
	cntList map[int]uint64, channelCntList map[string]uint64, wait bool) *CoordErr {
	var updateInfo RpcConfirmedDelayedCursor
	updateInfo.TopicName = info.Name
	updateInfo.TopicPartition = info.Partition
	updateInfo.TopicWriteEpoch = info.EpochForWrite
	updateInfo.Epoch = info.Epoch
	updateInfo.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	updateInfo.TopicLeaderSession = leaderSession.Session
	updateInfo.UpdatedChannel = ch
	updateInfo.KeyList = cursorList
	updateInfo.ChannelCntList = channelCntList
	updateInfo.Timestamp = ts
	updateInfo.OtherCntList = cntList
	if wait {
		retErr, err := nrpc.CallFast("UpdateDelayedQueueState", &updateInfo)
		return convertRpcError(err, retErr)
	} else {
		err := nrpc.dc.Send("UpdateDelayedQueueState", &updateInfo)
		return convertRpcError(err, nil)
	}
}

func (nrpc *NsqdRpcClient) PutDelayedMessage(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, log CommitLogData, message *nsqd.Message) *CoordErr {
	// it seems grpc is slower, so disable it.
	var putData RpcPutMessage
	putData.LogData = log
	putData.TopicName = info.Name
	putData.TopicPartition = info.Partition
	putData.TopicMessage = message
	putData.TopicWriteEpoch = info.EpochForWrite
	putData.Epoch = info.Epoch
	putData.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	putData.TopicLeaderSession = leaderSession.Session
	retErr, err := nrpc.CallWithRetry("PutDelayedMessage", &putData)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) PutMessage(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, log CommitLogData, message *nsqd.Message) *CoordErr {
	// it seems grpc is slower, so disable it.
	if nrpc.grpcClient != nil && false {
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		var req pb.RpcPutMessage
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session
		req.TopicData = &rpcData
		var pbLogData pb.CommitLogData
		pbLogData.LogID = log.LogID
		pbLogData.Epoch = int64(log.Epoch)
		pbLogData.MsgNum = log.MsgNum
		pbLogData.MsgCnt = log.MsgCnt
		pbLogData.MsgSize = log.MsgSize
		pbLogData.MsgOffset = log.MsgOffset
		pbLogData.LastMsgLogID = log.LastMsgLogID
		req.LogData = &pbLogData

		var msg pb.NsqdMessage
		msg.ID = uint64(message.ID)
		msg.Body = message.Body
		msg.Trace_ID = message.TraceID
		msg.Attemps = uint32(message.Attempts())
		msg.Timestamp = message.Timestamp
		req.TopicMessage = &msg

		retErr, err := nrpc.grpcClient.PutMessage(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}
	var putData RpcPutMessage
	putData.LogData = log
	putData.TopicName = info.Name
	putData.TopicPartition = info.Partition
	putData.TopicMessage = message
	putData.TopicWriteEpoch = info.EpochForWrite
	putData.Epoch = info.Epoch
	putData.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	putData.TopicLeaderSession = leaderSession.Session
	retErr, err := nrpc.CallWithRetry("PutMessage", &putData)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) PutMessages(leaderSession *TopicLeaderSession, info *TopicPartitionMetaInfo, log CommitLogData, messages []*nsqd.Message) *CoordErr {
	if nrpc.grpcClient != nil && false {
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_SHORT)
		var req pb.RpcPutMessages
		var rpcData pb.RpcTopicData
		rpcData.TopicName = info.Name
		rpcData.TopicPartition = int32(info.Partition)
		rpcData.TopicWriteEpoch = int64(info.EpochForWrite)
		rpcData.Epoch = int64(info.Epoch)
		rpcData.TopicLeaderSessionEpoch = int64(leaderSession.LeaderEpoch)
		rpcData.TopicLeaderSession = leaderSession.Session

		req.TopicData = &rpcData
		var pbLogData pb.CommitLogData
		pbLogData.LogID = log.LogID
		pbLogData.Epoch = int64(log.Epoch)
		pbLogData.MsgNum = log.MsgNum
		pbLogData.MsgCnt = log.MsgCnt
		pbLogData.MsgSize = log.MsgSize
		pbLogData.MsgOffset = log.MsgOffset
		pbLogData.LastMsgLogID = log.LastMsgLogID
		req.LogData = &pbLogData

		for _, message := range messages {
			var msg pb.NsqdMessage
			msg.ID = uint64(message.ID)
			msg.Body = message.Body
			msg.Trace_ID = message.TraceID
			msg.Attemps = uint32(message.Attempts())
			msg.Timestamp = message.Timestamp
			req.TopicMessage = append(req.TopicMessage, &msg)
		}

		retErr, err := nrpc.grpcClient.PutMessages(ctx, &req)
		cancel()
		if err == nil {
			return convertRpcError(err, retErr)
		}
		// maybe old server not implemented the grpc method.
	}

	var putData RpcPutMessages
	putData.LogData = log
	putData.TopicName = info.Name
	putData.TopicPartition = info.Partition
	putData.TopicMessages = messages
	putData.TopicWriteEpoch = info.EpochForWrite
	putData.Epoch = info.Epoch
	putData.TopicLeaderSessionEpoch = leaderSession.LeaderEpoch
	putData.TopicLeaderSession = leaderSession.Session
	retErr, err := nrpc.CallWithRetry("PutMessages", &putData)
	return convertRpcError(err, retErr)
}

func (nrpc *NsqdRpcClient) GetLastCommitLogID(topicInfo *TopicPartitionMetaInfo) (int64, *CoordErr) {
	var req RpcCommitLogReq
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var retErr CoordErr
	ret, err := nrpc.CallWithRetry("GetLastCommitLogID", &req)
	if err != nil || ret == nil {
		return 0, convertRpcError(err, &retErr)
	}
	return ret.(int64), convertRpcError(err, &retErr)
}

func (nrpc *NsqdRpcClient) GetLastDelayedQueueCommitLogID(topicInfo *TopicPartitionMetaInfo) (int64, *CoordErr) {
	var req RpcCommitLogReq
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	var retErr CoordErr
	ret, err := nrpc.CallWithRetry("GetLastDelayedQueueCommitLogID", &req)
	if err != nil || ret == nil {
		return 0, convertRpcError(err, &retErr)
	}
	return ret.(int64), convertRpcError(err, &retErr)
}

func (nrpc *NsqdRpcClient) GetCommitLogFromOffset(topicInfo *TopicPartitionMetaInfo, logCountNumIndex int64,
	logIndex int64, offset int64, fromDelayedQueue bool) (bool, int64, int64, int64, CommitLogData, *CoordErr) {
	var req RpcCommitLogReq
	req.LogStartIndex = logIndex
	req.LogOffset = offset
	req.TopicName = topicInfo.Name
	req.TopicPartition = topicInfo.Partition
	req.LogCountNumIndex = logCountNumIndex
	req.UseCountIndex = true
	var rsp *RpcCommitLogRsp
	var rspVar interface{}
	var err error
	if !fromDelayedQueue {
		rspVar, err = nrpc.CallWithRetry("GetCommitLogFromOffset", &req)
	} else {
		rspVar, err = nrpc.CallWithRetry("GetDelayedQueueCommitLogFromOffset", &req)
	}
	if err != nil {
		return false, 0, 0, 0, CommitLogData{}, convertRpcError(err, nil)
	}
	rsp = rspVar.(*RpcCommitLogRsp)
	return rsp.UseCountIndex, rsp.LogCountNumIndex, rsp.LogStartIndex, rsp.LogOffset, rsp.LogData, convertRpcError(err, &rsp.ErrInfo)

}

func (nrpc *NsqdRpcClient) PullCommitLogsAndData(topic string, partition int, logCountNumIndex int64,
	logIndex int64, startOffset int64, num int, fromDelayed bool) ([]CommitLogData, [][]byte, error) {
	if nrpc.grpcClient != nil && false {
		var req pb.PullCommitLogsReq
		var rpcData pb.RpcTopicData
		rpcData.TopicName = topic
		rpcData.TopicPartition = int32(partition)
		req.TopicData = &rpcData
		req.StartLogOffset = startOffset
		req.StartIndexCnt = logIndex
		req.LogMaxNum = int32(num)
		req.LogCountNumIndex = logCountNumIndex
		req.UseCountIndex = true
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		var rsp *pb.PullCommitLogsRsp
		var err error
		if !fromDelayed {
			rsp, err = nrpc.grpcClient.PullCommitLogsAndData(ctx, &req)
		} else {
			rsp, err = nrpc.grpcClient.PullDelayedQueueCommitLogsAndData(ctx, &req)
		}
		cancel()
		if err == nil {
			logs := make([]CommitLogData, 0, len(rsp.Logs))
			for _, log := range rsp.Logs {
				logs = append(logs, CommitLogData{
					LogID:        log.LogID,
					Epoch:        EpochType(log.Epoch),
					MsgNum:       log.MsgNum,
					MsgCnt:       log.MsgCnt,
					MsgSize:      log.MsgSize,
					MsgOffset:    log.MsgOffset,
					LastMsgLogID: log.LastMsgLogID,
				})
			}
			return logs, rsp.DataList, nil
		}
		// maybe old server not implemented the grpc method.
	}

	var r RpcPullCommitLogsReq
	r.TopicName = topic
	r.TopicPartition = partition
	r.StartLogOffset = startOffset
	r.StartIndexCnt = logIndex
	r.LogMaxNum = num
	r.LogCountNumIndex = logCountNumIndex
	r.UseCountIndex = true
	var ret *RpcPullCommitLogsRsp
	var retVar interface{}
	var err error
	if !fromDelayed {
		retVar, err = nrpc.CallWithRetry("PullCommitLogsAndData", &r)
	} else {
		retVar, err = nrpc.CallWithRetry("PullDelayedQueueCommitLogsAndData", &r)
	}
	if err != nil {
		return nil, nil, err
	}
	ret = retVar.(*RpcPullCommitLogsRsp)
	return ret.Logs, ret.DataList, nil
}

func (nrpc *NsqdRpcClient) GetFullSyncInfo(topic string, partition int, fromDelayed bool) (*LogStartInfo, *CommitLogData, error) {
	var r RpcGetFullSyncInfoReq
	r.TopicName = topic
	r.TopicPartition = partition
	var ret *RpcGetFullSyncInfoRsp
	var retVar interface{}
	var err error
	if !fromDelayed {
		retVar, err = nrpc.CallWithRetry("GetFullSyncInfo", &r)
	} else {
		retVar, err = nrpc.CallWithRetry("GetDelayedQueueFullSyncInfo", &r)
	}
	if err != nil {
		return nil, nil, err
	}
	ret = retVar.(*RpcGetFullSyncInfoRsp)
	return &ret.StartInfo, &ret.FirstLogData, nil
}

func (nrpc *NsqdRpcClient) GetBackupedDelayedQueue(topic string, partition int) (io.Reader, error) {
	var r RpcGetBackupedDQReq
	r.TopicName = topic
	r.TopicPartition = partition
	var ret *RpcGetBackupedDQRsp
	var retVar interface{}
	var err error
	retVar, err = nrpc.CallWithRetry("GetBackupedDelayedQueue", &r)
	if err != nil {
		return nil, err
	}
	ret = retVar.(*RpcGetBackupedDQRsp)
	return bytes.NewBuffer(ret.Buffer), nil
}

func (nrpc *NsqdRpcClient) GetNodeInfo(nid string) (*NsqdNodeInfo, error) {
	var r RpcNodeInfoReq
	r.NodeID = nid
	var ret *RpcNodeInfoRsp
	retVar, err := nrpc.CallFast("GetNodeInfo", &r)
	if err != nil {
		return nil, err
	}
	ret = retVar.(*RpcNodeInfoRsp)
	var nodeInfo NsqdNodeInfo
	nodeInfo.ID = ret.ID
	nodeInfo.NodeIP = ret.NodeIP
	nodeInfo.HttpPort = ret.HttpPort
	nodeInfo.RpcPort = ret.RpcPort
	nodeInfo.TcpPort = ret.TcpPort
	return &nodeInfo, nil
}

func (nrpc *NsqdRpcClient) CallRpcTest(data string) (string, *CoordErr) {
	var req RpcTestReq
	req.Data = data
	var ret *RpcTestRsp
	retVar, err := nrpc.CallWithRetry("TestRpcError", &req)
	if err != nil {
		return "", convertRpcError(err, nil)
	}
	ret = retVar.(*RpcTestRsp)
	return ret.RspData, convertRpcError(err, ret.RetErr)
}

func (nrpc *NsqdRpcClient) CallRpcTesttimeout(data string) error {
	_, err := nrpc.CallWithRetry("TestRpcTimeout", "req")
	return err
}

func (nrpc *NsqdRpcClient) CallRpcTestCoordErr(data string) *CoordErr {
	var req RpcTestReq
	req.Data = data
	reply, err := nrpc.CallWithRetry("TestRpcCoordErr", &req)
	if err != nil {
		return convertRpcError(err, nil)
	}
	return reply.(*CoordErr)
}
