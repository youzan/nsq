package consistence

import (
	"net"
	"os"
	"time"

	pb "github.com/youzan/nsq/consistence/coordgrpc"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/nsqd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// grpc is not used anymore

func (self *NsqdCoordinator) checkWriteForGRpcCall(rpcData *pb.RpcTopicData) (*TopicCoordinator, *CoordErr) {
	if rpcData == nil {
		return nil, ErrPubArgError
	}
	topicCoord, err := self.getTopicCoord(rpcData.TopicName, int(rpcData.TopicPartition))
	if err != nil || topicCoord == nil {
		coordLog.Infof("rpc call with missing topic :%v", rpcData)
		return nil, ErrMissingTopicCoord
	}
	tcData := topicCoord.GetData()
	if tcData.GetTopicEpochForWrite() != EpochType(rpcData.TopicWriteEpoch) {
		coordLog.Infof("rpc call with wrong epoch :%v, current: %v", rpcData, tcData.GetTopicEpochForWrite())
		return nil, ErrEpochMismatch
	}
	if tcData.GetLeaderSession() != rpcData.TopicLeaderSession {
		coordLog.Infof("rpc call with wrong session:%v, local: %v", rpcData, tcData.GetLeaderSession())
		return nil, ErrLeaderSessionMismatch
	}
	return topicCoord, nil
}

type nsqdCoordGRpcServer struct {
	nsqdCoord      *NsqdCoordinator
	rpcServer      *grpc.Server
	dataRootPath   string
	disableRpcTest bool
}

func NewNsqdCoordGRpcServer(coord *NsqdCoordinator, rootPath string) *nsqdCoordGRpcServer {
	return &nsqdCoordGRpcServer{
		nsqdCoord:    coord,
		rpcServer:    grpc.NewServer(),
		dataRootPath: rootPath,
	}
}

func (s *nsqdCoordGRpcServer) start(ip, port string) error {
	lis, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		coordLog.Errorf("starting grpc server error: %v", err)
		os.Exit(1)
	}
	pb.RegisterNsqdCoordRpcV2Server(s.rpcServer, s)
	go s.rpcServer.Serve(lis)
	coordLog.Infof("nsqd grpc coordinator server listen at: %v", lis.Addr())
	return nil
}

func (s *nsqdCoordGRpcServer) stop() {
	if s.rpcServer != nil {
		s.rpcServer.Stop()
	}
}

func (s *nsqdCoordGRpcServer) UpdateChannelOffset(ctx context.Context, req *pb.RpcChannelOffsetArg) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// update local channel offset
	var chOffset ChannelConsumerOffset
	chOffset.Flush = req.ChannelOffset.Flush
	chOffset.VOffset = req.ChannelOffset.Voffset
	chOffset.VCnt = req.ChannelOffset.Vcnt
	chOffset.AllowBackward = req.ChannelOffset.AllowBackward
	chOffset.NeedUpdateConfirmed = req.ChannelOffset.NeedUpdateConfirmed
	for _, interval := range req.ChannelOffset.ConfirmedIntervals {
		chOffset.ConfirmedInterval = append(chOffset.ConfirmedInterval, nsqd.MsgQueueInterval{
			Start:  interval.Start,
			End:    interval.End,
			EndCnt: interval.EndCnt,
		})
	}
	err = s.nsqdCoord.updateChannelOffsetOnSlave(tc.GetData(), req.Channel, chOffset)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}

func (s *nsqdCoordGRpcServer) PutMessage(ctx context.Context, req *pb.RpcPutMessage) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// do local pub message
	commitData := fromPbCommitLogData(req.LogData)
	var msg nsqd.Message
	msg.ID = nsqd.MessageID(req.TopicMessage.ID)
	msg.TraceID = req.TopicMessage.Trace_ID
	msg.SetAttempts(uint16(req.TopicMessage.Attemps))
	msg.Timestamp = req.TopicMessage.Timestamp

	msg.Body = req.TopicMessage.Body
	err = s.nsqdCoord.putMessageOnSlave(tc, commitData, &msg, false)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}

func (s *nsqdCoordGRpcServer) PutMessages(ctx context.Context, req *pb.RpcPutMessages) (*pb.CoordErr, error) {
	var coordErr pb.CoordErr
	if coordLog.Level() >= levellogger.LOG_DEBUG {
		s := time.Now().Unix()
		defer func() {
			e := time.Now().Unix()
			if e-s > int64(RPC_TIMEOUT/2) {
				coordLog.Infof("PutMessage rpc call used: %v", e-s)
			}
		}()
	}

	tc, err := s.nsqdCoord.checkWriteForGRpcCall(req.TopicData)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
		return &coordErr, nil
	}
	// do local pub message
	commitData := fromPbCommitLogData(req.LogData)
	var msgs []*nsqd.Message
	for _, pbm := range req.TopicMessage {
		var msg nsqd.Message
		msg.ID = nsqd.MessageID(pbm.ID)
		msg.TraceID = pbm.Trace_ID
		msg.SetAttempts(uint16(pbm.Attemps))
		msg.Timestamp = pbm.Timestamp
		msg.Body = pbm.Body
		msgs = append(msgs, &msg)
	}

	err = s.nsqdCoord.putMessagesOnSlave(tc, commitData, msgs)
	if err != nil {
		coordErr.ErrMsg = err.ErrMsg
		coordErr.ErrCode = int32(err.ErrCode)
		coordErr.ErrType = int32(err.ErrType)
	}
	return &coordErr, nil
}

func (s *nsqdCoordGRpcServer) PullCommitLogsAndData(ctx context.Context, req *pb.PullCommitLogsReq) (*pb.PullCommitLogsRsp, error) {
	rsp := &pb.PullCommitLogsRsp{}
	rreq := fromPbPullCommitLogsReq(req)
	rrsp, err := s.nsqdCoord.pullCommitLogsAndData(rreq, false)
	if err != nil {
		return rsp, err
	}
	rsp = toPbPullCommitLogRsp(rrsp)
	return rsp, nil
}

func toPbCommitLogData(l CommitLogData) pb.CommitLogData {
	var commitData pb.CommitLogData
	commitData.Epoch = int64(l.Epoch)
	commitData.LogID = l.LogID
	commitData.MsgNum = l.MsgNum
	commitData.MsgCnt = l.MsgCnt
	commitData.MsgSize = l.MsgSize
	commitData.MsgOffset = l.MsgOffset
	commitData.LastMsgLogID = l.LastMsgLogID
	return commitData
}

func fromPbCommitLogData(l *pb.CommitLogData) CommitLogData {
	var commitData CommitLogData
	commitData.Epoch = EpochType(l.Epoch)
	commitData.LogID = l.LogID
	commitData.MsgNum = l.MsgNum
	commitData.MsgCnt = l.MsgCnt
	commitData.MsgSize = l.MsgSize
	commitData.MsgOffset = l.MsgOffset
	commitData.LastMsgLogID = l.LastMsgLogID
	return commitData
}

func toPbPullCommitLogRsp(rrsp *RpcPullCommitLogsRsp) *pb.PullCommitLogsRsp {
	rsp := &pb.PullCommitLogsRsp{}
	rsp.DataList = rrsp.DataList
	rsp.Logs = make([]pb.CommitLogData, 0, len(rrsp.Logs))
	for _, l := range rrsp.Logs {
		rsp.Logs = append(rsp.Logs, toPbCommitLogData(l))
	}
	return rsp
}

func fromPbPullCommitLogsReq(req *pb.PullCommitLogsReq) *RpcPullCommitLogsReq {
	rreq := &RpcPullCommitLogsReq{
		StartLogOffset:   req.StartLogOffset,
		LogMaxNum:        int(req.LogMaxNum),
		StartIndexCnt:    req.StartIndexCnt,
		LogCountNumIndex: req.LogCountNumIndex,
		UseCountIndex:    req.UseCountIndex,
	}
	rreq.TopicName = req.TopicData.TopicName
	rreq.TopicPartition = int(req.TopicData.TopicPartition)
	rreq.Epoch = EpochType(req.TopicData.Epoch)
	rreq.TopicWriteEpoch = EpochType(req.TopicData.TopicWriteEpoch)
	rreq.TopicLeaderSessionEpoch = EpochType(req.TopicData.TopicLeaderSessionEpoch)
	rreq.TopicLeaderSession = req.TopicData.TopicLeaderSession
	rreq.TopicLeader = req.TopicData.TopicLeader
	return rreq
}

func (s *nsqdCoordGRpcServer) PullDelayedQueueCommitLogsAndData(ctx context.Context, req *pb.PullCommitLogsReq) (*pb.PullCommitLogsRsp, error) {
	rsp := &pb.PullCommitLogsRsp{}
	rreq := fromPbPullCommitLogsReq(req)
	rrsp, err := s.nsqdCoord.pullCommitLogsAndData(rreq, true)
	if err != nil {
		return rsp, err
	}
	rsp = toPbPullCommitLogRsp(rrsp)
	return rsp, nil
}
