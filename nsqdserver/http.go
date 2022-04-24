package nsqdserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/clusterinfo"
	"github.com/youzan/nsq/internal/ext"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqd"
)

const HTTP_EXT_HEADER_PREFIX = "X-Nsqext-"

type httpServer struct {
	ctx         *context
	tlsEnabled  bool
	tlsRequired bool
	router      http.Handler
}

func newHTTPServer(ctx *context, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(nsqd.NsqLogger())

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(nsqd.NsqLogger())
	router.NotFound = http_api.LogNotFoundHandler(nsqd.NsqLogger())
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqd.NsqLogger())
	s := &httpServer{
		ctx:         ctx,
		tlsEnabled:  tlsEnabled,
		tlsRequired: tlsRequired,
		router:      router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("POST", "/loglevel/set", http_api.Decorate(s.doSetLogLevel, log, http_api.V1))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.NegotiateVersion))
	router.Handle("POST", "/pub_ext", http_api.Decorate(s.doPUBExt, http_api.NegotiateVersion))
	router.Handle("POST", "/pubtrace", http_api.Decorate(s.doPUBTrace, http_api.V1))
	router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.NegotiateVersion))
	router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.NegotiateVersion))
	router.Handle("GET", "/serverstats", http_api.Decorate(s.doServerStats, log, http_api.V1))
	router.Handle("GET", "/coordinator/stats", http_api.Decorate(s.doCoordStats, log, http_api.V1))
	router.Handle("GET", "/message/stats", http_api.Decorate(s.doMessageStats, log, http_api.V1))
	router.Handle("GET", "/message/get", http_api.Decorate(s.doMessageGet, log, http_api.V1))
	router.Handle("POST", "/message/finish", http_api.Decorate(s.doMessageFinish, log, http_api.V1))
	router.Handle("GET", "/message/historystats", http_api.Decorate(s.doMessageHistoryStats, log, http_api.V1))
	router.Handle("POST", "/message/trace/enable", http_api.Decorate(s.enableMessageTrace, log, http_api.V1))
	router.Handle("POST", "/message/trace/disable", http_api.Decorate(s.disableMessageTrace, log, http_api.V1))
	router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/skip", http_api.Decorate(s.doSkipChannel, log, http_api.V1))
	router.Handle("POST", "/channel/unskip", http_api.Decorate(s.doSkipChannel, log, http_api.V1))
	router.Handle("POST", "/channel/skipZanTest", http_api.Decorate(s.doSkipZanTest, log, http_api.V1))
	router.Handle("POST", "/channel/unskipZanTest", http_api.Decorate(s.doSkipZanTest, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))
	router.Handle("POST", "/channel/fixconfirmed", http_api.Decorate(s.doFixChannelConfirmed, log, http_api.V1))
	router.Handle("POST", "/channel/finishmemdelayed", http_api.Decorate(s.doFinishMemDelayed, log, http_api.V1))
	router.Handle("POST", "/channel/emptydelayed", http_api.Decorate(s.doEmptyChannelDelayed, log, http_api.V1))
	router.Handle("POST", "/channel/setoffset", http_api.Decorate(s.doSetChannelOffset, log, http_api.V1))
	router.Handle("POST", "/channel/setorder", http_api.Decorate(s.doSetChannelOrder, log, http_api.V1))
	router.Handle("POST", "/channel/setclientlimit", http_api.Decorate(s.doSetChannelClientLimit, log, http_api.V1))
	router.Handle("POST", "/channel/ratelimit", http_api.Decorate(s.doSetChannelRateLimit, log, http_api.V1))
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/delayqueue/enable", http_api.Decorate(s.doEnableDelayedQueue, log, http_api.V1))
	router.Handle("PUT", "/filemeta_writer/enable", http_api.Decorate(s.doEnableFileMetaWriter, log, http_api.V1))
	router.Handle("GET", "/delayqueue/backupto", http_api.Decorate(s.doDelayedQueueBackupTo, log, http_api.V1Stream))

	router.Handle("POST", "/topic/cleanunused", http_api.Decorate(s.doCleanUnusedTopicData, log, http_api.V1))
	router.Handle("POST", "/topic/greedyclean", http_api.Decorate(s.doGreedyCleanTopic, log, http_api.V1))
	router.Handle("POST", "/topic/fixdata", http_api.Decorate(s.doFixTopicData, log, http_api.V1))
	//router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, http_api.DeprecatedAPI, log, http_api.V1))
	router.Handle("POST", "/disable/write", http_api.Decorate(s.doDisableClusterWrite, log, http_api.V1))

	router.Handler("GET", "/metric", promhttp.Handler())
	// debug
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.HandlerFunc("GET", "/debug/pprof/trace", pprof.Trace)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(setBlockRateHandler, log, http_api.V1))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("POST", "/debug/freememory", http_api.Decorate(freeOSMemory, log, http_api.V1))

	return s
}

func freeOSMemory(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	debug.FreeOSMemory()
	return nil, nil
}

func setBlockRateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, http_api.Err{http.StatusBadRequest, fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetBlockProfileRate(rate)
	return nil, nil
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !s.tlsEnabled && s.tlsRequired {
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED"}`)
		http_api.Respond(w, 403, "", resp)
		return
	}
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.ctx.getHealth()
	if !s.ctx.isHealthy() {
		return nil, http_api.Err{500, health}
	}
	return health, nil
}

func (s *httpServer) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, http_api.Err{400, "BAD_LEVEL_STRING"}
	}
	nsqd.NsqLogger().SetLevel(int32(level))
	consistence.SetCoordLogLevel(int32(level))
	return nil, nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	httpPort := 0
	if s.ctx.reverseProxyPort == "" {
		httpPort = s.ctx.realHTTPAddr().Port
	} else {
		httpPort, _ = strconv.Atoi(s.ctx.reverseProxyPort)
	}
	return struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		StartTime        int64  `json:"start_time"`
		HASupport        bool   `json:"ha_support"`
	}{
		Version:          version.Binary,
		BroadcastAddress: s.ctx.getOpts().BroadcastAddress,
		Hostname:         hostname,
		TCPPort:          s.ctx.realTCPAddr().Port,
		HTTPPort:         httpPort,
		StartTime:        s.ctx.getStartTime().Unix(),
		HASupport:        s.ctx.nsqdCoord != nil,
	}, nil
}

func (s *httpServer) getExistingTopicChannelFromQuery(req *http.Request) (url.Values, *nsqd.Topic, string, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, nil, "", http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, channelName, err := http_api.GetTopicPartitionChannelArgs(reqParams)
	if err != nil {
		return nil, nil, "", http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}

	topic, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		nsqd.NsqLogger().Logf("topic not found - %s, %v", topicName, err)
		return nil, nil, "", http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	return reqParams, topic, channelName, err
}

//TODO: will be refactored for further extension
func getTag(reqParams url.Values) string {
	return reqParams.Get("tag")
}

func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (url.Values, *nsqd.Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, err := http_api.GetTopicPartitionArgs(reqParams)
	if err != nil {
		return nil, nil, http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}

	topic, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		nsqd.NsqLogger().Logf("topic not found - %s-%v, %v", topicName, topicPart, err)
		return nil, nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	if topicPart != topic.GetTopicPart() {
		return nil, nil, http_api.Err{http.StatusNotFound, "Topic partition not exist"}
	}

	return reqParams, topic, nil
}

func (s *httpServer) doFixTopicData(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, localTopic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, http_api.Err{http.StatusInternalServerError, err.Error()}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	checkCorrupt := reqParams.Get("checkcorrupt")
	localTopic.TryFixData(checkCorrupt == "true")

	if s.ctx.nsqdCoord != nil {
		err = s.ctx.nsqdCoord.TryFixLocalTopic(localTopic.GetTopicName(), localTopic.GetTopicPart())
		if err != nil {
			return nil, http_api.Err{http.StatusInternalServerError, err.Error()}
		}
	}
	return nil, nil
}

func (s *httpServer) doGreedyCleanTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, localTopic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}
	err = s.ctx.GreedyCleanTopicOldData(localTopic)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doDisableClusterWrite(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	disableType := reqParams.Get("type")
	if disableType == "all" {
		consistence.DisableClusterWrite(consistence.ClusterWriteDisabledForAll)
	} else if disableType == "ordered" {
		consistence.DisableClusterWrite(consistence.ClusterWriteDisabledForOrdered)
	} else {
		consistence.DisableClusterWrite(consistence.NoClusterWriteDisable)
	}
	return nil, nil
}

func (s *httpServer) doPUBTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return s.internalPUB(w, req, ps, true, false)
}
func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return s.internalPUB(w, req, ps, false, false)
}

func (s *httpServer) doPUBExt(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return s.internalPUB(w, req, ps, false, true)
}

func (s *httpServer) internalPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params, enableTrace bool, pubExt bool) (interface{}, error) {
	startPub := time.Now().UnixNano()
	// do not support chunked for http pub, use tcp pub instead.
	if req.ContentLength > s.ctx.getOpts().MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	} else if req.ContentLength <= 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	params, topic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		nsqd.NsqLogger().Logf("get topic err: %v", err)
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	readMax := req.ContentLength + 1
	wb := topic.IncrPubWaitingBytes(int64(readMax))
	defer topic.IncrPubWaitingBytes(-1 * int64(readMax))
	if wb > s.ctx.getOpts().MaxPubWaitingSize ||
		(topic.IsWaitChanFull() && wb > s.ctx.getOpts().MaxPubWaitingSize/10) {
		nsqd.TopicPubRefusedByLimitedCnt.With(prometheus.Labels{
			"topic": topic.GetTopicName(),
		}).Inc()
		return nil, http_api.Err{http.StatusTooManyRequests, fmt.Sprintf("E_PUB_TOO_MUCH_WAITING pub too much waiting in the queue: %d", wb)}
	}
	b := topic.BufferPoolGet(int(req.ContentLength))
	defer topic.BufferPoolPut(b)
	asyncAction := !enableTrace
	n, err := io.CopyN(b, io.LimitReader(req.Body, readMax), int64(req.ContentLength))
	body := b.Bytes()[:req.ContentLength]

	if err != nil {
		nsqd.NsqLogger().Logf("read request body error: %v", err)
		body = body[:n]
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// we ignore EOF, maybe the ContentLength is not match?
			nsqd.NsqLogger().LogWarningf("read request body eof: %v, ContentLength: %v,return length %v.",
				err, req.ContentLength, n)
		} else {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	}
	if len(body) == 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		var err error
		var traceIDStr string
		var traceID uint64
		var needTraceRsp bool
		var extContent ext.IExtContent
		isExt := topic.IsExt()

		traceIDStr = params.Get("trace_id")
		traceID, err = strconv.ParseUint(traceIDStr, 10, 0)
		if err != nil && enableTrace {
			nsqd.NsqLogger().Logf("trace id invalid %v, %v",
				traceIDStr, err)
			return nil, http_api.Err{400, "INVALID_TRACE_ID"}
		} else if enableTrace {
			needTraceRsp = true
		}

		//check if request is PUB_WITH_EXT
		var jsonHeaderExt map[string]interface{}
		if pubExt {
			//parse json header ext
			headerStr, err := url.QueryUnescape(params.Get("ext"))
			if err != nil || headerStr == "" {
				return nil, http_api.Err{400, ext.E_INVALID_JSON_HEADER}
			}

			err = json.Unmarshal([]byte(headerStr), &jsonHeaderExt)
			if err != nil {
				return nil, http_api.Err{400, ext.E_INVALID_JSON_HEADER}
			}

			//check header X-Nsqext-XXX:value
			for hKey := range req.Header {
				if strings.HasPrefix(hKey, HTTP_EXT_HEADER_PREFIX) {
					key := strings.TrimPrefix(hKey, HTTP_EXT_HEADER_PREFIX)
					//key parse from X-Nsqext- will always be convert to lowercase
					key = strings.ToLower(key)
					//override parsed kv in json header from header
					jsonHeaderExt[key] = req.Header.Get(hKey)
				}
			}

			//check trace id
			traceIDI, exist := jsonHeaderExt[ext.TRACE_ID_KEY]
			if exist {
				var ok bool
				if traceIDStr, ok = traceIDI.(string); !ok {
					nsqd.NsqLogger().Logf("trace id invalid %v should be string, %v",
						traceIDStr, err)
					return nil, http_api.Err{400, "INVALID_TRACE_ID"}
				}
				traceID, err = strconv.ParseUint(traceIDStr, 10, 0)
				if err != nil {
					nsqd.NsqLogger().Logf("trace id invalid %v, %v",
						traceIDStr, err)
					return nil, http_api.Err{400, "INVALID_TRACE_ID"}
				}
				needTraceRsp = true
			}

			jsonHeaderExtBytes, err := json.Marshal(&jsonHeaderExt)
			if err != nil {
				return nil, http_api.Err{400, ext.E_INVALID_JSON_HEADER}
			}

			jhe := ext.NewJsonHeaderExt()
			jhe.SetJsonHeaderBytes(jsonHeaderExtBytes)
			extContent = jhe
		} else {
			extContent = ext.NewNoExt()
		}
		if !isExt && extContent.ExtVersion() != ext.NO_EXT_VER {
			canIgnoreExt := true
			if jsonHeaderExt != nil {
				// if only internal header, we can ignore
				for k, _ := range jsonHeaderExt {
					// for future, if any internal header can not be ignored, we should check here
					if !strings.HasPrefix(k, "##") {
						canIgnoreExt = false
						nsqd.NsqLogger().Debugf("custom ext content can not be ignored in topic: %v, %v", topic.GetFullName(), k)
						break
					}
				}
			}
			if s.ctx.getOpts().AllowExtCompatible && canIgnoreExt {
				extContent = ext.NewNoExt()
				nsqd.NsqLogger().LogDebugf("topic %v put message with ext to old topic, ignore ext", topic.GetFullName())
			} else {
				return nil, http_api.Err{400, ext.E_EXT_NOT_SUPPORT}
			}
		}
		if needTraceRsp || atomic.LoadInt32(&topic.EnableTrace) == 1 {
			asyncAction = false
		}

		id := nsqd.MessageID(0)
		offset := nsqd.BackendOffset(0)
		rawSize := int32(0)
		if asyncAction {
			err = internalPubAsync(nil, body, topic, extContent)
		} else {
			id, offset, rawSize, _, err = s.ctx.PutMessage(topic, body, extContent, traceID)
		}
		if err != nil {
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
			if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, http_api.Err{400, FailedOnNotWritable}
				}
			}
			return nil, http_api.Err{503, err.Error()}
		}

		if traceID != 0 || atomic.LoadInt32(&topic.EnableTrace) == 1 || nsqd.NsqLogger().Level() >= levellogger.LOG_DETAIL {
			nsqd.GetMsgTracer().TracePubClient(topic.GetTopicName(), topic.GetTopicPart(), traceID, id, offset, req.RemoteAddr)
		}
		cost := time.Now().UnixNano() - startPub
		topic.GetDetailStats().UpdateTopicMsgStats(int64(len(body)), cost/1000)
		if needTraceRsp {
			return struct {
				Status      string `json:"status"`
				ID          uint64 `json:"id"`
				TraceID     string `json:"trace_id"`
				QueueOffset uint64 `json:"queue_offset"`
				DataRawSize uint32 `json:"rawsize"`
			}{"OK", uint64(id), traceIDStr, uint64(offset), uint32(rawSize)}, nil
		} else {
			return "OK", nil
		}
	} else {
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		topic.DisableForSlave(s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()))
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
}

func (s *httpServer) doMPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	startPub := time.Now().UnixNano()
	if req.ContentLength > s.ctx.getOpts().MaxBodySize {
		return nil, http_api.Err{413, "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	wb := topic.IncrPubWaitingBytes(int64(req.ContentLength))
	defer topic.IncrPubWaitingBytes(-1 * int64(req.ContentLength))
	if wb > s.ctx.getOpts().MaxPubWaitingSize ||
		(topic.IsMWaitChanFull() && wb > s.ctx.getOpts().MaxPubWaitingSize/10) {
		nsqd.TopicPubRefusedByLimitedCnt.With(prometheus.Labels{
			"topic": topic.GetTopicName(),
		}).Inc()
		return nil, http_api.Err{http.StatusTooManyRequests, fmt.Sprintf("E_PUB_TOO_MUCH_WAITING pub too much waiting in the queue: %d", wb)}
	}

	var msgs []*nsqd.Message
	var buffers []*bytes.Buffer
	var exit bool

	_, ok := reqParams["binary"]
	if ok {
		tmp := make([]byte, 4)
		msgs, buffers, err = readMPUB(req.Body, tmp, topic,
			s.ctx.getOpts().MaxMsgSize, s.ctx.getOpts().MaxBodySize, false)
		defer func() {
			for _, b := range buffers {
				topic.BufferPoolPut(b)
			}
		}()

		if err != nil {
			return nil, http_api.Err{413, err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.getOpts().MaxBodySize + 1
		rdr := nsqd.NewBufioReader(io.LimitReader(req.Body, readMax))
		defer nsqd.PutBufioReader(rdr)
		total := 0
		for !exit {
			var block []byte
			block, err = rdr.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					return nil, http_api.Err{500, "INTERNAL_ERROR"}
				}
				exit = true
			}
			total += len(block)
			if int64(total) == readMax {
				return nil, http_api.Err{413, "BODY_TOO_BIG"}
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1]
			}

			// silently discard 0 length messages
			// this maintains the behavior pre 0.2.22
			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.ctx.getOpts().MaxMsgSize {
				return nil, http_api.Err{413, "MSG_TOO_BIG"}
			}

			msg := nsqd.NewMessage(0, block)
			msgs = append(msgs, msg)
			topic.GetDetailStats().UpdateTopicMsgStats(int64(len(block)), 0)
		}
	}

	if !s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		//should we forward to master of topic?
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		topic.DisableForSlave(s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()))
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	err = internalMPubAsync(topic, msgs)
	//s.ctx.setHealth(err)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
		if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, http_api.Err{400, FailedOnNotWritable}
			}
		}
		return nil, http_api.Err{503, err.Error()}
	}
	cost := time.Now().UnixNano() - startPub
	topic.GetDetailStats().UpdateTopicMsgStats(0, cost/1000/int64(len(msgs)))
	return "OK", nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, err := http_api.GetTopicPartitionArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}
	err = s.ctx.deleteExistingTopic(topicName, topicPart)
	if err != nil {
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}
	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		//check channel exists in topic partition info, if topic has DisableChannelAutoCreate
		dynamicInfo := topic.GetDynamicInfo()
		if dynamicInfo.DisableChannelAutoCreate {
			return nil, http_api.Err{403, fmt.Sprintf("channel %v not registered. channel should be initialized before use.", channelName)}
		}

		topic.GetChannel(channelName)
		// need sync channel after created
		err := s.ctx.SyncChannels(topic)
		if err != nil {
			nsqd.NsqLogger().Logf("topic %v create channel:%v sync failed: %v ",
				topic.GetFullName(), channelName, err.Error())
			return nil, http_api.Err{500, err.Error()}
		}
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doEmptyChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		var startFrom ConsumeOffset
		startFrom.OffsetType = offsetSpecialType
		startFrom.OffsetValue = -1
		queueOffset, cnt, err := s.ctx.SetChannelOffset(channel, &startFrom, true)
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
		nsqd.NsqLogger().Logf("topic %v empty the channel %v to end offset: %v:%v, by client:%v",
			topic.GetTopicName(), channelName, queueOffset, cnt, req.RemoteAddr)
		err = s.ctx.EmptyChannelDelayedQueue(channel)
		if err != nil {
			nsqd.NsqLogger().Logf("empty the channel %v failed to empty delayed: %v, by client:%v",
				channelName, err, req.RemoteAddr)
		}
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doEmptyChannelDelayed(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}
	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		err = s.ctx.EmptyChannelDelayedQueue(channel)
		if err != nil {
			nsqd.NsqLogger().Logf("failed to empty the channel %v delayed data: %v, by client:%v",
				channelName, err, req.RemoteAddr)
		}
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doFixChannelConfirmed(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		channel.TryFixConfirmedByResetRead()
		nsqd.NsqLogger().Logf("topic %v fix the channel %v confirmed by client:%v",
			topic.GetTopicName(), channelName, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doSetChannelClientLimit(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}
	rdyStr := reqParams.Get("ready")
	rdy, err := strconv.Atoi(rdyStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}
	clientAddrPrefix := reqParams.Get("client_prefix")
	channel.SetClientLimitedRdy(clientAddrPrefix, rdy)
	return nil, nil
}

func (s *httpServer) doSetChannelRateLimit(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}
	rateStr := reqParams.Get("ratekilobytes")
	rateKB, err := strconv.Atoi(rateStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}
	channel.ChangeLimiterBytes(int64(rateKB))
	return nil, nil
}

func (s *httpServer) doSetChannelOrder(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	orderStr := reqParams.Get("order")
	if orderStr == "true" {
		channel.SetOrdered(true)
	} else if orderStr == "false" {
		channel.SetOrdered(false)
	} else {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}
	return nil, nil
}

func (s *httpServer) doSetChannelOffset(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}
	readMax := req.ContentLength + 1
	body := make([]byte, req.ContentLength)
	n, err := io.ReadFull(io.LimitReader(req.Body, readMax), body)
	if err != nil {
		nsqd.NsqLogger().Logf("read request body error: %v", err)
		body = body[:n]
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// we ignore EOF, maybe the ContentLength is not match?
			nsqd.NsqLogger().LogWarningf("read request body eof: %v, ContentLength: %v,return length %v.",
				err, req.ContentLength, n)
		} else {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	}
	if len(body) == 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}
	startFrom := &ConsumeOffset{}
	err = startFrom.FromBytes(body)
	if err != nil {
		nsqd.NsqLogger().Logf("offset %v error: %v", string(body), err)
		return nil, http_api.Err{400, err.Error()}
	}

	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		queueOffset, cnt, err := s.ctx.SetChannelOffset(channel, startFrom, true)
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
		nsqd.NsqLogger().Logf("set the channel offset: %v (actual set : %v:%v), by client:%v",
			startFrom, queueOffset, cnt, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	if s.ctx.checkConsumeForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		dynamicInfo := topic.GetDynamicInfo()
		if dynamicInfo.DisableChannelAutoCreate {
			return nil, http_api.Err{403, fmt.Sprintf("topic %v has channel auto create disabled. channel %v cuold not be deleted localy in nsqd.", topic.GetTopicName(), channelName)}
		}
		clusterErr := s.ctx.DeleteExistingChannel(topic, channelName)
		if clusterErr != nil {
			return nil, http_api.Err{500, clusterErr.Error()}
		}
		nsqd.NsqLogger().Logf("deleted the channel : %v, by client:%v",
			channelName, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}

	return nil, nil
}

func (s *httpServer) doSkipZanTest(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	if !topic.IsExt() {
		nsqd.NsqLogger().Errorf("could not skip zan test messages on topic not support ext. topic:%v", topic.GetTopicName())
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	nsqd.NsqLogger().Logf("topic:%v channel:%v ", topic.GetTopicName(), channel.GetName())

	if strings.Contains(req.URL.Path, "unskipZanTest") {
		//update channel state before set channel consume offset
		err = s.ctx.UpdateChannelState(topic, channel, -1, -1, nsqd.ZanTestUnskip)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		nsqd.NsqLogger().Logf("skip zan test messages")
	} else {
		err = s.ctx.UpdateChannelState(topic, channel, -1, -1, nsqd.ZanTestSkip)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		nsqd.NsqLogger().Logf("unskip zan test messages")
	}

	return nil, nil
}

func (s *httpServer) doSkipChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	nsqd.NsqLogger().Logf("topic:%v channel:%v ", topic.GetTopicName(), channel.GetName())

	if strings.Contains(req.URL.Path, "unskip") {
		//update channel state before set channel consume offset
		err = s.ctx.UpdateChannelState(topic, channel, -1, 0, -1)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	} else {
		err = s.ctx.UpdateChannelState(topic, channel, -1, 1, -1)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	}

	var startFrom ConsumeOffset
	startFrom.OffsetType = offsetSpecialType
	startFrom.OffsetValue = -1
	queueOffset, cnt, err := s.ctx.SetChannelOffset(channel, &startFrom, true)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	nsqd.NsqLogger().Logf("empty the channel to end offset: %v:%v, by client:%v",
		queueOffset, cnt, req.RemoteAddr)
	err = s.ctx.EmptyChannelDelayedQueue(channel)
	if err != nil {
		nsqd.NsqLogger().Logf("empty the channel %v failed to empty delayed: %v, by client:%v",
			channelName, err, req.RemoteAddr)
	}

	return nil, nil
}

func (s *httpServer) doPauseChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	nsqd.NsqLogger().Logf("topic:%v channel:%v ", topic.GetTopicName(), channel.GetName())
	if strings.Contains(req.URL.Path, "unpause") {
		err = s.ctx.UpdateChannelState(topic, channel, 0, -1, -1)
	} else {
		err = s.ctx.UpdateChannelState(topic, channel, 1, -1, -1)
	}
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	return nil, nil
}

func (s *httpServer) enableMessageTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	channelName := reqParams.Get("channel")
	slow := reqParams.Get("slow")

	parts := s.ctx.getPartitions(topicName)
	for _, t := range parts {
		if channelName != "" {
			ch, err := t.GetExistingChannel(channelName)
			if err != nil {
				continue
			}
			if slow == "true" {
				ch.SetSlowTrace(true)
			} else {
				ch.SetTrace(true)
			}
			nsqd.NsqLogger().Logf("channel %v trace enabled", ch.GetName())
		} else {
			t.SetTrace(true)
			nsqd.NsqLogger().Logf("topic %v trace enabled", t.GetFullName())
		}
	}
	return nil, nil
}

func (s *httpServer) disableMessageTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	channelName := reqParams.Get("channel")
	slow := reqParams.Get("slow")
	parts := s.ctx.getPartitions(topicName)
	for _, t := range parts {
		if channelName != "" {
			ch, err := t.GetExistingChannel(channelName)
			if err != nil {
				continue
			}
			if slow == "true" {
				ch.SetSlowTrace(false)
			} else {
				ch.SetTrace(false)
			}
			nsqd.NsqLogger().Logf("channel %v trace disabled", ch.GetName())
		} else {
			t.SetTrace(false)
			nsqd.NsqLogger().Logf("topic %v trace disabled", t.GetFullName())
		}
	}
	return nil, nil
}

func (s *httpServer) doMessageHistoryStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	topicPartStr := reqParams.Get("partition")
	topicPart, err := strconv.Atoi(topicPartStr)
	if err != nil && topicName != "" && topicPartStr != "" {
		nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	if topicName == "" && topicPartStr == "" {
		topicStats := s.ctx.getStats(true, "", "", true)
		var topicHourlyPubStatsList []*clusterinfo.NodeHourlyPubsize
		for _, topicStat := range topicStats {
			partitionNum, err := strconv.Atoi(topicStat.TopicPartition)
			if err != nil {
				nsqd.NsqLogger().LogErrorf("failed to parse partition string %s - %s", topicStat.TopicPartition, err)
				continue
			}
			topic, err := s.ctx.getExistingTopic(topicStat.TopicName, partitionNum)

			if err != nil {
				nsqd.NsqLogger().LogErrorf("failure to get topic  %s:%s - %s", topicStat.TopicName, topicStat.TopicPartition, err)
				continue
			}
			pubhs := topic.GetDetailStats().GetHourlyStats()
			cur := time.Now().Hour() + 2 + 21
			latestHourlyPubsize := pubhs[cur%len(pubhs)]
			aTopicHourlyPubStat := &clusterinfo.NodeHourlyPubsize{
				TopicName:      topicStat.TopicName,
				TopicPartition: topicStat.TopicPartition,
				HourlyPubSize:  latestHourlyPubsize,
			}
			topicHourlyPubStatsList = append(topicHourlyPubStatsList, aTopicHourlyPubStat)
		}

		return struct {
			NodehourlyPubsizeStats []*clusterinfo.NodeHourlyPubsize `json:"node_hourly_pub_size_stats"`
		}{topicHourlyPubStatsList}, nil
	} else {

		t, err := s.ctx.getExistingTopic(topicName, topicPart)
		if err != nil {
			return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
		}
		pubhs := t.GetDetailStats().GetHourlyStats()
		// since the newest 2 hours data maybe update during get, we ignore the newest 2 points
		cur := time.Now().Hour() + 2
		pubhsNew := make([]int64, 0, 22)
		for len(pubhsNew) < 22 {
			pubhsNew = append(pubhsNew, pubhs[cur%len(pubhs)])
			cur++
		}
		return struct {
			HourlyPubSize []int64 `json:"hourly_pub_size"`
		}{pubhsNew}, nil
	}
}

func (s *httpServer) doMessageGet(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	searchMode := reqParams.Get("search_mode")
	searchPosStr := reqParams.Get("search_pos")
	searchPos, err := strconv.ParseInt(searchPosStr, 10, 64)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	var topicPart int
	if searchMode == "id" {
		topicPart = consistence.GetPartitionFromMsgID(searchPos)
		if topicPart >= 1024 {
			nsqd.NsqLogger().LogErrorf("get invalid partition %v from message id- %v",
				topicPart, searchPos)
			return nil, http_api.Err{400, "invalid message id"}
		}
	} else {
		topicPartStr := reqParams.Get("partition")
		topicPart, err = strconv.Atoi(topicPartStr)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
			if searchMode == "id" {
				topicPart = consistence.GetPartitionFromMsgID(searchPos)
				if topicPart >= 1024 {
					nsqd.NsqLogger().LogErrorf("get invalid partition %v from message id- %v",
						topicPart, searchPos)
					return nil, http_api.Err{400, "invalid message id"}
				}
			} else {
				return nil, http_api.Err{400, err.Error()}
			}
		}
	}

	t, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	searchDelayedQueue := reqParams.Get("delayed_queue")
	searchNeedExt := reqParams.Get("needext")
	if searchDelayedQueue == "true" {
		dq := t.GetDelayedQueue()
		if dq == nil {
			return nil, http_api.Err{404, "no delayed queue on the topic"}
		}
		if searchMode != "id" {
			return nil, http_api.Err{400, "delayed queue search mode must be id"}
		}
		ch := reqParams.Get("channel")
		if ch == "" {
			return nil, http_api.Err{400, "delayed queue search channel must be given"}
		}
		msg, err := dq.FindChannelMessageDelayed(nsqd.MessageID(searchPos), ch, true)
		if err != nil {
			return nil, http_api.Err{400, err.Error()}
		}
		if msg == nil {
			return nil, http_api.Err{404, "no message found in delayed queue"}
		}
		var extStr string
		if searchNeedExt == "true" {
			extStr = string(msg.ExtBytes)
		}
		return struct {
			ID        nsqd.MessageID `json:"id"`
			OrigID    uint64         `json:"orig_id"`
			TraceID   uint64         `json:"trace_id"`
			Body      string         `json:"body"`
			Timestamp int64          `json:"timestamp"`
			Attempts  uint16         `json:"attempts"`

			Offset nsqd.BackendOffset `json:"offset"`
			Ext    string             `json:"ext"`
		}{msg.ID, uint64(msg.DelayedOrigID), msg.TraceID, string(msg.Body), msg.Timestamp, msg.Attempts(), msg.Offset, extStr}, nil
	}
	var realOffset int64
	var curCnt int64
	if searchMode == "count" {
		_, realOffset, curCnt, err = s.ctx.nsqdCoord.SearchLogByMsgCnt(topicName, topicPart, searchPos)
	} else if searchMode == "id" {
		_, realOffset, curCnt, err = s.ctx.nsqdCoord.SearchLogByMsgID(topicName, topicPart, searchPos)
	} else if searchMode == "virtual_offset" {
		_, realOffset, curCnt, err = s.ctx.nsqdCoord.SearchLogByMsgOffset(topicName, topicPart, searchPos)
	} else {
		return nil, http_api.Err{400, "search mode should be one of id/count/virtual_offset"}
	}
	if err != nil {
		return nil, http_api.Err{404, err.Error()}
	}
	backendReader := t.GetDiskQueueSnapshot(true)
	if backendReader == nil {
		return nil, http_api.Err{500, "Failed to get queue reader"}
	}
	backendReader.SeekTo(nsqd.BackendOffset(realOffset), curCnt)
	ret := backendReader.ReadOne()
	if ret.Err != nil {
		nsqd.NsqLogger().LogErrorf("search %v-%v, read data error: %v", searchMode, searchPos, ret)
		return nil, http_api.Err{400, err.Error()}
	}
	msg, err := nsqd.DecodeMessage(ret.Data, t.IsExt())
	if err != nil {
		nsqd.NsqLogger().LogErrorf("search %v-%v, decode data error: %v", searchMode, searchPos, err)
		return nil, http_api.Err{400, err.Error()}
	}
	var extStr string
	if searchNeedExt == "true" {
		extStr = string(msg.ExtBytes)
	}
	return struct {
		ID        nsqd.MessageID `json:"id"`
		TraceID   uint64         `json:"trace_id"`
		Body      string         `json:"body"`
		Timestamp int64          `json:"timestamp"`
		Attempts  uint16         `json:"attempts"`

		Offset        nsqd.BackendOffset `json:"offset"`
		QueueCntIndex int64              `json:"queue_cnt_index"`
		Ext           string             `json:"ext"`
	}{msg.ID, msg.TraceID, string(msg.Body), msg.Timestamp, msg.Attempts(), ret.Offset, ret.CurCnt, extStr}, nil
}

func (s *httpServer) doMessageStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, t, chName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	statStr := t.GetTopicChannelDebugStat(chName)
	return statStr, nil
}

func (s *httpServer) doMessageFinish(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, t, chName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}
	topicName := t.GetTopicName()
	topicPart := t.GetTopicPart()

	ch, err := t.GetExistingChannel(chName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}
	msgIDStr := reqParams.Get("msgid")
	msgID, err := strconv.ParseInt(msgIDStr, 10, 64)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to get msgid - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	if !s.ctx.checkConsumeForMasterWrite(topicName, topicPart) {
		nsqd.NsqLogger().Logf("topic %v fin message failed for not leader", topicName)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}

	err = s.ctx.FinishMessageForce(ch, nsqd.MessageID(msgID))
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}

	nsqd.NsqLogger().Logf("topic %v-%v channel %v msgid %v is finished by api", topicName,
		topicPart, chName, msgID)
	return nil, nil
}

func (s *httpServer) doFinishMemDelayed(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, t, chName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	ch, err := t.GetExistingChannel(chName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if !s.ctx.checkConsumeForMasterWrite(t.GetTopicName(), t.GetTopicPart()) {
		nsqd.NsqLogger().Logf("topic %v fin message failed for not leader", t.GetTopicName())
		return nil, http_api.Err{400, FailedOnNotLeader}
	}

	memDelayedMsgs := ch.GetMemDelayedMsgs()
	for _, id := range memDelayedMsgs {
		err = s.ctx.FinishMessageForce(ch, id)
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
	}
	nsqd.NsqLogger().Logf("topic %v-%v channel %v msgids %v is finished by api", t.GetTopicName(),
		t.GetTopicPart(), chName, memDelayedMsgs)
	return nil, nil
}

func (s *httpServer) doCoordStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqdCoord != nil {
		reqParams, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
			return nil, http_api.Err{400, "INVALID_REQUEST"}
		}
		topicName := reqParams.Get("topic")
		topicPartStr := reqParams.Get("partition")
		topicPart := -1
		if topicPartStr != "" {
			topicPart, err = strconv.Atoi(topicPartStr)
			if err != nil {
				nsqd.NsqLogger().LogErrorf("invalid partition: %v - %s", topicPartStr, err)
				return nil, http_api.Err{400, "INVALID_REQUEST"}
			}
		}

		return s.ctx.nsqdCoord.Stats(topicName, topicPart), nil
	}
	return nil, http_api.Err{500, "Coordinator is disabled."}
}

func (s *httpServer) doServerStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.ctx.getHealth()
	startTime := s.ctx.getStartTime()
	pubFailed := getServerPubFailed()
	return struct {
		Version         string `json:"version"`
		Health          string `json:"health"`
		StartTime       int64  `json:"start_time"`
		ServerPubFailed int64  `json:"server_pub_failed"`
	}{version.Binary, health, startTime.Unix(), pubFailed}, nil
}

func (s *httpServer) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	formatString := reqParams.Get("format")
	topicName := reqParams.Get("topic")
	topicPart := reqParams.Get("partition")
	channelName := reqParams.Get("channel")
	leaderOnlyStr := reqParams.Get("leaderOnly")
	needClients := reqParams.Get("needClients")
	var leaderOnly bool
	leaderOnly, _ = strconv.ParseBool(leaderOnlyStr)

	jsonFormat := formatString == "json"
	filterClients := needClients != "true"
	if topicName != "" && needClients == "" {
		// compatible with old, we always return clients if topic is specified and needClients is not specified
		filterClients = false
	}

	stats := s.ctx.getStats(leaderOnly, topicName, channelName, filterClients)
	health := s.ctx.getHealth()
	startTime := s.ctx.getStartTime()
	uptime := time.Since(startTime)

	// If we WERE given a channel-name, remove stats for all the other channels:
	// If we need the partition for topic, remove other partitions
	if len(channelName) > 0 || len(topicPart) > 0 {
		filteredStats := make([]nsqd.TopicStats, 0, len(stats))
		// Find the desired-topic-index:
		for _, topicStats := range stats {
			if len(topicPart) > 0 && topicStats.TopicPartition != topicPart {
				nsqd.NsqLogger().Logf("ignored stats topic partition mismatch - %v, %v", topicPart, topicStats.TopicPartition)
				continue
			}
			if len(channelName) > 0 {
				// Find the desired-channel:
				for _, channelStats := range topicStats.Channels {
					if channelStats.ChannelName == channelName {
						topicStats.Channels = []nsqd.ChannelStats{channelStats}
						// We've got the channel we were looking for:
						break
					}
				}
			}

			// We've got the topic we were looking for:
			// now only the mulit ordered topic can have several partitions on the same node
			filteredStats = append(filteredStats, topicStats)
		}
		stats = filteredStats
	}

	if !jsonFormat {
		return s.printStats(stats, health, startTime, uptime), nil
	}

	return struct {
		Version   string            `json:"version"`
		Health    string            `json:"health"`
		StartTime int64             `json:"start_time"`
		Topics    []nsqd.TopicStats `json:"topics"`
	}{version.Binary, health, startTime.Unix(), stats}, nil
}

func (s *httpServer) printStats(stats []nsqd.TopicStats, health string, startTime time.Time, uptime time.Duration) []byte {
	var buf bytes.Buffer
	w := &buf
	now := time.Now()
	io.WriteString(w, fmt.Sprintf("%s\n", version.String("nsqd")))
	io.WriteString(w, fmt.Sprintf("start_time %v\n", startTime.Format(time.RFC3339)))
	io.WriteString(w, fmt.Sprintf("uptime %s\n", uptime))
	if len(stats) == 0 {
		io.WriteString(w, "\nNO_TOPICS\n")
		return buf.Bytes()
	}
	io.WriteString(w, fmt.Sprintf("\nHealth: %s\n", health))
	for _, t := range stats {
		var pausedPrefix string
		pausedPrefix = "   "
		io.WriteString(w, fmt.Sprintf("\n%s[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d e2e%%: %s\n",
			pausedPrefix,
			t.TopicName,
			t.Depth,
			t.BackendDepth,
			t.MessageCount,
			t.E2eProcessingLatency))
		for _, c := range t.Channels {
			if c.Paused {
				pausedPrefix = "   *P "
			} else {
				pausedPrefix = "      "
			}
			io.WriteString(w,
				fmt.Sprintf("%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d e2e%%: %s\n",
					pausedPrefix,
					c.ChannelName,
					c.Depth,
					c.BackendDepth,
					c.InFlightCount,
					c.DeferredCount,
					c.RequeueCount,
					c.TimeoutCount,
					c.MessageCount,
					c.E2eProcessingLatency))
			for _, client := range c.Clients {
				connectTime := time.Unix(client.ConnectTime, 0)
				// truncate to the second
				duration := time.Duration(int64(now.Sub(connectTime).Seconds())) * time.Second
				_, port, _ := net.SplitHostPort(client.RemoteAddress)
				io.WriteString(w, fmt.Sprintf("        [%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s\n",
					client.Version,
					fmt.Sprintf("%s:%s", client.Name, port),
					client.State,
					client.InFlightCount,
					client.ReadyCount,
					client.FinishCount,
					client.RequeueCount,
					client.MessageCount,
					duration,
				))
			}
		}
	}
	return buf.Bytes()
}

func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.getOpts().MaxMsgSize + 1
		body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.ctx.getOpts()
		switch opt {
		case "nsqlookupd_tcp_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdTCPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "verbose":
			err := json.Unmarshal(body, &opts.Verbose)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			err := json.Unmarshal(body, &opts.LogLevel)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("log level set to : %v", opts.LogLevel)
		case "allow_ext_compatible":
			err := json.Unmarshal(body, &opts.AllowExtCompatible)
			if err != nil {
				nsqd.NsqLogger().Logf("invalid value : %v", string(body))
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("pub ext compatible set to : %v", opts.AllowExtCompatible)
		case "allow_sub_ext_compatible":
			err := json.Unmarshal(body, &opts.AllowSubExtCompatible)
			if err != nil {
				nsqd.NsqLogger().Logf("invalid value : %v", string(body))
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("sub ext compatible set to : %v", opts.AllowSubExtCompatible)
		case "max_conn_for_client":
			err := json.Unmarshal(body, &opts.MaxConnForClient)
			if err != nil {
				nsqd.NsqLogger().Logf("invalid value : %v", string(body))
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("max conn for client set to : %v", opts.MaxConnForClient)
		case "max_pub_waiting_size":
			err := json.Unmarshal(body, &opts.MaxPubWaitingSize)
			if err != nil {
				nsqd.NsqLogger().Logf("invalid value : %v", string(body))
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("max pub waiting size set to : %v", opts.MaxPubWaitingSize)
		default:
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		s.ctx.swapOpts(&opts)
		s.ctx.triggerOptsNotification()
	}

	v, ok := getOptByCfgName(s.ctx.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	val := reflect.ValueOf(opts).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		if name != cfgName {
			continue
		}
		return val.FieldByName(field.Name).Interface(), true
	}
	return nil, false
}

func (s *httpServer) doEnableDelayedQueue(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	enableStr := reqParams.Get("enable")
	if enableStr == "true" {
		atomic.StoreInt32(&nsqd.EnableDelayedQueue, 1)
		s.ctx.persistMetadata()
	}
	return nil, nil
}

func (s *httpServer) doEnableFileMetaWriter(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	enableStr := reqParams.Get("state")
	if enableStr == "true" {
		nsqd.SwitchEnableFileMetaWriter(true)
	} else if enableStr == "false" {
		nsqd.SwitchEnableFileMetaWriter(false)
	}
	return nil, nil
}

func (s *httpServer) doDelayedQueueBackupTo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	topicPartStr := reqParams.Get("partition")
	topicPart, err := strconv.Atoi(topicPartStr)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	t, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	dq := t.GetDelayedQueue()
	if dq == nil {
		return nil, http_api.Err{400, "No delayed queue on this topic"}
	}
	_, err = dq.BackupKVStoreTo(w)
	if err != nil {
		nsqd.NsqLogger().Logf("failed to backup delayed queue for topic %v: %v", topicName, err)
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doCleanUnusedTopicData(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqdCoord == nil {
		return nil, nil
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	localTopics := make([]string, 0)
	topicName := reqParams.Get("topic")
	checkAll := reqParams.Get("checkall")
	dryRunStr := reqParams.Get("dryrun")
	topicPartStr := reqParams.Get("partition")
	topicPart := -1
	if topicPartStr != "" {
		topicPart, err = strconv.Atoi(topicPartStr)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
			return nil, http_api.Err{400, "INVALID_REQUEST"}
		}
	}
	if topicName != "" {
		localTopics = append(localTopics, topicName)
	}
	// find all topic path
	if len(localTopics) == 0 && checkAll == "true" {
		files, _ := ioutil.ReadDir(s.ctx.getOpts().DataPath)
		for _, f := range files {
			if !f.IsDir() {
				continue
			}
			localTopics = append(localTopics, f.Name())
		}
	}
	dryRun := true
	if dryRunStr == "false" {
		dryRun = false
	}
	for _, tn := range localTopics {
		if topicPart >= 0 {
			_, err := s.ctx.getExistingTopic(tn, topicPart)
			if err == nil {
				continue
			}
			err = s.ctx.nsqdCoord.TryCleanUnusedTopicOnLocal(tn, topicPart, dryRun)
			if err != nil {
				nsqd.NsqLogger().Logf("failed to clean topic %v: %v", tn, err)
			}
		} else {
			meta, err := s.ctx.nsqdCoord.GetTopicMetaInfo(tn)
			maxPart := 8
			if err != nil {
				if err != consistence.ErrKeyNotFound {
					continue
				}
			} else {
				maxPart = meta.PartitionNum
			}
			for p := 0; p < maxPart; p++ {
				_, err := s.ctx.getExistingTopic(tn, p)
				if err == nil {
					continue
				}
				err = s.ctx.nsqdCoord.TryCleanUnusedTopicOnLocal(tn, p, dryRun)
				if err != nil {
					//
					nsqd.NsqLogger().Logf("failed to clean topic %v: %v", tn, err)
				}
			}
		}
	}
	return nil, nil
}
