package nsqadmin

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"bytes"
	"sort"

	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/youzan/nsq/internal/clusterinfo"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/protocol"
	"github.com/youzan/nsq/internal/version"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

func maybeWarnMsg(msgs []string) string {
	if len(msgs) > 0 {
		return "WARNING: " + strings.Join(msgs, "; ")
	}
	return ""
}

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, timeout time.Duration) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.User != nil {
			passwd, _ := target.User.Password()
			req.SetBasicAuth(target.User.Username(), passwd)
		}
	}
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: http_api.NewDeadlineTransport(timeout),
	}
}

type httpServer struct {
	ctx    *Context
	router http.Handler
	client *http_api.Client
	ci     *clusterinfo.ClusterInfo
}

func NewHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(adminLog)

	client := http_api.NewClient(ctx.nsqadmin.httpClientTLSConfig)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(adminLog)
	router.NotFound = http_api.LogNotFoundHandler(adminLog)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(adminLog)
	s := &httpServer{
		ctx:    ctx,
		router: router,
		client: client,
		ci:     clusterinfo.New(ctx.nsqadmin.opts.Logger, client),
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	router.Handle("GET", "/", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics/:topic", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics/:topic/:channel", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/nodes", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/nodes/:node", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/counter", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/lookup", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/statistics", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/search", http_api.Decorate(s.indexHandler, log))

	router.Handle("GET", "/static/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	router.Handle("GET", "/fonts/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	if s.ctx.nsqadmin.opts.ProxyGraphite {
		proxy := NewSingleHostReverseProxy(ctx.nsqadmin.graphiteURL, 20*time.Second)
		router.Handler("GET", "/render", proxy)
	}

	// v1 endpoints
	router.Handle("GET", "/api/topics", http_api.Decorate(s.topicsHandler, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic", http_api.Decorate(s.topicHandler, log, http_api.V1))
	router.Handle("GET", "/api/coordinators/:node/:topic/:partition", http_api.Decorate(s.coordinatorHandler, log, http_api.V1))
	router.Handle("GET", "/api/lookup/nodes", http_api.Decorate(s.lookupNodesHandler, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic/:channel", http_api.Decorate(s.channelHandler, log, http_api.V1))
	router.Handle("GET", "/api/nodes", http_api.Decorate(s.nodesHandler, log, http_api.V1))
	router.Handle("GET", "/api/nodes/:node", http_api.Decorate(s.nodeHandler, log, http_api.V1))
	router.Handle("POST", "/api/search/messages", http_api.Decorate(s.searchMessageTrace, s.authCheck, log, http_api.V1))
	router.Handle("POST", "/api/topics", http_api.Decorate(s.createTopicChannelHandler, s.authCheck, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic", http_api.Decorate(s.topicActionHandler, s.authCheck, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic/:channel", http_api.Decorate(s.channelActionHandler, s.adminCheck, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic/:channel/admin", http_api.Decorate(s.channelAdminActionHandler, s.adminCheck, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic/:channel/client", http_api.Decorate(s.channelClientActionHandler, s.authCheck, log, http_api.V1))
	router.Handle("DELETE", "/api/nodes/:node", http_api.Decorate(s.tombstoneNodeForTopicHandler, s.adminCheck, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic", http_api.Decorate(s.deleteTopicHandler, s.adminCheck, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic/:channel", http_api.Decorate(s.deleteChannelHandler, s.adminCheck, log, http_api.V1))
	router.Handle("GET", "/api/counter", http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", "/api/graphite", http_api.Decorate(s.graphiteHandler, log, http_api.V1))
	router.Handle("GET", "/api/statistics", http_api.Decorate(s.statisticsHandler, log, http_api.V1))
	router.Handle("GET", "/api/statistics/:sortBy", http_api.Decorate(s.statisticsHandler, log, http_api.V1))
	router.Handle("GET", "/api/cluster/stats", http_api.Decorate(s.clusterStatsHandler, log, http_api.V1))
	router.Handle("GET", "/api/oauth/cas/callback", http_api.Decorate(s.casAuthCallbackHandler, log, http_api.V1))
	router.Handle("GET", "/api/oauth/cas/callback/logout", http_api.Decorate(s.casAuthCallbackLogoutHandler, log, http_api.V1))
	return s
}

func (s *httpServer) getExistingUserInfo(req *http.Request) (IUserAuth, error) {
	return GetUserModel(s.ctx, req)
}

func (s *httpServer) logoutUser(w http.ResponseWriter, req *http.Request) error {
	return LogoutUser(s.ctx, w, req)
}

func (s *httpServer) getUserInfo(w http.ResponseWriter, req *http.Request) (IUserAuth, error) {
	u, err := s.getExistingUserInfo(req)
	if err != nil {
		s.ctx.nsqadmin.logf("error getting existing user, err: %v", err)
		return nil, err
	}
	if u == nil {
		u, err = NewCasUserModel(s.ctx, w, req)
	}
	return u, err
}

func (s *httpServer) validAccessToken(req *http.Request) (valid bool) {
	token, ok := parseAccessToken(req)
	if ok && s.ctx.nsqadmin.accessTokens[token] {
		return true
	}
	return
}

func parseAccessToken(r *http.Request) (token string, ok bool) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return
	}
	const prefix = "Basic "
	if !strings.HasPrefix(auth, prefix) {
		return
	}
	return auth[len(prefix):], true
}

func (s *httpServer) adminCheck(f http_api.APIHandler) http_api.APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		//check user info
		if s.ctx.nsqadmin.IsAuthEnabled() {
			u, err := s.getUserInfo(w, req)
			if err != nil {
				s.ctx.nsqadmin.logf("error in fetching user model %v", err)
				return nil, http_api.Err{http.StatusInternalServerError, "fail to find associated user info"}
			}
			if !u.IsAdmin() && !s.validAccessToken(req) {
				return nil, http_api.Err{http.StatusUnauthorized, "administrator priority needed"}
			}
		}
		return f(w, req, ps)
	}
}

func (s *httpServer) authCheck(f http_api.APIHandler) http_api.APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		//check user info
		if s.ctx.nsqadmin.IsAuthEnabled() {
			u, err := s.getUserInfo(w, req)
			if err != nil {
				s.ctx.nsqadmin.logf("error in fetching user model %v", err)
				return nil, http_api.Err{http.StatusInternalServerError, "fail to find associated user info"}
			}
			if !u.IsLogin() && !s.validAccessToken(req) {
				return nil, http_api.Err{http.StatusUnauthorized, "authentication needed"}
			}
		}
		return f(w, req, ps)
	}
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

type DCLookupdAddrs struct {
	DC           string   `json:"dc"`
	LookupdAddrs []string `json:"lookupAddrs"`
}

func transform2DCLookupdAddrs(dcLookupdAddrs map[string][]string) []*DCLookupdAddrs {
	dcLookupdAddrsList := make([]*DCLookupdAddrs, 0)
	for dc, _ := range dcLookupdAddrs {
		dcLookupdAddrsList = append(dcLookupdAddrsList, &DCLookupdAddrs{dc, dcLookupdAddrs[dc]})
	}
	return dcLookupdAddrsList
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	asset, _ := Asset("index.html")
	t, _ := template.New("index").Funcs(template.FuncMap{"dcLookupdList": transform2DCLookupdAddrs}).Parse(string(asset))

	w.Header().Set("Content-Type", "text/html")
	lookupdAddresses := make([]string, 0)
	//all lookupd addresses from dc
	dcLookupdAddresses := make(map[string][]string)
	lookupdAddresseMap := make(map[string]bool)
	http2DCMap := make(map[string]string)
	for _, addr := range s.ctx.nsqadmin.opts.NSQDHTTPAddresses {
		lookupdAddresseMap[addr] = false
	}

	lookupdNodesDC, err := s.ci.ListAllLookupdNodes(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
	if err != nil {
		s.ctx.nsqadmin.logf("WARNING: failed to list lookupd nodes : %v", err)
	} else {
		s.ctx.nsqadmin.logf("list lookupd found nodes : %v", lookupdNodesDC)
		for _, lookupdNodes := range lookupdNodesDC {
			for _, n := range lookupdNodes.AllNodes {
				addr := net.JoinHostPort(n.NodeIP, n.HttpPort)
				lookupdAddresseMap[addr] = false
				http2DCMap[addr] = lookupdNodes.DC
			}
			if lookupdNodes.LeaderNode.ID != "" {
				leaderAddr := net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort)
				lookupdAddresseMap[leaderAddr] = true
			}
		}
	}
	for addr, isLeader := range lookupdAddresseMap {
		if _, exist := dcLookupdAddresses[http2DCMap[addr]]; !exist {
			dcLookupdAddresses[http2DCMap[addr]] = make([]string, 0)
		}

		if isLeader {
			lookupdAddresses = append(lookupdAddresses, addr+" (Leader)")
			dcLookupdAddresses[http2DCMap[addr]] = append(dcLookupdAddresses[http2DCMap[addr]], addr+" (Leader)")
		} else {
			lookupdAddresses = append(lookupdAddresses, addr)
			dcLookupdAddresses[http2DCMap[addr]] = append(dcLookupdAddresses[http2DCMap[addr]], addr)
		}
	}
	delete(dcLookupdAddresses, "")
	s.ctx.nsqadmin.logf("total lookupd nodes : %v", lookupdAddresses)
	u, _ := s.getUserInfo(w, req)
	//add redirect query to ca auth url
	authUrl, err := url.Parse(s.ctx.nsqadmin.opts.AuthUrl)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to parse authentication url %v : %v", s.ctx.nsqadmin.opts.AuthUrl, err)
		return nil, http_api.Err{http.StatusInternalServerError, "INTERNAL ERROR"}
	}
	t.Execute(w, struct {
		Version                 string
		ProxyGraphite           bool
		GraphEnabled            bool
		GraphiteURL             string
		StatsdInterval          int
		UseStatsdPrefixes       bool
		StatsdCounterFormat     string
		StatsdGaugeFormat       string
		StatsdPrefix            string
		NSQLookupd              []string
		DCNSQLookupd            map[string][]string
		AllNSQLookupds          []string
		DCAllNSQLookupds        map[string][]string
		AuthUrl                 string
		LogoutUrl               string
		Login                   bool
		User                    string
		AuthEnabled             bool
		HasNotificationEndpoint bool
		EnableZanTestSkip       bool
	}{
		Version:                 version.Binary,
		ProxyGraphite:           s.ctx.nsqadmin.opts.ProxyGraphite,
		GraphEnabled:            s.ctx.nsqadmin.opts.GraphiteURL != "",
		GraphiteURL:             s.ctx.nsqadmin.opts.GraphiteURL,
		StatsdInterval:          int(s.ctx.nsqadmin.opts.StatsdInterval / time.Second),
		UseStatsdPrefixes:       s.ctx.nsqadmin.opts.UseStatsdPrefixes,
		StatsdCounterFormat:     s.ctx.nsqadmin.opts.StatsdCounterFormat,
		StatsdGaugeFormat:       s.ctx.nsqadmin.opts.StatsdGaugeFormat,
		StatsdPrefix:            s.ctx.nsqadmin.opts.StatsdPrefix,
		NSQLookupd:              s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		DCNSQLookupd:            s.ctx.nsqadmin.DC2LookupAddresses(),
		AllNSQLookupds:          lookupdAddresses,
		DCAllNSQLookupds:        dcLookupdAddresses,
		AuthUrl:                 authUrl.String(),
		LogoutUrl:               s.ctx.nsqadmin.opts.LogoutUrl,
		Login:                   (s.ctx.nsqadmin.IsAuthEnabled() && u.IsLogin()) || (!s.ctx.nsqadmin.IsAuthEnabled()),
		User:                    u.GetUserName(),
		AuthEnabled:             s.ctx.nsqadmin.IsAuthEnabled(),
		HasNotificationEndpoint: s.ctx.nsqadmin.opts.NotificationHTTPEndpoint != "",
		EnableZanTestSkip:       s.ctx.nsqadmin.opts.EnableZanTestSkip,
	})

	return nil, nil
}

func (s *httpServer) staticAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	assetName := ps.ByName("asset")

	asset, err := Asset(assetName)
	if err != nil {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	ext := path.Ext(assetName)
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		switch ext {
		case ".svg":
			ct = "image/svg+xml"
		case ".woff":
			ct = "application/font-woff"
		case ".ttf":
			ct = "application/font-sfnt"
		case ".eot":
			ct = "application/vnd.ms-fontobject"
		case ".woff2":
			ct = "application/font-woff2"
		}
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}

	return string(asset), nil
}

func (s *httpServer) topicsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	var topics []*clusterinfo.TopicInfo
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC) > 0 {
		fetchMetaStr, _ := reqParams.Get("metaInfo")
		topics, err = s.ci.GetLookupdTopicsMeta(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, fetchMetaStr == "true")
	} else {
		topics, err = s.ci.GetNSQDTopics(s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topics - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	inactive, _ := reqParams.Get("inactive")
	if inactive == "true" {
		maxWeight := 10
		if len(topics) < 10 {
			maxWeight = len(topics)
		}
		sem := semaphore.NewWeighted(int64(maxWeight))
		ctx := context.TODO()
		var channelMapLock sync.RWMutex
		topicChannelMap := make(map[string][]string)
		if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC) == 0 {
			goto respond
		}

		for _, topic := range topics {
			if err := sem.Acquire(ctx, 1); err != nil {
				s.ctx.nsqadmin.logf("ERROR: failed to get semapher - %s", err)
				break
			}
			go func() {
				defer sem.Release(1)
				producers, _, _ := s.ci.GetLookupdTopicProducers(
					topic.TopicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
				if len(producers) == 0 {
					topicChannels, _ := s.ci.GetLookupdTopicChannels(
						topic.TopicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
					channelMapLock.Lock()
					topicChannelMap[topic.TopicName] = topicChannels
					channelMapLock.Unlock()
				}
			}()
		}
		if err := sem.Acquire(ctx, int64(maxWeight)); err != nil {
			s.ctx.nsqadmin.logf("Failed to acquire semaphore: %v", err)
		}
	respond:
		return struct {
			Topics  map[string][]string `json:"topics"`
			Message string              `json:"message"`
		}{topicChannelMap, maybeWarnMsg(messages)}, nil
	}

	return struct {
		Topics  []*clusterinfo.TopicInfo `json:"topics"`
		Message string                   `json:"message"`
	}{topics, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) lookupNodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	nodesDC, err := s.ci.ListAllLookupdNodes(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get lookupd nodes - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: partial error %s", err)
		messages = append(messages, pe.Error())
	}
	return struct {
		*clusterinfo.LookupdNodes
		LookupdNodesDC []*clusterinfo.LookupdNodes `json:"lookupd_nodes_dc"`
		Message        string                      `json:"message"`
	}{nodesDC[0], nodesDC, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) coordinatorHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	partition := ps.ByName("partition")

	var messages []string
	node := ps.ByName("node")

	producers, _, err := s.ci.GetTopicProducers(topicName,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	producer := producers.Search(node)
	if producer == nil {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicCoordStats, err := s.ci.GetNSQDCoordStats(clusterinfo.Producers{producer}, topicName, partition)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to get nsqd coordinator stats - %s", err)
		messages = append(messages, err.Error())
	}

	return struct {
		*clusterinfo.CoordStats
		Message string `json:"message"`
	}{topicCoordStats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")

	producers, _, err := s.ci.GetTopicProducers(topicName,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	topicStats, _, err := s.ci.GetNSQDStatsWithClients(producers, topicName, "partition", true)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	topicCoordStats, err := s.ci.GetNSQDCoordStats(producers, topicName, "")
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to get nsqd topic %v coordinator stats - %s", topicName, err)
		messages = append(messages, err.Error())
	}

	statsMap := make(map[string]map[string]clusterinfo.TopicCoordStat)
	if topicCoordStats != nil {
		for _, stat := range topicCoordStats.TopicCoordStats {
			t, ok := statsMap[stat.Name]
			if !ok {
				t = make(map[string]clusterinfo.TopicCoordStat)
				statsMap[stat.Name] = t
			}
			t[strconv.Itoa(stat.Partition)] = stat
		}
	}

	isOrdered := false
	isMultiPart := false
	isExt := false
	isChannelAutoCreateDisabled := false
	if len(topicStats) > 0 {
		isOrdered = topicStats[0].IsMultiOrdered
		isMultiPart = topicStats[0].IsMultiPart
		isExt = topicStats[0].IsExt
		isChannelAutoCreateDisabled = topicStats[0].IsChannelAutoCreateDisabled
	}

	allNodesTopicStats := &clusterinfo.TopicStats{
		TopicName:                   topicName,
		StatsdName:                  topicName,
		IsMultiOrdered:              isOrdered,
		IsMultiPart:                 isMultiPart,
		IsExt:                       isExt,
		IsChannelAutoCreateDisabled: isChannelAutoCreateDisabled,
	}
	for _, t := range topicStats {
		stat, ok := statsMap[t.TopicName]
		if ok {
			v, ok := stat[t.TopicPartition]
			if ok {
				t.ISRStats = v.ISRStats
				t.CatchupStats = v.CatchupStats
			}
		}
		t.SyncingNum = len(t.ISRStats) + len(t.CatchupStats)
		historyStat, err := s.ci.GetNSQDMessageHistoryStats(t.Node, t.TopicName, t.TopicPartition)
		if err != nil {
			s.ctx.nsqadmin.logf("WARNING: %s", err)
			messages = append(messages, err.Error())
		} else {
			t.PartitionHourlyPubSize = historyStat
		}
		allNodesTopicStats.Add(t)
	}

	return struct {
		*clusterinfo.TopicStats
		Message string `json:"message"`
	}{allNodesTopicStats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	producers, _, err := s.ci.GetTopicProducers(topicName,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	_, allChannelStats, err := s.ci.GetNSQDStatsWithClients(producers, topicName, "partition", true)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get channel metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	//if there is only one channel, disable channel deletion button
	if len(allChannelStats) <= 1 {
		cs, ok := allChannelStats[channelName]
		if ok {
			cs.OnlyChannel = true
		}
	}

	return struct {
		*clusterinfo.ChannelStats
		Message string `json:"message"`
	}{allChannelStats[channelName], maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get nodes - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Nodes   clusterinfo.Producers `json:"nodes"`
		Message string                `json:"message"`
	}{producers, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodeHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	node := ps.ByName("node")

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	producer := producers.Search(node)
	if producer == nil {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicStats, _, err := s.ci.GetNSQDStats(clusterinfo.Producers{producer}, "", "channel-depth", false)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to get nsqd stats - %s", err)
		return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
	}

	var totalClients int64
	var totalMessages int64
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			if len(cs.Clients) != 0 {
				totalClients += int64(len(cs.Clients))
			} else {
				totalClients += int64(cs.ClientNum)
			}
		}
		totalMessages += ts.MessageCount
	}

	return struct {
		Node          string                    `json:"node"`
		TopicStats    []*clusterinfo.TopicStats `json:"topics"`
		TotalMessages int64                     `json:"total_messages"`
		TotalClients  int64                     `json:"total_clients"`
		Message       string                    `json:"message"`
	}{
		Node:          node,
		TopicStats:    topicStats,
		TotalMessages: totalMessages,
		TotalClients:  totalClients,
		Message:       maybeWarnMsg(messages),
	}, nil
}

func (s *httpServer) tombstoneNodeForTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	node := ps.ByName("node")

	var body struct {
		Topic string `json:"topic"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_BODY"}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	err = s.ci.TombstoneNodeForTopic(body.Topic, node,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to tombstone node for topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	s.notifyAdminActionWithUser("tombstone_topic_producer", body.Topic, "", node, req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func hashTraceID(v string) int {
	h := int32(0)
	if len(v) > 0 {
		for i := 0; i < len(v); i++ {
			h = 31*h + int32(v[i])
		}
	}
	if h < 0 {
		h = -1 * h
	}
	return int(h)
}

const (
	MAX_INCR_ID_BIT = 50
)

func GetPartitionFromMsgID(id int64) int {
	// the max partition id will be less than 1024
	return int((uint64(id) & (uint64(1024-1) << MAX_INCR_ID_BIT)) >> MAX_INCR_ID_BIT)
}

func (s *httpServer) searchMessageTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var warnMessages []string
	if s.ctx.nsqadmin.opts.TraceQueryURL == "" {
		return nil, http_api.Err{400, "TRACE service url is not configured"}
	}
	var queryParam struct {
		Topic     string   `json:"topic"`
		Partition string   `json:"partition_id"`
		Channel   string   `json:"channel"`
		MsgID     string   `json:"msgid"`
		TraceID   string   `json:"traceid"`
		Hours     string   `json:"hours"`
		IsHashed  bool     `json:"ishashed"`
		DC        []string `json:"dc"`
	}
	err := json.NewDecoder(req.Body).Decode(&queryParam)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if !protocol.IsValidTopicName(queryParam.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}
	dcChecked := make(map[string]bool)
	if len(queryParam.DC) > 0 {
		for _, dc := range queryParam.DC {
			dcChecked[dc] = true
		}
	} else if len(s.ctx.nsqadmin.opts.DCNSQLookupdHTTPAddresses) > 0 {
		return nil, http_api.Err{400, "AT_LEAST_ONE_DC_NEEDED"}
	}
	filters := make(IndexFieldsQuery, 0)
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	v := reqParams.Values["topic"]
	topicName := queryParam.Topic
	if topicName == "" && len(v) > 0 {
		topicName = v[0]
	}
	filters["topic"] = topicName

	isHashed := queryParam.IsHashed
	var tid string
	if isHashed {
		tid = queryParam.TraceID
	} else {
		tid = strconv.Itoa(hashTraceID(queryParam.TraceID))
	}
	filters["traceid"] = tid
	requestMsgID := int64(0)
	requestMsgID, _ = strconv.ParseInt(queryParam.MsgID, 10, 64)
	filters["msgid"] = queryParam.MsgID

	for k, v := range reqParams.Values {
		if len(v) == 0 {
			continue
		}
		if k == "hashed" {
			continue
		}
		filters[k] = v[0]
	}

	recentHour := 2
	if queryParam.Hours != "" {
		recentHour, err = strconv.Atoi(queryParam.Hours)
		if err != nil {
			recentHour = 2
		}
	}
	queryBody := NewLogQueryInfo(
		s.ctx.nsqadmin.opts.TraceAppName,
		s.ctx.nsqadmin.opts.TraceLogIndexName,
		time.Hour*time.Duration(recentHour),
		filters, s.ctx.nsqadmin.opts.TraceLogPageCount)
	d, _ := json.Marshal(queryBody)

	s.ctx.nsqadmin.logf("search body: %v", string(d))
	traceReq, err := http.NewRequest("POST", s.ctx.nsqadmin.opts.TraceQueryURL, bytes.NewReader(d))
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	traceReq.Header.Add("Content-Type", "application/json; charset=UTF-8")
	var traceResp TraceLogResp
	resp, err := http.DefaultClient.Do(traceReq)
	if err != nil {
		s.ctx.nsqadmin.logf("search failed: %v", err)
		warnMessages = append(warnMessages, err.Error())
	} else {
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			s.ctx.nsqadmin.logf("search failed: %v", err)
			warnMessages = append(warnMessages, err.Error())
		} else {
			if resp.StatusCode != http.StatusOK {
				s.ctx.nsqadmin.logf("search failed: %v", fmt.Errorf("trace query service response: %v %v", resp.Status, string(body)).Error())
				warnMessages = append(warnMessages, resp.Status)
			} else {
				err = json.Unmarshal(body, &traceResp)
				if err != nil {
					s.ctx.nsqadmin.logf("parse search respnse err: %v", err)
					warnMessages = append(warnMessages, err.Error())
				}
				s.ctx.nsqadmin.logf("parse search response : %v", traceResp)
			}
		}
	}
	resultList := traceResp.Data

	if topicName == "" {
		if len(resultList.LogDataDtos) > 0 {
			topicName = resultList.LogDataDtos[0].Topic
		}
	}
	if topicName == "" {
		return nil, http_api.Err{400, "topic should not be empty to search message"}
	}

	_, partitionProducers, _ := s.ci.GetTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if partitionProducers == nil {
		return nil, http_api.Err{500, fmt.Sprintf("partition producers node for %v not found", topicName)}
	}

	//filter out trace messages which do not source from current nsqd
	var tracelogFilteredLock sync.Mutex
	tracelogFiltered := &TraceLog{
		LogDataDtos: make([]TraceLogData, 0),
		TotalCount:  0,
	}

	needGetRequestMsg := true
	maxWeight := int64(10)
	ctx := context.TODO()
	sem := semaphore.NewWeighted(maxWeight)
	for index, m := range resultList.LogDataDtos {
		idx := index
		//check dc from trace message host, filter out messages does not belong to query DC
		dcPrefix := strings.SplitN(m.HostName, "-", 2)[0]
		if len(dcChecked) > 0 {
			if _, exist := dcChecked[dcPrefix]; !exist {
				//set msg id to 0 to prevent adding to logDataFilterEmpty after current loop
				resultList.LogDataDtos[idx].TraceLogItemInfo.MsgID = 0
				continue
			}
		}
		items := make([]TraceLogItemInfo, 0)
		err = json.Unmarshal([]byte(m.Extra), &items)
		// try compatible
		if err != nil || len(items) == 0 {
			err = json.Unmarshal([]byte(m.Extra1), &items)
			if err != nil || len(items) == 0 {
				s.ctx.nsqadmin.logf("msg extra invalid: %v: %v, %v", m.Extra, m.Extra1, err)
				extraJsonStr, _ := strconv.Unquote(m.Extra1)
				err = json.Unmarshal([]byte(extraJsonStr), &items)
				if err != nil || len(items) == 0 {
					s.ctx.nsqadmin.logf("msg extra1 invalid: %v, %v", m.Extra1, err)
					continue
				}
			}
		}
		item := items[0]
		if queryParam.Channel != "" && item.Channel != queryParam.Channel {
			continue
		}
		resultList.LogDataDtos[idx].TraceLogItemInfo = item
		pid := GetPartitionFromMsgID(int64(item.MsgID))
		if len(partitionProducers[strconv.Itoa(pid)]) == 0 {
			s.ctx.nsqadmin.logf("partition producer not found: %v", pid)
			continue
		}

		//nsqd producers of pid among DC
		producers := partitionProducers[strconv.Itoa(pid)]

		if int64(item.MsgID) == requestMsgID {
			needGetRequestMsg = false
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			s.ctx.nsqadmin.logf("ERROR: fail to acquire signal - %v", err)
			warnMessages = append(warnMessages, err.Error())
			break
		}

		for _, producer := range producers {
			//skip producer ip or DC does not match
			if (producer.DC != "" && dcPrefix != producer.DC) || producer.BroadcastAddress != m.HostIp {
				continue
			} else {
				go func() {
					defer sem.Release(int64(1))
					//loop in nsqd node in all DC
					msgBody, _, err := s.ci.GetNSQDMessageByID(*producer, item.Topic, strconv.Itoa(pid), int64(item.MsgID))
					if err != nil {
						s.ctx.nsqadmin.logf("get msg %v data failed : %v", item, err)
					} else {
						resultList.LogDataDtos[idx].RawMsgData = msgBody
						tracelogFilteredLock.Lock()
						//append messages to new filtered log list
						tracelogFiltered.LogDataDtos = append(tracelogFiltered.LogDataDtos, resultList.LogDataDtos[idx])
						tracelogFilteredLock.Unlock()
					}
				}()
			}
		}
	}

	if err := sem.Acquire(ctx, maxWeight); err != nil {
		s.ctx.nsqadmin.logf("ERROR: fail to acquire signal - %v", err)
		warnMessages = append(warnMessages, err.Error())
	}

	//update total count of tracelogFiltered
	tracelogFiltered.TotalCount = len(tracelogFiltered.LogDataDtos)
	logDataFilterEmpty := make(TLListT, 0, len(tracelogFiltered.LogDataDtos))
	for _, v := range tracelogFiltered.LogDataDtos {
		if v.MsgID == 0 {
			continue
		}
		logDataFilterEmpty = append(logDataFilterEmpty, v)
	}
	sort.Sort(logDataFilterEmpty)
	// js can not handle int64 in json, we convert int64 to string for showing.
	logDataForJs := make([]TraceLogDataForJs, 0, len(logDataFilterEmpty))
	for _, v := range logDataFilterEmpty {
		var jsv TraceLogDataForJs
		jsv.TraceLogItemInfoForJs = v.ToJsJson()
		jsv.RawMsgData = v.RawMsgData
		jsv.DC = v.DC
		logDataForJs = append(logDataForJs, jsv)
	}
	if len(warnMessages) > 0 && requestMsgID > 0 {
		needGetRequestMsg = true
	}
	//s.ctx.nsqadmin.logf("sorted msg trace data : %v", logDataFilterEmpty)
	var requestMsg string
	requestMsgDC := make(map[string]string)
	if needGetRequestMsg && requestMsgID > 0 {
		pid := GetPartitionFromMsgID(int64(requestMsgID))
		if len(partitionProducers[strconv.Itoa(pid)]) == 0 {
			s.ctx.nsqadmin.logf("partition producer not found: %v", pid)
		} else {
			//loop through partitionProducers in multi dc context
			producersDC := partitionProducers[strconv.Itoa(pid)]
			hasMultiDC := len(producersDC) > 1
			for _, producer := range producersDC {
				if _, exist := dcChecked[producer.DC]; len(dcChecked) > 0 && !exist {
					continue
				}

				msgBody, _, err := s.ci.GetNSQDMessageByID(*producer, topicName, strconv.Itoa(pid), requestMsgID)
				if err != nil {
					s.ctx.nsqadmin.logf("get msg %v data failed : %v", requestMsgID, err)
					//warnMessages = append(warnMessages, err.Error())
				} else {
					s.ctx.nsqadmin.logf("get msg %v data : %v", requestMsgID, msgBody)
					if hasMultiDC {
						requestMsgDC[producer.DC] = msgBody
						var buf bytes.Buffer
						err := json.Indent(&buf, []byte(msgBody), "", "  ")
						if err == nil {
							requestMsgDC[producer.DC] = buf.String()
						} else {
							s.ctx.nsqadmin.logf("pretty json failed : %v", err)
						}
					} else {
						requestMsg = msgBody
						var buf bytes.Buffer
						err := json.Indent(&buf, []byte(msgBody), "", "  ")
						if err == nil {
							requestMsg = buf.String()
						} else {
							s.ctx.nsqadmin.logf("pretty json failed : %v", err)
						}
					}
				}
			}
		}
	}

	return struct {
		LogDataDtos  []TraceLogDataForJs `json:"logDataDtos"`
		TotalCount   int                 `json:"totalCount"`
		RequestMsg   string              `json:"request_msg"`
		RequestMsgDC map[string]string   `json:"request_msg_dc"`
		Message      string              `json:"message"`
	}{logDataForJs, tracelogFiltered.TotalCount, requestMsg, requestMsgDC, maybeWarnMsg(warnMessages)}, nil
}

func (s *httpServer) createTopicChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	var body struct {
		Topic                    string `json:"topic"`
		PartitionNum             string `json:"partition_num"`
		Replicator               string `json:"replicator"`
		RetentionDays            string `json:"retention_days"`
		SyncDisk                 string `json:"syncdisk"`
		Channel                  string `json:"channel"`
		OrderedMulti             string `json:"orderedmulti"`
		Ext                      string `json:"extend"`
		DisableChannelAutoCreate string `json:"disable_channel_auto_create"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	if body.PartitionNum == "" {
		return nil, http_api.Err{400, "INVALID_TOPIC_PARTITION_NUM"}
	}
	pnum, err := strconv.Atoi(body.PartitionNum)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if body.Replicator == "" {
		return nil, http_api.Err{400, "INVALID_TOPIC_REPLICATOR"}
	}
	replica, err := strconv.Atoi(body.Replicator)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if body.SyncDisk == "" {
		body.SyncDisk = "2000"
	}
	syncDisk, _ := strconv.Atoi(body.SyncDisk)
	err = s.ci.CreateTopic(body.Topic, pnum, replica,
		syncDisk, body.RetentionDays, body.OrderedMulti, body.Ext, body.DisableChannelAutoCreate,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to create topic/channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	s.notifyAdminActionWithUser("create_topic", body.Topic, "", "", req)

	//create default channel
	if body.Channel != "" {
		go func() {
			retry := s.ctx.nsqadmin.opts.ChannelCreationRetry
			for i := 0; i < retry; i++ {
				var err error
				err = s.ci.CreateTopicChannelAfterTopicCreation(body.Topic, body.Channel,
					s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, pnum)

				if err == nil {
					s.notifyAdminActionWithUser("create_channel", body.Topic, body.Channel, "", req)
					s.ctx.nsqadmin.logf("channel created.")
					break
				} else {
					s.ctx.nsqadmin.logf(err.Error())
					backoffTimeout := time.Duration(s.ctx.nsqadmin.opts.ChannelCreationBackoffInterval*pnum) * time.Millisecond
					s.ctx.nsqadmin.logf("Backoff for %v as previous channel %v creation attempt failed.", backoffTimeout, body.Channel)
					time.Sleep(backoffTimeout)
				}
			}
		}()
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	var err error
	topicName := ps.ByName("topic")

	err = s.ci.DeleteTopic(topicName,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to delete topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminActionWithUser("delete_topic", topicName, "", "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	var err error
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	err = s.ci.DeleteChannel(topicName, channelName,
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
		s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to delete channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminActionWithUser("delete_channel", topicName, channelName, "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	return s.topicChannelAction(req, topicName, "")
}

func (s *httpServer) channelClientActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return s.topicChannelClientAction(req, topicName, channelName)
}

func (s *httpServer) topicChannelClientAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	var messages []string
	var body ChannelActionRequest

	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	switch body.Action {
	case "sink":
		if channelName != "" {
			s.notifyAdminActionWithUserAndOrder("sink", topicName, channelName, "", body.Order, req)
		}
	case "sink_removal":
		if channelName != "" {
			s.notifyAdminActionWithUserAndOrder("sink_removal", topicName, channelName, "", body.Order, req)

		}
	case "finish":
		if channelName != "" {
			//parse dc
			node := body.Node
			//parse node and partition
			if node == "" {
				return nil, http_api.Err{400, fmt.Sprintf("INVALID_NSQD_NODE")}
			}
			partition := body.Partition

			//parse msgId
			var msgid int64
			msgid, err = strconv.ParseInt(body.MsgId, 10, 64)
			if err != nil {
				return nil, http_api.Err{400, fmt.Sprintf("INVALID_MSGID: %s", err)}
			}
			if msgid <= 0 {
				return nil, http_api.Err{400, fmt.Sprintf("INVALID_MSGID")}
			}
			err = s.ci.FinishMessage(topicName, channelName, node, partition, msgid)
			if err != nil && strings.Contains(err.Error(), "Message ID not in flight") {
				return nil, http_api.Err{400, fmt.Sprintf("INVALID_MSGID: Message ID not in flight")}
			}
			s.notifyAdminActionWithUser("finish_message", topicName, channelName, node, req)
		}
	default:
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) channelActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return s.topicChannelAction(req, topicName, channelName)
}

func (s *httpServer) channelAdminActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return s.topicChannelAdminAction(req, topicName, channelName)
}

type ChannelActionRequest struct {
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
	Node      string `json:"node"`
	Partition int    `json:"partition"`
	MsgId     string `json:"msgid"`
	Order     bool   `json:"order"`
}

func (s *httpServer) topicChannelAdminAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	var messages []string

	var body ChannelActionRequest
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, fmt.Sprintf("INVALID_REQUEST: %v", err)}
	}

	switch body.Action {
	default:
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicChannelAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	var messages []string

	var body ChannelActionRequest
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	switch body.Action {
	case "pause":
		if channelName != "" {
			err = s.ci.PauseChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
			s.notifyAdminActionWithUser("pause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.PauseTopic(topicName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
			s.notifyAdminActionWithUser("pause_topic", topicName, "", "", req)
		}
	case "unpause":
		if channelName != "" {
			err = s.ci.UnPauseChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
			s.notifyAdminActionWithUser("unpause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.UnPauseTopic(topicName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
			s.notifyAdminActionWithUser("unpause_topic", topicName, "", "", req)
		}
	case "skip":
		if channelName != "" {
			err = s.ci.SkipChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			if err == nil {
				err = s.ci.EmptyChannel(topicName, channelName,
					s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
					s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
			}
			s.notifyAdminActionWithUser("skip_channel", topicName, channelName, "", req)
		}
	case "unskip":
		if channelName != "" {
			err = s.ci.UnSkipChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			s.notifyAdminActionWithUser("unskip_channel", topicName, channelName, "", req)
		}
	case "skipZanTest":
		if channelName != "" {
			err = s.ci.SkipZanTest(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			s.notifyAdminActionWithUser("skip_zantest", topicName, channelName, "", req)
		}
	case "unskipZanTest":
		if channelName != "" {
			err = s.ci.UnskipZanTest(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			s.notifyAdminActionWithUser("unskip_zantest", topicName, channelName, "", req)
		}
	case "empty":
		if channelName != "" {
			err = s.ci.EmptyChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			s.notifyAdminActionWithUser("empty_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.EmptyTopic(topicName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC,
				s.ctx.nsqadmin.opts.NSQDHTTPAddresses)

			s.notifyAdminActionWithUser("empty_topic", topicName, "", "", req)
		}
	case "create":
		if channelName != "" {
			err = s.ci.CreateTopicChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)

			s.notifyAdminActionWithUser("create_channel", topicName, channelName, "", req)
		}
	case "reset":
		if channelName != "" {
			//parse timestamp
			tsStr := fmt.Sprintf("timestamp:%v", body.Timestamp)
			err = s.ci.ResetChannel(topicName, channelName,
				s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, tsStr)

			s.notifyAdminActionWithUser("reset_channel", topicName, channelName, "", req)

		}
	default:
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

type counterStats struct {
	Node         string `json:"node"`
	TopicName    string `json:"topic_name"`
	ChannelName  string `json:"channel_name"`
	MessageCount int64  `json:"message_count"`
}

type rankStats struct {
	Name              string `json:"name"`
	TotalChannelDepth int64  `json:"total_channel_depth,omitempty"`
	MessageCount      int64  `json:"message_count,omitempty"`
	HourlyPubSize     int64  `json:"hourly_pubsize,omitempty"`

	RequeueCount      int64  `json:"requeue_count,omitempty"`
	DelayedQueueCount uint64 `json:"delayed_queue_count,omitempty"`
	TimeoutCount      int64  `json:"timeout_count,omitempty"`
}

type RankList []*rankStats

func (t RankList) Len() int      { return len(t) }
func (t RankList) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type ChannelByTimeout struct {
	RankList
}

func (c ChannelByTimeout) Less(i, j int) bool {
	channeli := c.RankList[i]
	channelj := c.RankList[j]
	return channeli.TimeoutCount > channelj.TimeoutCount
}

type ChannelByRequeue struct {
	RankList
}

func (c ChannelByRequeue) Less(i, j int) bool {
	channeli := c.RankList[i]
	channelj := c.RankList[j]
	return channeli.RequeueCount > channelj.RequeueCount
}

type ChannelByDelayedQueue struct {
	RankList
}

func (c ChannelByDelayedQueue) Less(i, j int) bool {
	channeli := c.RankList[i]
	channelj := c.RankList[j]
	return channeli.DelayedQueueCount > channelj.DelayedQueueCount
}

type TopicsByChannelDepth struct {
	RankList
}

func (c TopicsByChannelDepth) Less(i, j int) bool {
	if c.RankList[i].TotalChannelDepth == c.RankList[j].TotalChannelDepth {
		return c.RankList[i].Name < c.RankList[j].Name
	}
	l := c.RankList[i].TotalChannelDepth
	r := c.RankList[j].TotalChannelDepth
	return l > r
}

type TopicsByHourlyPubsize struct {
	RankList
}

func (c TopicsByHourlyPubsize) Less(i, j int) bool {
	if c.RankList[i].HourlyPubSize == c.RankList[j].HourlyPubSize {
		return c.RankList[i].Name < c.RankList[j].Name
	}
	l := c.RankList[i].HourlyPubSize
	r := c.RankList[j].HourlyPubSize
	return l > r
}

func (s *httpServer) casAuthCallbackLogoutHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	err := s.logoutUser(w, req)
	if err == nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	return nil, err
}

func (s *httpServer) casAuthCallbackHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	v, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: fail to parse cas URL queries %v", req.URL.String())
		return nil, err
	}
	redirectPath := v.Get("qs")

	u, err := s.getUserInfo(w, req)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: fail to get user info, %v", err)
		return nil, err
	}
	u.DoAuth(w, req)
	s.ctx.nsqadmin.logf("ACCESS: %v, login", u)
	if redirectPath != "" {
		s.ctx.nsqadmin.logf("ACCESS: redirect to :%v ", req.Host+redirectPath)
		http.Redirect(w, req, req.Host+redirectPath, http.StatusMovedPermanently)
	} else {
		//redirect to default redirect page in config
		http.Redirect(w, req, s.ctx.nsqadmin.opts.RedirectUrl, http.StatusMovedPermanently)
	}
	return nil, nil
}

func (s *httpServer) clusterStatsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	//get leader lookupd
	clustersInfo := make([]*clusterinfo.ClusterNodeInfo, 0)
	lookupdNodesDC, err := s.ci.ListAllLookupdNodes(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC)
	if err != nil {
		s.ctx.nsqadmin.logf("WARNING: failed to list lookupd nodes : %v", err)
		return nil, err
	} else {
		for _, lookupdNodes := range lookupdNodesDC {
			if lookupdNodes.LeaderNode.ID != "" {
				leaderAddr := net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort)
				clusterNodeInfo, err := s.ci.GetClusterInfo([]string{leaderAddr})
				clusterNodeInfo.DC = lookupdNodes.DC
				if err != nil {
					s.ctx.nsqadmin.logf("ERROR: failed to get cluster nodes stats - %s", err)
					return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
				} else {
					clustersInfo = append(clustersInfo, clusterNodeInfo)
				}

			} else {
				s.ctx.nsqadmin.logf("WARNING: failed to find lookupd leader at this moment")
				return nil, http_api.Err{503, "Service Unavailable"}
			}
		}
		return struct {
			ClustersInfo []*clusterinfo.ClusterNodeInfo `json:"clustersInfo"`
		}{clustersInfo}, nil
	}
}

func (s *httpServer) statisticsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	sortBy := ps.ByName("sortBy")
	s.ctx.nsqadmin.logf("sortBy filter passed in statisticsHandler: " + sortBy)
	if "" == sortBy {
		sortByStr := []string{"channel-depth", "hourly-pubsize", "channel-timeout", "channel-requeue", "channel-delayedqueue"}
		return struct {
			Filter []string `json:"filters"`
		}{sortByStr}, nil
	}

	var rankName string
	switch sortBy {
	case "channel-depth":
		rankName = "Top10 topics in Total Channel Depth"
	case "channel-timeout":
		rankName = "Top10 channels in Timeout"
	case "channel-requeue":
		rankName = "Top10 channels in Requeue"
	case "channel-delayedqueue":
		rankName = "Top10 channels in Delayed Queue"
	default:
		rankName = "Top10 topics in Hourly Pub Size(in bytes)"
	}

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get statistics producer list - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	//get topic channel sortted by partition depth
	topicStatsList, channelStatMap, err := s.ci.GetNSQDStats(producers, "", sortBy, true)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqd stats - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	rank := make([]*rankStats, 0, 1000)
	topicMap := make(map[string]*rankStats)
	switch sortBy {
	case "channel-depth":
		fallthrough
	case "hourly-pubsize":
		nodeMsgHistoryMap, err := s.ci.GetNSQDAllMessageHistoryStats(producers)
		if err != nil {
			_, ok := err.(clusterinfo.PartialErr)
			if !ok {
				s.ctx.nsqadmin.logf("ERROR: failed to get producer topic message history - %s", err)
			}
			s.ctx.nsqadmin.logf("WARNING: %s", err)
			//Do not append errors to messages for compatibility with old nsqd
		}

		//merge nodes under topic
		for _, topicStat := range topicStatsList {
			item, ok := topicMap[topicStat.TopicName]
			if !ok {
				item = &rankStats{
					Name:              topicStat.TopicName,
					TotalChannelDepth: 0,
					MessageCount:      0,
				}
				topicMap[topicStat.TopicName] = item
			}

			item.TotalChannelDepth += topicStat.TotalChannelDepth
			item.MessageCount += topicStat.MessageCount
			if nodeMsgHistoryMap != nil && !ok {
				hpSize, ok := nodeMsgHistoryMap[item.Name]
				if ok {
					item.HourlyPubSize = hpSize
				}
			}
		}

		for _, item := range topicMap {
			if item.TotalChannelDepth <= 0 && item.MessageCount <= 0 {
				continue
			}
			rank = append(rank, item)
		}
	case "channel-timeout":
		fallthrough
	case "channel-delayedqueue":
		fallthrough
	case "channel-requeue":
		channelStatMapDC := make(map[string]*clusterinfo.ChannelStats)
		for _, channelStat := range channelStatMap {
			key := channelStat.TopicName + "/" + channelStat.ChannelName
			if _, exist := channelStatMapDC[key]; !exist {
				channelStatMapDC[key] = channelStat
			} else {
				channelStatMapDC[key].Merge(channelStat)
			}
		}
		for _, channelStat := range channelStatMapDC {
			if channelStat.RequeueCount <= 0 && channelStat.TimeoutCount <= 0 && channelStat.DelayedQueueCount <= 0 {
				continue
			}
			item := &rankStats{
				Name:              channelStat.TopicName + "/" + channelStat.ChannelName,
				RequeueCount:      channelStat.RequeueCount,
				TimeoutCount:      channelStat.TimeoutCount,
				DelayedQueueCount: channelStat.DelayedQueueCount,
			}
			rank = append(rank, item)
		}

	}

	//sort by filter
	switch sortBy {
	case "channel-depth":
		sort.Sort(TopicsByChannelDepth{rank})
	case "hourly-pubsize":
		sort.Sort(TopicsByHourlyPubsize{rank})
	case "channel-timeout":
		sort.Sort(ChannelByTimeout{rank})
	case "channel-delayedqueue":
		sort.Sort(ChannelByDelayedQueue{rank})
	case "channel-requeue":
		sort.Sort(ChannelByRequeue{rank})
	}

	maxLen := 0
	if len(rank) < 10 {
		maxLen = len(rank)
	} else {
		maxLen = 10
	}

	return struct {
		RankName string       `json:"rank_name"`
		Top10    []*rankStats `json:"top10"`
		Message  string       `json:"message"`
	}{
		RankName: rankName,
		Top10:    rank[:maxLen],
		Message:  maybeWarnMsg(messages),
	}, nil
}

type dcCounter struct {
	DC    string                   `json:"dc"`
	Stats map[string]*counterStats `json:"stats"`
}

func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	stats := make(map[string]*counterStats)

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddressesDC, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get counter producer list - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	_, channelStats, err := s.ci.GetNSQDStats(producers, "", "", false)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqd stats - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	for _, channelStats := range channelStats {
		for _, hostChannelStats := range channelStats.NodeStats {
			key := fmt.Sprintf("%s:%s:%s", channelStats.TopicName, channelStats.ChannelName, hostChannelStats.Node)
			s, ok := stats[key]
			if !ok {
				s = &counterStats{
					Node:        hostChannelStats.Node,
					TopicName:   channelStats.TopicName,
					ChannelName: channelStats.ChannelName,
				}
				stats[key] = s
			}
			s.MessageCount += hostChannelStats.MessageCount
		}
	}

	return struct {
		Stats   map[string]*counterStats `json:"stats"`
		Message string                   `json:"message"`
	}{stats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) graphiteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{400, "INVALID_ARG_METRIC"}
	}

	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TARGET"}
	}

	params := url.Values{}
	params.Set("from", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.opts.StatsdInterval*2/time.Second))
	params.Set("until", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.opts.StatsdInterval/time.Second))
	params.Set("format", "json")
	params.Set("target", target)
	query := fmt.Sprintf("/render?%s", params.Encode())
	url := s.ctx.nsqadmin.opts.GraphiteURL + query

	s.ctx.nsqadmin.logf("GRAPHITE: %s", url)

	var response []struct {
		Target     string       `json:"target"`
		DataPoints [][]*float64 `json:"datapoints"`
	}
	_, err = s.client.GETV1(url, &response)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	var rateStr string
	rate := *response[0].DataPoints[0][0]
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := s.ctx.nsqadmin.opts.StatsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return struct {
		Rate string `json:"rate"`
	}{rateStr}, nil
}
