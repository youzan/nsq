package nsqlookupd_migrate

import (
	"fmt"
	"net/http"
	"github.com/julienschmidt/httprouter"
	"net/url"
	"encoding/json"
	"strings"
	"github.com/viki-org/dnscache"
	"sync"
	"github.com/twinj/uuid"
	"strconv"
	"time"
	"net"
	"github.com/absolute8511/glog"
)

type Decorator func(APIHandler) APIHandler

type APIHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, *QueryHost, error)

type Err struct {
	Code int
	Text string
}

type ProxyResponse struct {
	Code int
	Content string
}

func (resp ProxyResponse) String() string {
	return fmt.Sprintf("Code: %v, Content :%v", resp.Code, resp.Content)
}

type QueryHost struct {
	QueryId	string
	RemoteAddr string
	QueryURI	string
	TimeArrives	time.Time
	Resp		ProxyResponse
}

func (qh *QueryHost) String() string {
	return fmt.Sprintf("query ID: %v, remote addr: %v, query URI: %v", qh.QueryId, qh.RemoteAddr, qh.QueryURI)
}

func (e *Err) Error() string {
	return e.Text
}

type httpServer struct {
	lookupAddrOri, lookupAddrTar string
	Router http.Handler
	context *Context
	mg ITopicMigrateGuard
	client *http.Client
}



var mLog *MigrateLogger
var listlookup_not_found_msg = "{\"message\":\"NOT_FOUND\"}"

func SetupLogger(context *Context) {
	mLog = context.Logger
}

func NewHTTPServer(context *Context) (*httpServer, error) {
	mLog = NewMigrateLogger(context.LogLevel)
	context.Logger = mLog
	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = LogPanicHandler()
	router.NotFound = LogNotFoundHandler()
	router.MethodNotAllowed = LogMethodNotAllowedHandler()
	mg, err := NewTopicMigrateGuard(context)
	if err != nil {
		mLog.Error("Fail to initialize topic migrate guard.")
		return nil, err
	}

	resolver := dnscache.New(5 * time.Minute)
	tr := &http.Transport{
		MaxIdleConnsPerHost:       64,
		DisableCompression: true,
		Dial:   func(network string, address string) (net.Conn, error) {
			separator := strings.LastIndex(address, ":")
			ip, _ := resolver.FetchOneString(address[:separator])
			return net.Dial("tcp", ip + address[separator:])
		},
	}
	client := &http.Client{Transport: tr}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(){
		defer wg.Done()
		mg.Init()
	}()

	s := &httpServer{
		lookupAddrOri: context.LookupAddrOri,
		lookupAddrTar: context.LookupAddrTar,
		Router: router,
		context: context,
		mg: mg,
		client:client,
	}
	wg.Wait()
	router.Handle("GET", "/lookup", Decorate(s.lookupMigrateHandler, TagMigrate))
	router.Handle("GET", "/listlookup", Decorate(s.listListLookupHandler, TagMigrate))
	router.Handle("GET", "/topics", Decorate(s.topicsHandler, TagMigrate))
	router.Handle("POST", "/switch", Decorate(s.switchesHandler, TagMigrate))
	mLog.Info("HTTP server initialized")
	return s, nil
}

func (s *httpServer) init() {

}

func (s *httpServer) migrateLookup(qh *QueryHost, topic string, access string, meta bool, compatibleHeadAdded bool, migrateFin bool) (interface{}, http.Header, error) {
	type lookupResp struct {
		lookupinfo interface{}
		h          http.Header
		err        error
	}
	ch_target := make(chan lookupResp, 0)
	ch_ori := make(chan lookupResp, 0)
	go func() {
		lookupinfo_target, h, err := s.lookup(qh, s.lookupAddrTar, topic, access, compatibleHeadAdded, meta)
		mLog.AccessTrace("%v: lookupinfo target %v fetched.", qh.QueryId, lookupinfo_target)
		ch_target <- lookupResp{lookupinfo_target, h, err}
	}()

	if !migrateFin {
		go func() {
			lookupinfo_ori, h, err := s.lookup(qh, s.lookupAddrOri, topic, access, compatibleHeadAdded, meta)
			mLog.AccessTrace("%v: lookupinfo origin %v fetched.", qh.QueryId, lookupinfo_ori)
			ch_ori <- lookupResp{lookupinfo_ori, h, err}
		}()
	} else {
		go func() {
			mLog.AccessTrace("%v: empty lookupinfo origin filled.", qh.QueryId)
			ch_ori <- lookupResp{nil, nil, nil}
		}()
	}

	lookup_tar := <- ch_target
	lookup_ori := <-ch_ori
	//error handle
	if !migrateFin && (lookup_tar.err != nil || lookup_ori.err != nil) {
		//degrade to lookup proxy
		return lookup_ori.lookupinfo, lookup_ori.h, lookup_ori.err
	}

	if migrateFin || access == "w" {
		return lookup_tar.lookupinfo, lookup_tar.h, lookup_tar.err
	}

	//when we hit this line, it is for access="r" which has not finished migration
	if compatibleHeadAdded {
		origin, _ := lookup_ori.lookupinfo.(Lookupinfo)
		target, _ := lookup_tar.lookupinfo.(Lookupinfo)
		origin.Producers = append(origin.Producers, target.Producers...)
		//origin.Meta = target.Meta
		origin.Partitions = make(map[string]*Producerinfo)
		for k, v := range target.Partitions {
			origin.Partitions[k] = v
		}

		mLog.AccessTrace("%v: target producers/partitions info merged into source producers.", qh.QueryId)
		return	origin, lookup_ori.h, nil
	} else {
		origin, _ := lookup_ori.lookupinfo.(Lookupinfo_old)
		target, _ := lookup_tar.lookupinfo.(Lookupinfo_old)
		origin.Data.Producers = append(origin.Data.Producers, target.Data.Producers...)
		//origin.Data.Meta = target.Data.Meta
		origin.Data.Partitions = make(map[string]*Producerinfo)
		for k, v := range target.Data.Partitions {
			origin.Data.Partitions[k] = v
		}

		mLog.AccessTrace("%v: target producers/partitions info old merged into source producers.", qh.QueryId)
		return origin, lookup_ori.h, nil
	}
}

func (s *httpServer)lookup(qh *QueryHost, lookupAdrr string, topic string, access string, compatibleHeadAdded bool, metainfo bool) (interface{}, http.Header, error) {
	client:=  s.client
	var url string
	switch access {
		case "w": {
			if metainfo {
				url = fmt.Sprintf("%s/lookup?topic=%s&access=w&metainfo=true", lookupAdrr, topic)
			} else {
				url = fmt.Sprintf("%s/lookup?topic=%s&access=w", lookupAdrr, topic)
			}
		}
		default: {
			if metainfo {
				url = fmt.Sprintf("%s/lookup?topic=%s&access=r&metainfo=true", lookupAdrr, topic)
			} else {
				url = fmt.Sprintf("%s/lookup?topic=%s&access=r", lookupAdrr, topic)
			}
		}

	}

	mLog.AccessTrace("%v: GET access to %v", qh.QueryId, url)
	req, err := http.NewRequest("GET", url, nil)
	if(err != nil) {
		msg := fmt.Sprintf("%v: fail to create request GET %v, Err: %v", qh.QueryId, url, err)
		mLog.Error(msg)
		return nil, nil, &Err{500, msg}
	}
	if compatibleHeadAdded {
		req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	}
	resp, err := client.Do(req)
	mLog.AccessTrace("%v: response status %v", qh.QueryId, resp.Status)
	if(err != nil) {
		msg := fmt.Sprintf("%v: error on request. Err: %v", qh.QueryId, err)
		mLog.Error(msg)
		return nil, nil, &Err{500, msg}
	}

	defer resp.Body.Close()

	if(compatibleHeadAdded) {
		var lookupinfo Lookupinfo
		if err := json.NewDecoder(resp.Body).Decode(&lookupinfo); err != nil {
			msg := fmt.Sprintf("%v: fail to parse lookup info from %v, Err: %v", qh.QueryId, url, err)
			mLog.Error(msg)
			return nil, nil, &Err{500, msg}
		}
		if resp.StatusCode != 200 {
			return lookupinfo, resp.Header, &Err{resp.StatusCode, "error from upstream"}
		}
		if lookupinfo.Partitions == nil {
			lookupinfo.Partitions = make(map[string]*Producerinfo)
		}
		mLog.AccessTrace("%v: lookup query returns", qh.QueryId)
		return lookupinfo, resp.Header, nil
	} else {
		var lookupinfo_old Lookupinfo_old
		if err := json.NewDecoder(resp.Body).Decode(&lookupinfo_old); err != nil {
			msg := fmt.Sprintf("%v: fail to parse lookup info from %v, Err: %v", qh.QueryId, url, err)
			mLog.Error(msg)
			return nil, nil, &Err{500, msg}
		}
		if resp.StatusCode != 200 {
			return lookupinfo_old, resp.Header, &Err{resp.StatusCode, "error from upstream"}
		}
		if lookupinfo_old.Data.Partitions == nil {
			lookupinfo_old.Data.Partitions = make(map[string]*Producerinfo)
		}
		mLog.AccessTrace("%v: lookup query without Accept header returns", qh.QueryId)
		return lookupinfo_old, resp.Header, nil
	}
}

func (s *httpServer)switchesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}
	mLog.AccessTrace("%v: query %v arrives.", qh.QueryId, qh)
	defer mLog.AccessTrace("%v: switchesHandler returns in %v", qh.QueryId, time.Since(qh.TimeArrives))

	decoder := json.NewDecoder(req.Body)
	var switches map[string]int
	err := decoder.Decode(&switches)
	if err != nil {
		mLog.Error("fail to parse pass in topic switches")
		return nil, qh, err
	}
	var cnt int
	if len(switches) > 0 {
		cnt = s.mg.UpdateTopicSwitches(switches)
	}

	return cnt, qh, err
}

func (s *httpServer)topicsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}
	mLog.AccessTrace("%v: query %v arrives.", qh.QueryId, qh)
	defer mLog.AccessTrace("%v: topicsHandler returns in %v", qh.QueryId, time.Since(qh.TimeArrives))
	//check header
	val := req.Header.Get("Accept")
	hasHeader := false
	if strings.Contains(val, "application/vnd.nsq; version=1.0") {
		hasHeader = true
		w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
	}
	topics, h, err := s.getTopics(qh, s.context.LookupAddrOri, hasHeader)

	var jsonBytes []byte
	var marshall_err error
	if topics != nil {
		jsonBytes, marshall_err = json.Marshal(topics)
		if marshall_err != nil {
			return nil, qh, &Err{500, err.Error()}
		}
	}
	w.Header().Set("Content-Type", h.Get("Content-Type"))
	return jsonBytes, qh, err
}

func (s *httpServer)getTopics(qh *QueryHost, lookupAdrr string, compatibleHeadAdded bool) (interface{}, http.Header, error) {
	client := s.client
	var url string
	url = fmt.Sprintf("%s/topics", lookupAdrr)

	mLog.AccessTrace("%v: GET access to %v, compatibleHeader:%v", qh.QueryId, url, compatibleHeadAdded)
	defer mLog.AccessTrace("%v: topics query returns", qh.QueryId)
	req, err := http.NewRequest("GET", url, nil)
	if(err != nil) {
		msg := fmt.Sprintf("%v: fail to create request GET %v, Err: %v", qh.QueryId, url, err)
		mLog.Error(msg)
		return nil, nil, &Err{500, msg}
	}
	if compatibleHeadAdded {
		req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	}
	resp, err := client.Do(req)
	mLog.AccessTrace("%v: response status %v", qh.QueryId, resp.Status)
	if(err != nil) {
		msg := fmt.Sprintf("%v: error on request. Err: %v", qh.QueryId, err)
		mLog.AccessTrace(msg)
		mLog.Error(msg)
		return nil, nil, &Err{500, msg}
	}

	defer resp.Body.Close()

	var topics Topics
	var topics_old Topics_old
	var decodeErr error
	var response interface{}
	if compatibleHeadAdded {
		decodeErr = json.NewDecoder(resp.Body).Decode(&topics)
		response = topics
	} else {
		decodeErr = json.NewDecoder(resp.Body).Decode(&topics_old)
		response = topics_old
	}

	if decodeErr != nil {
		msg := fmt.Sprintf("%v: fail to parse topics response from %v, Err: %v", qh.QueryId, url, err)
		mLog.Error(msg)
		mLog.AccessTrace(msg)
		return nil, nil, &Err{500, msg}
	}
	if resp.StatusCode != 200 {
		return response,  resp.Header, &Err{resp.StatusCode, "error from upstream"}
	}
	return response, resp.Header, nil

}

func (s *httpServer)listListLookupHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}

	mLog.AccessTrace("%v: query %v arrives.", qh.QueryId, qh)
	defer mLog.AccessTrace("%v: listListLookupHandler returns in %v", qh.QueryId, time.Since(qh.TimeArrives))
	//check header
	val := req.Header.Get("Accept")
	if strings.Contains(val, "application/vnd.nsq; version=1.0") {
		w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
	}
	err := &Err{404, listlookup_not_found_msg}
	bytes := []byte(listlookup_not_found_msg)
	return bytes, qh, err
}

func escape(param string) string {
	return strings.Replace(strings.Replace(param, "\n", "", -1), "\r", "", -1)
}

// return all lookup nodes that registered on etcd, and mark the master/slave info
func (s *httpServer)lookupMigrateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
	qh := &QueryHost{
		QueryId: uuid.NewV4().String(),
		RemoteAddr:req.RemoteAddr,
		QueryURI:req.RequestURI,
		TimeArrives:time.Now(),
	}

	mLog.AccessTrace("%v: query %v arrives.", qh.QueryId, qh)
	defer mLog.AccessTrace("%v: lookupMigrateHandler returns in %v", qh.QueryId, time.Since(qh.TimeArrives))
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, qh, &Err{500, "fail to parse query url"}
	}
	//check access token
	access := reqParams.Get("access")
	if access == "" {
		mLog.AccessTrace("%v: access header not specified in request param, set as read.", qh.QueryId)
		access = "r"
	} else {
		access = escape(access)
	}

	meta := false
	metaParam := reqParams.Get("metainfo")
	if metaParam != "" {
		mLog.AccessTrace("%v: metainfo header specified as true.", qh.QueryId)
		meta = true
	}
	//check topic
	topic := reqParams.Get("topic")
	topic = escape(topic)
	//check header
	val := req.Header.Get("Accept")
	hasHeader := false
	if strings.Contains(val, "application/vnd.nsq; version=1.0") {
		hasHeader = true
		w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
	}
	var lookupinfo interface{}
	var h http.Header

	migrate := true
	migrateFin := false
	mState := s.mg.GetTopicSwitch(topic)
	mLog.AccessTrace("%v: topic: %v, access: %v, accept header: %v, has Header: %v, migrate state: %v", qh.QueryId, topic, access, val, hasHeader, mState)
	w.Header().Add("nsqlookupd_migrate_state", strconv.Itoa(mState))
	switch mState {
	case M_CSR:
		if access == "w" {
			migrate = false
		}
	case M_CSR_PDR:
		if access == "w" {
			migrateFin = true
		}
	case M_FIN:
		migrateFin = true
	default:
		migrate = false
	}


	var nsqlookupd_err error
	if migrate {
		lookupinfo, h, nsqlookupd_err = s.migrateLookup(qh, topic, access, meta, hasHeader, migrateFin)
		if nsqlookupd_err != nil {
			mLog.Error("%v: fail to fetch migrate lookup info", qh.QueryId)
		}
	} else {
		lookupinfo, h, nsqlookupd_err = s.lookup(qh, s.lookupAddrOri, topic, access, hasHeader, meta)
		if nsqlookupd_err != nil {
			mLog.Error("%v: fail to fetch lookup info from %v", qh.QueryId, s.lookupAddrOri)
		}
	}

	//what if there is no lookup info
	var jsonBytes []byte
	var marshall_err error
	if lookupinfo != nil {
		jsonBytes, marshall_err = json.Marshal(lookupinfo)
		if marshall_err != nil {
			return nil, qh, &Err{500, err.Error()}
		}
	}
	w.Header().Set("Content-Type", h.Get("Content-Type"))
	return jsonBytes, qh, nsqlookupd_err
}

func LogPanicHandler() func(w http.ResponseWriter, req *http.Request, p interface{}) {
	return func(w http.ResponseWriter, req *http.Request, p interface{}) {
		glog.Errorf("ERROR: panic in HTTP handler - %s", p)
	}
}

func LogNotFoundHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	})
}

func LogMethodNotAllowedHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	})
}

func Decorate(f APIHandler, ds ...Decorator) httprouter.Handle {
	decorated := f
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		decorated(w, req, ps)
	}
}

func TagMigrate(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, *QueryHost, error) {
		data, qh, err := f(w, req, ps)
		var statusCode int
		w.Header().Add("NSQ-Migrate", "true")
		if err != nil {
			mLog.Info("%v:%v", qh.QueryId, err)
			statusCode = err.(*Err).Code
			Response(w, statusCode, data)
		} else {
			statusCode = 200
			Response(w, statusCode, data)
		}
		qh.Resp.Code = statusCode
		switch data.(type) {
			case string: {
				qh.Resp.Content, _ = data.(string)
			}
			case []byte: {
				qh.Resp.Content = string(data.([]byte))
			}
			default: {
				qh.Resp.Content = "<bytes>"
			}
		}
		mLog.AccessTrace("%v: query returns %v in %v", qh.QueryId, qh.Resp, time.Since(qh.TimeArrives))
		return nil, qh, nil
	}
}

func Response(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	switch data.(type) {
	case string:
		response = []byte(data.(string))
	case []byte:
		response = data.([]byte)
	default:
		response = []byte{}
	}

	w.WriteHeader(code)
	w.Write(response)
}



