package nsqadmin

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"encoding/json"
)

type AdminAction struct {
	Action    string `json:"action"`
	Topic     string `json:"topic"`
	Order	  bool   `json:"order"`
	Channel   string `json:"channel,omitempty"`
	Node      string `json:"node,omitempty"`
	Timestamp int64  `json:"timestamp"`
	User      string `json:"user,omitempty"`
	RemoteIP  string `json:"remote_ip"`
	UserAgent string `json:"user_agent"`
	URL       string `json:"url"` // The URL of the HTTP request that triggered this action
	Via       string `json:"via"` // the Hostname of the nsqadmin performing this action
}

func (a *AdminAction) String() string {
	bytes, err := json.Marshal(*a)
	if err == nil {
		return string(bytes)
	}
	return ""
}

func basicAuthUser(req *http.Request) string {
	s := strings.SplitN(req.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != "Basic" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return ""
	}
	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return ""
	}
	return pair[0]
}


func (s *httpServer) notifyAdminActionWithUser(action, topic, channel, node string, req *http.Request) {
	s.notifyAdminActionWithUserAndOrder(action, topic, channel, node, false, req)
}

func (s *httpServer) notifyAdminActionWithUserAndOrder(action, topic, channel, node string, order bool, req *http.Request) {
	via, _ := os.Hostname()
	u := url.URL{
		Scheme:   "http",
		Host:     req.Host,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
	}
	if req.TLS != nil || req.Header.Get("X-Scheme") == "https" {
		u.Scheme = "https"
	}
	a := &AdminAction{
		Action:    action,
		Topic:     topic,
		Order:	   order,
		Channel:   channel,
		Node:      node,
		Timestamp: time.Now().Unix(),
		RemoteIP:  req.RemoteAddr,
		UserAgent: req.UserAgent(),
		URL:       u.String(),
		Via:       via,
	}
	user, _ := s.getExistingUserInfo(req)
	if user != nil && user.IsLogin() {
		a.User = user.GetUserName()
	} else {
		a.User = basicAuthUser(req)
	}
	// access log
	s.ctx.nsqadmin.logf("ACCESS: %v", a.String())
	if s.ctx.nsqadmin.opts.NotificationHTTPEndpoint == "" {
		return
	}

	// Perform all work in a new goroutine so this never blocks
	go func() { s.ctx.nsqadmin.notifications <- a }()
}

func (s *httpServer) notifyAdminAction(action, topic, channel, node string, req *http.Request) {
	if s.ctx.nsqadmin.opts.NotificationHTTPEndpoint == "" {
		return
	}
	via, _ := os.Hostname()

	u := url.URL{
		Scheme:   "http",
		Host:     req.Host,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
	}
	if req.TLS != nil || req.Header.Get("X-Scheme") == "https" {
		u.Scheme = "https"
	}

	a := &AdminAction{
		Action:    action,
		Topic:     topic,
		Channel:   channel,
		Node:      node,
		Timestamp: time.Now().Unix(),
		User:      basicAuthUser(req),
		RemoteIP:  req.RemoteAddr,
		UserAgent: req.UserAgent(),
		URL:       u.String(),
		Via:       via,
	}
	// Perform all work in a new goroutine so this never blocks
	go func() { s.ctx.nsqadmin.notifications <- a }()
}
