package nsqadmin

import (
	"encoding/json"
	"strconv"
	"time"
)

type QueryPairs struct {
	FieldName  string `json:"fieldName"`
	FieldValue string `json:"fieldValue"`
}

type IndexFieldsQuery map[string]string

type TraceLogQueryInfo struct {
	Sort         string `json:"sort"`
	AppName      string `json:"appName"`
	LogIndexName string `json:"logIndexName"`
	Level        string `json:"level"`
	Host         string `json:"host"`
	Content      string `json:"content"`
	// 2017-01-01 00:00:00
	StartTime   string `json:"startTime"`
	EndTime     string `json:"endTime"`
	IndexFields string `json:"indexFields"`
	PageNumber  int    `json:"pageNumber"`
	PageSize    int    `json:"pageSize"`
	Type        int    `json:"type"`
}

func NewLogQueryInfo(appName string,
	logIndexName string, span time.Duration, indexFields IndexFieldsQuery, pageCnt int) *TraceLogQueryInfo {
	q := &TraceLogQueryInfo{
		Sort:         "score",
		AppName:      appName,
		LogIndexName: logIndexName,
		Level:        "ALL",
		EndTime:      time.Now().Format("2006-01-02 15:04:05"),
		Type:         1,
	}
	q.StartTime = time.Now().Add(-1 * span).Format("2006-01-02 15:04:05")
	d, _ := json.Marshal(indexFields)
	q.IndexFields = string(d)
	q.PageNumber = 1
	q.PageSize = pageCnt
	return q
}

type TraceLogItemInfo struct {
	MsgID     uint64 `json:"msgid"`
	TraceID   uint64 `json:"traceid"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel"`
	Timestamp int64  `json:"timestamp"`
	Action    string `json:"action"`
}

func (tl TraceLogItemInfo) ToJsJson() TraceLogItemInfoForJs {
	return TraceLogItemInfoForJs{
		MsgID:     strconv.FormatUint(tl.MsgID, 10),
		TraceID:   strconv.FormatUint(tl.TraceID, 10),
		Topic:     tl.Topic,
		Channel:   tl.Channel,
		Timestamp: strconv.FormatInt(tl.Timestamp, 10),
		Action:    tl.Action,
	}
}

type TraceLogItemInfoForJs struct {
	MsgID     string `json:"msgid"`
	TraceID   string `json:"traceid"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel"`
	Timestamp string `json:"timestamp"`
	Action    string `json:"action"`
}

type TraceLogData struct {
	ID       string `json:"id"`
	Time     string `json:"time"`
	Level    string `json:"level"`
	HostIp   string `json:"hostIp"`
	HostName string `json:"hostName"`
	Content  string `json:"content"`
	Extra    string `json:"extra"`
	Extra1   string `json:"extra1"`
	TraceLogItemInfo
	RawMsgData map[string]string `json:"raw_msg_data"`
}
type TraceLogDataForJs struct {
	TraceLogItemInfoForJs
	RawMsgData map[string]string `json:"raw_msg_data"`

}

type TraceLog struct {
	LogDataDtos []TraceLogData `json:"logDataDtos"`
	TotalCount  int            `json:"totalCount"`
}

type TLListT []TraceLogData

func (l TLListT) Len() int {
	return len(l)
}
func (l TLListT) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l TLListT) Less(i, j int) bool {
	return l[i].Timestamp < l[j].Timestamp
}

type TraceLogResp struct {
	Success bool     `json:"success"`
	Code    int      `json:"code"`
	Msg     string   `json:"msg"`
	Data    TraceLog `json:"data"`
}
