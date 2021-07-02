package nsqdserver

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/go-nsq"
	"github.com/youzan/nsq/internal/statsd"
	"github.com/youzan/nsq/internal/test"
	nsqdNs "github.com/youzan/nsq/nsqd"
)

func readValidate2(t *testing.T, conn io.Reader, f int32, d string) []byte {
	for {
		resp, err := nsq.ReadResponse(conn)
		test.Equal(t, err, nil)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Equal(t, err, nil)

		if d != string(heartbeatBytes) && string(data) == string(heartbeatBytes) {
			continue
		}

		test.Equal(t, frameType, f)
		test.Equal(t, string(data), d)
		return data
	}
}

func TestStats(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	prodconn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer prodconn.Close()
	identify(t, prodconn, nil, frameTypeResponse)

	cmd := nsq.Publish(topicName, []byte("0"))
	test.Nil(t, err)
	cmd.WriteTo(prodconn)
	resp, _ := nsq.ReadResponse(prodconn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, int32(0), frameType)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	stats := nsqd.GetStats(false, false)
	t.Logf("stats: %+v", stats)

	test.Equal(t, len(stats), 1)
	test.Equal(t, 1, len(stats[0].Clients))
	test.Equal(t, int64(1), stats[0].ClientNum)
	test.Equal(t, len(stats[0].Channels), 1)
	test.Equal(t, len(stats[0].Channels[0].Clients), 1)

	stats = nsqd.GetStats(false, true)
	t.Logf("stats: %+v", stats)

	test.Equal(t, len(stats), 1)
	test.Equal(t, len(stats[0].Channels), 1)
	test.Equal(t, len(stats[0].Channels[0].Clients), 0)
	test.Equal(t, stats[0].Channels[0].ClientNum, int64(1))
	test.Equal(t, 0, len(stats[0].Clients))
	test.Equal(t, int64(1), stats[0].ClientNum)
}

func TestClientAttributes(t *testing.T) {
	userAgent := "Test User Agent"

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"snappy":     true,
		"user_agent": userAgent,
	}, frameTypeResponse)
	resp := struct {
		Snappy    bool   `json:"snappy"`
		UserAgent string `json:"user_agent"`
	}{}
	err = json.Unmarshal(data, &resp)
	test.Equal(t, err, nil)
	test.Equal(t, resp.Snappy, true)

	r := snappy.NewReader(conn)
	w := snappy.NewWriter(conn)
	readValidate2(t, r, frameTypeResponse, "OK")

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	sub(t, readWriter{r, w}, topicName, "ch")

	testURL := fmt.Sprintf("http://127.0.0.1:%d/stats?format=json&needClients=true", httpAddr.Port)

	statsData, err := API(testURL)
	test.Equal(t, err, nil)

	client := statsData.Get("topics").GetIndex(0).Get("channels").GetIndex(0).Get("clients").GetIndex(0)
	test.Equal(t, client.Get("user_agent").MustString(), userAgent)
	test.Equal(t, client.Get("snappy").MustBool(), true)
}

func TestStatsConnectFailed(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.StatsdProtocol = "udp"
	opts.StatsdInterval = time.Second * 2
	opts.StatsdAddress = "nsq-monitor.example.com:1234"
	opts.Logger = newTestLogger(t)
	nc := statsd.NewClient(opts.StatsdAddress, opts.StatsdPrefix)
	err := nc.CreateSocket(opts.StatsdProtocol)
	t.Log(err)
	assert.NotNil(t, err)

	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	interval := opts.StatsdInterval
	s := time.Now()
	for {
		stats := nsqd.GetStats(false, false)
		t.Logf("stats: %+v", stats)
		time.Sleep(time.Second)
		if time.Since(s) > interval*2 {
			break
		}
	}
}
