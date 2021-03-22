package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"path"
	"sort"
	"time"

	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/clusterinfo"
	"github.com/youzan/nsq/internal/http_api"
	"github.com/youzan/nsq/internal/levellogger"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqd"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic              = flag.String("topic", "", "NSQ topic")
	partition          = flag.Int("partition", -1, "NSQ topic partition")
	channelName        = flag.String("channel_name", "", "name for channel")
	dataPath           = flag.String("data_path", "", "the data path of nsqd")
	srcNsqLookupd      = flag.String("src_nsqlookupd", "", "")
	dstNsqLookupd      = flag.String("dest_nsqlookupd", "", "")
	view               = flag.String("view", "commitlog", "commitlog | topicdata | delayedqueue | channelstats")
	searchMode         = flag.String("search_mode", "count", "the view start of mode. (count|id|timestamp|virtual_offset|check_channels|check_topics)")
	viewStart          = flag.Int64("view_start", 0, "the start count of message.")
	viewStartID        = flag.Int64("view_start_id", 0, "the start id of message.")
	viewStartTimestamp = flag.Int64("view_start_timestamp", 0, "the start timestamp of message.")
	viewOffset         = flag.Int64("view_offset", 0, "the virtual offset of the queue")
	viewCnt            = flag.Int("view_cnt", 1, "the total count need to be viewed. should less than 1,000,000")
	viewCh             = flag.String("view_channel", "", "channel detail need to view")
	logLevel           = flag.Int("level", 3, "log level")
	//TODO: add ext ver for decode message
	isExt = flag.Bool("ext", false, "is there extension for message ")
)

func getBackendName(topicName string, part int) string {
	backendName := nsqd.GetTopicFullName(topicName, part)
	return backendName
}

type sortCnt struct {
	Name string
	Cnt  int
}

type sortCntListT []sortCnt

func (s sortCntListT) Len() int {
	return len(s)
}

func (s sortCntListT) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortCntListT) Less(i, j int) bool {
	if s[i].Cnt == s[j].Cnt {
		return s[i].Name < s[j].Name
	}
	return s[i].Cnt < s[j].Cnt
}

func checkTopicStats() {
	lsrc := clusterinfo.LookupdAddressDC{
		DC:   "",
		Addr: *srcNsqLookupd,
	}

	client := http_api.NewClient(nil)
	ci := clusterinfo.New(nil, client)
	srcnodes, err := ci.GetLookupdProducers([]clusterinfo.LookupdAddressDC{lsrc})
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}
	producers, _, err := ci.GetNSQDStatsWithClients(srcnodes, "", "", true)
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}

	topicConns := make(map[string]int)
	clientConns := make(map[string]int)
	for _, p := range producers {
		for _, c := range p.Clients {
			host, _, _ := net.SplitHostPort(c.RemoteAddress)
			clientConns[host] = clientConns[host] + 1
		}
		topicConns[p.TopicName] += topicConns[p.TopicName] + len(p.Clients)
	}

	topicList := make(sortCntListT, 0)
	for t, cnt := range topicConns {
		topicList = append(topicList, sortCnt{Name: t, Cnt: cnt})
	}
	clientList := make(sortCntListT, 0)
	for c, cnt := range clientConns {
		clientList = append(clientList, sortCnt{Name: c, Cnt: cnt})
	}
	sort.Sort(sort.Reverse(topicList))
	sort.Sort(sort.Reverse(clientList))
	for i, t := range topicList {
		fmt.Printf("topn producers topic: %v\n", t)
		if i > 100 {
			break
		}
	}
	for i, c := range clientList {
		fmt.Printf("topn client conn: %v\n", c)
		if i > 100 {
			break
		}
	}
}

func checkChannelStats() {
	lsrc := clusterinfo.LookupdAddressDC{
		DC:   "",
		Addr: *srcNsqLookupd,
	}

	client := http_api.NewClient(nil)
	ci := clusterinfo.New(nil, client)
	srcnodes, err := ci.GetLookupdProducers([]clusterinfo.LookupdAddressDC{lsrc})
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}
	_, channels, err := ci.GetNSQDStats(srcnodes, "", "", true)
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}
	ldest := clusterinfo.LookupdAddressDC{
		DC:   "",
		Addr: *dstNsqLookupd,
	}

	destnodes, err := ci.GetLookupdProducers([]clusterinfo.LookupdAddressDC{ldest})
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}
	_, channels2, err := ci.GetNSQDStats(destnodes, "", "", true)
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}

	for name, ch := range channels {
		ch2, ok := channels2[name]
		if !ok {
			log.Printf("src channel not found in dst: %v", name)
			continue
		}
		if ch.Skipped == ch2.Skipped && ch.Paused == ch2.Paused {
			continue
		}
		log.Printf("ch %v mismatch, src channel (paused: %v, skipped: %v), dest channel: (%v, %v)\n", name, ch.Paused, ch.Skipped,
			ch2.Paused, ch2.Skipped)
	}
	for name, ch := range channels2 {
		ch2, ok := channels[name]
		if !ok {
			log.Printf("dest channel not found in src: %v", name)
			continue
		}
		if ch.Skipped == ch2.Skipped && ch.Paused == ch2.Paused {
			continue
		}
		log.Printf("ch %v mismatch, src channel (paused: %v, skipped: %v), dest channel: (%v, %v)\n", name, ch2.Paused, ch2.Skipped,
			ch.Paused, ch.Skipped)
	}
}
func metaReaderKey(dataPath string, metaName string) string {
	return fmt.Sprintf(path.Join(dataPath, "%s.diskqueue.meta.v2.reader.dat"),
		metaName)
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_data_tool v%s\n", version.Binary)
		return
	}

	if *searchMode == "check_topics" {
		checkTopicStats()
		return
	}
	if *searchMode == "check_channels" {
		checkChannelStats()
		return
	}
	nsqd.SetLogger(levellogger.NewSimpleLog())
	nsqd.NsqLogger().SetLevel(int32(*logLevel))
	consistence.SetCoordLogger(levellogger.NewSimpleLog(), int32(*logLevel))

	if *topic == "" {
		log.Fatal("--topic is required\n")
	}
	if *partition == -1 {
		log.Fatal("--partition is required")
	}
	if *dataPath == "" {
		log.Fatal("--data_path is required")
	}

	if *viewCnt > 1000000 {
		log.Fatal("--view_cnt is too large")
	}

	topicDataPath := path.Join(*dataPath, *topic)

	backendName := getBackendName(*topic, *partition)
	metaStorage, err := nsqd.NewShardedDBMetaStorageForRead(path.Join(*dataPath, "shared_meta"))
	if err != nil {
		log.Fatalf("init disk writer failed: %v", err)
		return
	}
	backendWriter, err := nsqd.NewDiskQueueWriterForRead(backendName, topicDataPath, 1024*1024*1024, 1, 1024*1024*100, 1, metaStorage)
	if err != nil {
		if *view != "commitlog" {
			log.Fatalf("init disk writer failed: %v", err)
			return
		}
	}
	defer backendWriter.Close()
	log.Printf("disk queue %v-%v meta: %v, %v, %v",
		topicDataPath, backendName,
		backendWriter.GetQueueWriteEnd(), backendWriter.GetQueueReadStart(), backendWriter.GetQueueReadEnd())
	if *view == "delayedqueue" {
		opts := &nsqd.Options{
			MaxBytesPerFile: 1024 * 1024 * 100,
		}
		nsqd.NsqLogger().Infof("checking delayed")
		delayQ, err := nsqd.NewDelayQueueForRead(*topic, *partition, topicDataPath, opts, nil, *isExt)
		if err != nil {
			log.Fatalf("init delayed queue failed: %v", err)
		}
		if *viewStartID != 0 {
			// check if it is in the delayed queue
			// if exist, we need read index to get the delayedts
			// and then scan from that delayed ts to get the actually delayed message
			if !delayQ.IsChannelMessageDelayed(nsqd.MessageID(*viewStartID), *viewCh) {
				nsqd.NsqLogger().Infof("channel %v msg id %v is not in the delayed queue", *viewCh, *viewStartID)
				return
			}
			msg, err := delayQ.FindChannelMessageDelayed(nsqd.MessageID(*viewStartID), *viewCh, true)
			if err != nil {
				nsqd.NsqLogger().Infof("channel %v msg id %v find error: %v", *viewCh, *viewStartID, err.Error())
				return
			}
			if msg == nil {
				nsqd.NsqLogger().Infof("not found msg: %v", *viewStartID)
				return
			}
			nsqd.NsqLogger().Infof("found msg: %v", msg)
			fmt.Printf("%v:%v:%v:%v, header:%v, body: %v\n", msg.ID, msg.TraceID, msg.Timestamp, msg.Attempts(), string(msg.ExtBytes), string(msg.Body))
			return
		}
		recentKeys, cntList, chCntList := delayQ.GetOldestConsumedState([]string{*viewCh}, true)
		for _, k := range recentKeys {
			nsqd.NsqLogger().Infof("delayed recent: %v", k)
		}
		nsqd.NsqLogger().Infof("cnt list: %v", cntList)
		for k, v := range chCntList {
			nsqd.NsqLogger().Infof("channel %v cnt : %v", k, v)
		}
		rets := make([]nsqd.Message, *viewCnt)
		untilTime := time.Now().UnixNano()
		cnt, err := delayQ.PeekRecentChannelTimeout(untilTime, rets, *viewCh)
		if err != nil {
			nsqd.NsqLogger().Infof("peek recent timeout failed: %v", err.Error())
			return
		}
		for _, m := range rets[:cnt] {
			nsqd.NsqLogger().Infof("peeked channel msg : %v", m)
		}

		cnt, _ = delayQ.PeekAll(rets)
		for _, m := range rets[:cnt] {
			nsqd.NsqLogger().Infof("peeked msg : %v", m)
		}
		delayQ.Dump()
		return
	} else if *view == "channelstats" {
		k := metaReaderKey(topicDataPath, backendName+":"+*channelName)
		confirmed, end, err := metaStorage.RetrieveReader(k)
		if err != nil {
			log.Fatalf("meta data error: %v for %v", err, k)
			return
		}
		log.Printf("channel: %v, %v, %v", k, confirmed, end)
		return
	}
	topicCommitLogPath := consistence.GetTopicPartitionBasePath(*dataPath, *topic, *partition)
	tpLogMgr, err := consistence.InitTopicCommitLogMgr(*topic, *partition, topicCommitLogPath, 0)
	if err != nil {
		log.Fatalf("loading commit log %v failed: %v\n", topicCommitLogPath, err)
	}
	logIndex, lastOffset, lastLogData, err := tpLogMgr.GetLastCommitLogOffsetV2()
	if err != nil {
		log.Printf("loading last commit log failed: %v\n", err)
	} else {
		log.Printf("topic last commit log at %v:%v is : %v\n", logIndex, lastOffset, lastLogData)
	}

	// note: since there may be exist group commit. It is not simple to do the direct position of log.
	// we need to search in the ordered log data.
	searchOffset := int64(0)
	searchLogIndexStart := int64(0)
	if *searchMode == "count" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgCnt(*viewStart)
		if err != nil {
			log.Fatalln(err)
		}
	} else if *searchMode == "id" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgID(*viewStartID)
		if err != nil {
			log.Fatalln(err)
		}
	} else if *searchMode == "virtual_offset" {
		searchLogIndexStart, searchOffset, _, err = tpLogMgr.SearchLogDataByMsgOffset(*viewOffset)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		log.Fatalln("not supported search mode")
	}

	logData, err := tpLogMgr.GetCommitLogFromOffsetV2(searchLogIndexStart, searchOffset)
	if err != nil {
		log.Fatalf("topic read at: %v:%v failed: %v\n", searchLogIndexStart, searchOffset, err)
	}
	log.Printf("topic read at: %v:%v, %v\n", searchLogIndexStart, searchOffset, logData)

	if *view == "commitlog" {
		logs, err := tpLogMgr.GetCommitLogsV2(searchLogIndexStart, searchOffset, int(*viewCnt))
		if err != nil {
			if err != consistence.ErrCommitLogEOF {
				log.Fatalf("get logs failed: %v", err)
				return
			}
		}
		for _, l := range logs {
			fmt.Println(l)
		}
	} else if *view == "topicdata" {
		queueOffset := logData.MsgOffset
		if *searchMode == "virtual_offset" {
			if queueOffset != *viewOffset {
				queueOffset = *viewOffset
				log.Printf("search virtual offset not the same : %v, %v\n", logData.MsgOffset, *viewOffset)
			}
		}
		backendReader := nsqd.NewDiskQueueSnapshot(backendName, topicDataPath, backendWriter.GetQueueReadEnd())
		backendReader.SetQueueStart(backendWriter.GetQueueReadStart())
		if queueOffset == 0 {
			backendReader.SeekTo(nsqd.BackendOffset(queueOffset), 0)
		} else {
			backendReader.SeekTo(nsqd.BackendOffset(queueOffset), logData.MsgCnt-1)
		}
		cnt := *viewCnt
		for cnt > 0 {
			cnt--
			ret := backendReader.ReadOne()
			if ret.Err != nil {
				log.Fatalf("read data error: %v", ret)
				return
			}

			fmt.Printf("%v:%v:%v:%v, string: %v\n", ret.Offset, ret.MovedSize, ret.CurCnt, ret.Data, string(ret.Data))
			msg, err := nsqd.DecodeMessage(ret.Data, *isExt)
			if err != nil {
				log.Fatalf("decode data error: %v", err)
				continue
			}
			fmt.Printf("%v:%v:%v:%v, header:%v, body: %v\n", msg.ID, msg.TraceID, msg.Timestamp, msg.Attempts(), string(msg.ExtBytes), string(msg.Body))
		}
	}
}
