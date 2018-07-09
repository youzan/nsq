package consistence

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/youzan/nsq/internal/test"
	"golang.org/x/net/context"
)

func TestNodeRe(t *testing.T) {
	ClusterID := "test-nsq-cluster-unit-test-etcd-leadership"
	nodeMgr, _ := NewNsqdEtcdMgr(testEtcdServers)
	nodeMgr.InitClusterID(ClusterID)
	ID := "unit-test-etcd-node1"
	nodeInfo := &NsqdNodeInfo{
		ID:      ID,
		NodeIP:  "127.0.0.1",
		TcpPort: "2222",
		RpcPort: "2223",
	}
	err := nodeMgr.RegisterNsqd(nodeInfo)
	test.Nil(t, err)
	time.Sleep(10 * time.Second)
	err = nodeMgr.UnregisterNsqd(nodeInfo)
	test.Nil(t, err)
}

func TestETCDWatch(t *testing.T) {
	client, _ := NewEClient(testEtcdServers)
	watcher := client.Watch("q11", 0, true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()

	for {
		rsp, err := watcher.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				fmt.Println("watch canceled")
				return
			} else {
				time.Sleep(5 * time.Second)
			}
			continue
		}
		fmt.Println(rsp.Action, rsp.Node.Key, rsp.Node.Value)
	}
}

func TestEqualSesssion(t *testing.T) {
	var new TopicLeaderSession
	new.LeaderNode = &NsqdNodeInfo{}
	v, _ := json.Marshal(new)
	var old TopicLeaderSession
	json.Unmarshal(v, &old)
	if *new.LeaderNode != *old.LeaderNode {
		t.Errorf("should equal: %v, %v", new, old)
	}
	if !new.IsSame(&old) {
		t.Errorf("should equal: %v, %v", new, old)
	}
}
