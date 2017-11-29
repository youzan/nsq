package topic_migrate_manager

import (
	"sync"
	"fmt"
	"strconv"
	dcc "gitlab.qima-inc.com/wangjian/go-dcc-sdk"
	"github.com/youzan/nsq/internal/log"
	"github.com/youzan/nsq/internal/context"
	"errors"
)

var gLog *log.MigrateLogger
var errorMigrateGuardInitErr = errors.New("fail to initialize topic migrate guard")

type DCCTopicMigrateGuard struct {
	dccClient	*dcc.DccClient
	mc 		*MigrateConfig
	lock		sync.RWMutex
}

/**
Initialize topic guard with configs from DCC, update topic switch map
 */
func (g *DCCTopicMigrateGuard) Init() {
	getReq := &dcc.GetRequest{
		g.mc.App,
		g.mc.Key,
	}
	dcc.SetEnv(g.mc.Env)
	gLog.Info("initialize DCC request with config request {app:%v, key:%v, env:%v}", g.mc.App, g.mc.Key, g.mc.Env)
	v, err := g.dccClient.Get([]*dcc.GetRequest{getReq}, func(resp *dcc.Response) {
		gLog.Info("Topic switches updated.")
		g.updateTopicSwitches(resp)
	})
	if err != nil {
		gLog.Error("fail to initialize topic migrate switches with DCC config request {app:%v, key:%v, env:%v}. Err %v", g.mc.App, g.mc.Key, g.mc.Env, err)
		msg := fmt.Sprintf("fail to connect to dcc remote: %v", err)
		panic(msg)
	}
	g.updateTopicSwitches(v)
}

func (g *DCCTopicMigrateGuard) updateTopicSwitches(resp *dcc.Response) {
	if len(resp.RspList) <= 0 {
		gLog.Warn("no topic migrate switchs found.")
		return
	}
	g.lock.Lock()
	defer g.lock.Unlock()
	for _, val := range resp.RspList[0].CombVal {
		topic := val.Key
		s, err := strconv.ParseInt(val.Value, 10, 0)
		if err != nil {
			gLog.Error("fail to parse topic switch %v:%v", topic, val.Value)
		}
		gLog.Debug("Update switch %v:%v", topic, s)
		if g.mc.Switches[topic] != int(s) {
			gLog.Debug("Update switch %v:%v", topic, s)
			g.mc.Switches[topic] = int(s)
		}
	}
}

func (g *DCCTopicMigrateGuard) GetTopicSwitch(topic string) int {
	g.lock.RLock()
	defer g.lock.RUnlock()
	return g.mc.Switches[topic]
}

func (g *DCCTopicMigrateGuard) UpdateTopicSwitches(switches map[string]int) int {
	gLog.Error("pls use dcc interface to update topic migrate switches for app:%v, key:%v, env:%v", g.mc.App, g.mc.Key, g.mc.Key)
	return 0;
}

func NewDCCTopicMigrateGuard(context *context.Context) (ITopicMigrateGuard, error) {
	mc, err := NewMigrateConfig(context)
	if err != nil {
		return nil, errorMigrateGuardInitErr
	}
	dccClient := dcc.NewDccClient(context.DccUrl, context.DccBackupFile, "nsq_migrate")
	guard := &DCCTopicMigrateGuard{
		dccClient: dccClient,
		mc:	mc,
	}
	return guard, nil
}
