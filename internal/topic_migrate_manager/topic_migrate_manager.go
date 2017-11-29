package topic_migrate_manager

import (
	"sync"
	"github.com/youzan/nsq/internal/context"
)

type ITopicMigrateGuard interface {
	GetTopicSwitch(topic string) int
	UpdateTopicSwitches(switches map[string]int) int
	Init()
}

type HttpTopicMigrateGuard struct {
	mc 		*MigrateConfig
	lock		sync.RWMutex
}

func (g *HttpTopicMigrateGuard) GetTopicSwitch(topic string) int {
	g.lock.RLock()
	defer g.lock.RUnlock()
	return g.mc.Switches[topic]
}

func (g *HttpTopicMigrateGuard) UpdateTopicSwitches(switches map[string]int) int {
	g.lock.Lock()
	defer g.lock.Unlock()
	var cnt int
	for topic, sw := range switches {
		g.mc.Switches[topic] = sw
		cnt++
	}
	return cnt
}

func (g *HttpTopicMigrateGuard) Init() {
	gLog.Info("http topic migrate guard inited.")
}




func NewTopicMigrateGuard(context *context.Context) (ITopicMigrateGuard, error) {
	gLog = context.Logger
	mc, err := NewMigrateConfig(context)
	if err != nil {
		return nil, errorMigrateGuardInitErr
	}

	var guard ITopicMigrateGuard
	if context.MigrateGuard == "http" {
		guard = &HttpTopicMigrateGuard{
			mc:        mc,
		}
	} else {
		guard, err = NewDCCTopicMigrateGuard(context)
		if(nil != err) {
			return nil, err
		}
		//log
		gLog.Info("topic migrate guard initialized with url:%v, backupFile:%v, env:%v", context.DccUrl, context.DccBackupFile, context.Env)
	}
	return guard, nil
}