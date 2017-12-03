package nsqlookupd_migrate

import (
	"sync"
	"errors"
)

var errorMigrateGuardInitErr = errors.New("fail to initialize topic migrate guard")

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
}

func NewTopicMigrateGuard(context *Context) (ITopicMigrateGuard, error) {
	mc, err := NewMigrateConfig(context)
	if err != nil {
		return nil, errorMigrateGuardInitErr
	}

	guard := &HttpTopicMigrateGuard{
			mc:        mc,
		}

	return guard, nil
}