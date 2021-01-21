// description: Utility to perform master election/failover using etcd.
package consistence

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type EVENT_TYPE int

const (
	MASTER_ADD EVENT_TYPE = iota
	MASTER_DELETE
	MASTER_MODIFY
	MASTER_ERROR
)

const (
	RETRY_SLEEP = 200
)

type MasterEvent struct {
	Type          EVENT_TYPE
	Master        string
	ModifiedIndex uint64
}

type Master interface {
	Start()
	Stop()
	GetEventsChan() <-chan *MasterEvent
	GetKey() string
	GetMaster() string
}

type EtcdLock struct {
	sync.Mutex

	client             *EtcdClient
	name               string
	id                 string
	ttl                uint64
	enable             bool
	master             string
	watchStopChan      chan bool
	eventsChan         chan *MasterEvent
	stoppedChan        chan bool
	refreshStoppedChan chan bool
	ifHolding          bool
	modifiedIndex      uint64
	testRefreshPaused  int32
}

func NewMaster(etcdClient *EtcdClient, name, value string, ttl uint64) Master {
	return &EtcdLock{
		client:             etcdClient,
		name:               name,
		id:                 value,
		ttl:                ttl,
		enable:             false,
		master:             "",
		watchStopChan:      make(chan bool, 1),
		eventsChan:         make(chan *MasterEvent, 1),
		stoppedChan:        make(chan bool, 1),
		refreshStoppedChan: make(chan bool, 1),
		ifHolding:          false,
		modifiedIndex:      0,
	}
}

func (self *EtcdLock) Start() {
	coordLog.Infof("[EtcdLock][Start] start to acquire lock[%s] value[%s].", self.name, self.id)
	self.Lock()
	if self.enable {
		self.Unlock()
		return
	}
	self.enable = true
	self.Unlock()

	go func() {
		for {
			err := self.acquire()
			if err == nil {
				break
			}
		}
	}()
}

func (self *EtcdLock) Stop() {
	coordLog.Infof("[EtcdLock][Stop] stop acquire lock[%s].", self.name)
	self.Lock()
	if !self.enable {
		self.Unlock()
		return
	}
	self.enable = false
	self.Unlock()

	self.watchStopChan <- true
	// wait for acquire to finish
	<-self.stoppedChan
}

func (self *EtcdLock) GetEventsChan() <-chan *MasterEvent {
	return self.eventsChan
}

func (self *EtcdLock) GetKey() string {
	return self.name
}

func (self *EtcdLock) GetMaster() string {
	self.Lock()
	defer self.Unlock()
	return self.master
}

func (self *EtcdLock) acquire() (ret error) {
	defer func() {
		if r := recover(); r != nil {
			callers := ""
			for i := 0; true; i++ {
				_, file, line, ok := runtime.Caller(i)
				if !ok {
					break
				}
				callers = callers + fmt.Sprintf("%v:%v\n", file, line)
			}
			errMsg := fmt.Sprintf("[EtcdLock][acquire] Recovered from panic: %#v (%v)\n%v", r, r, callers)
			coordLog.Errorf(errMsg)
			ret = errors.New(errMsg)
		}
	}()

	var rsp *client.Response
	err := fmt.Errorf("Dummy error.")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-self.watchStopChan:
			cancel()
		}
	}()

	for {
		if !self.enable {
			self.stopAcquire()
			break
		}

		if err != nil || rsp == nil || rsp.Node == nil || rsp.Node.Value == "" {
			rsp, err = self.client.Get(self.name, false, false)
			if err != nil {
				if client.IsKeyNotFound(err) {
					coordLog.Infof("[EtcdLock][acquire] try to acquire lock[%s]", self.name)
					rsp, err = self.client.Create(self.name, self.id, self.ttl)
					if err != nil {
						coordLog.Errorf("[EtcdLock][acquire] etcd create lock[%s] error: %s", self.name, err.Error())
						continue
					}
				} else {
					coordLog.Errorf("[EtcdLock][acquire] etcd get lock[%s] error: %s", self.name, err.Error())
					time.Sleep(RETRY_SLEEP * time.Millisecond)
					continue
				}
			}
		}

		self.processEtcdRsp(rsp)

		self.Lock()
		self.master = rsp.Node.Value
		self.modifiedIndex = rsp.Node.ModifiedIndex
		self.Unlock()

		// normally it should use modifiedIndex, while for error index is outdated and cleared,
		// we should use cluster index instead (anyway we should use the larger one)
		wi := rsp.Node.ModifiedIndex
		if rsp.Index > wi {
			wi = rsp.Index
			coordLog.Infof("[EtcdLock] watch lock[%s] at cluster index: %v, modify index: %v", self.name, rsp.Index, rsp.Node.ModifiedIndex)
		}
		// to avoid dead connection issues, we add timeout for watch connection to wake up watch if too long no
		// any event
		coordLog.Debugf("[EtcdLock] begin watch lock[%s] %v", self.name, rsp.Index)
		// watch for v2 client should not +1 on index, since it is the after index (which will +1 in the method of watch)
		watcher := self.client.Watch(self.name, wi, false)
		rsp, err = watcher.Next(ctx)
		coordLog.Debugf("[EtcdLock] watch event lock[%s] %v", self.name, rsp)
		if err != nil {
			if err == context.Canceled {
				coordLog.Infof("[EtcdLock][acquire] watch lock[%s] stop by user.", self.name)
			} else if err == context.DeadlineExceeded {
				coordLog.Infof("[EtcdLock][acquire] watch lock[%s] timeout.", self.name)
			} else {
				coordLog.Warningf("[EtcdLock][acquire] failed to watch lock[%s] error: %s", self.name, err.Error())
			}
		}
	}

	return nil
}

func (self *EtcdLock) processEtcdRsp(rsp *client.Response) {
	if rsp.Node.Value == self.id {
		if !self.ifHolding {
			coordLog.Infof("[EtcdLock][processEtcdRsp] acquire lock: %s", self.name)
			self.ifHolding = true
			self.eventsChan <- &MasterEvent{Type: MASTER_ADD, Master: self.id, ModifiedIndex: rsp.Node.ModifiedIndex}
			go self.refresh()
		}
	} else {
		if self.ifHolding {
			coordLog.Errorf("[EtcdLock][processEtcdRsp] lost lock: %s", self.name)
			self.ifHolding = false
			self.refreshStoppedChan <- true
			self.eventsChan <- &MasterEvent{Type: MASTER_DELETE}
		}
		if self.master != rsp.Node.Value {
			coordLog.Infof("[EtcdLock][processEtcdRsp] modify lock[%s] to master[%s]", self.name, rsp.Node.Value)
			self.eventsChan <- &MasterEvent{Type: MASTER_MODIFY, Master: rsp.Node.Value, ModifiedIndex: rsp.Node.ModifiedIndex}
		}
	}
}

func (self *EtcdLock) stopAcquire() {
	if self.ifHolding {
		coordLog.Infof("[EtcdLock][stopAcquire] delete lock: %s", self.name)
		_, err := self.client.Delete(self.name, false)
		if err != nil {
			coordLog.Errorf("[EtcdLock][stopAcquire] failed to delete lock: %s error: %s", self.name, err.Error())
		}
		self.ifHolding = false
		self.refreshStoppedChan <- true
	}
	self.Lock()
	self.master = ""
	self.Unlock()
	self.stoppedChan <- true
}

func (self *EtcdLock) refresh() {
	ticker := time.NewTicker(time.Second * time.Duration(self.ttl*4/10))
	defer ticker.Stop()
	for {
		select {
		case <-self.refreshStoppedChan:
			coordLog.Infof("[EtcdLock][refresh] Stopping refresh for lock %s", self.name)
			return
		case <-ticker.C:
			if atomic.LoadInt32(&self.testRefreshPaused) == 1 {
				continue
			}
			self.Lock()
			modify := self.modifiedIndex
			self.Unlock()
			rsp, err := self.client.CompareAndSwap(self.name, self.id, self.ttl, self.id, modify)
			if err != nil {
				coordLog.Errorf("[EtcdLock][refresh] Failed to set ttl for lock[%s] error:%s", self.name, err.Error())
				rsp, err = self.client.Get(self.name, false, false)
				if err != nil {
					if client.IsKeyNotFound(err) {
						coordLog.Warningf("[EtcdLock] lock[%s] missing", self.name)
					}
				} else if rsp != nil && rsp.Node != nil {
					coordLog.Warningf("[EtcdLock] lock[%s] current:%s, %v", self.name, rsp.Node.Value, rsp.Index)
				}
			} else {
				self.Lock()
				self.modifiedIndex = rsp.Node.ModifiedIndex
				self.Unlock()
			}
		}
	}
}
