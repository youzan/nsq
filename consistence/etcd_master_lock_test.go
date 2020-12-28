package consistence

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type eventRecord struct {
	deleted  int32
	added    int32
	modified int32
	other    int32
}

func processMasterEvents(t *testing.T, er *eventRecord, master Master, leader chan string, stop chan struct{}) {
	for {
		select {
		case e := <-master.GetEventsChan():
			if e.Type == MASTER_ADD || e.Type == MASTER_MODIFY {
				if e.Type == MASTER_ADD {
					atomic.AddInt32(&er.added, 1)
				} else {
					atomic.AddInt32(&er.modified, 1)
				}
				t.Logf("master event type[%d] value[%v].", e.Type, e.Master)
				leader <- e.Master
			} else if e.Type == MASTER_DELETE {
				t.Logf("master event delete.")
				leader <- ""
				atomic.AddInt32(&er.deleted, 1)
			} else {
				t.Logf("master event unknown: %v.", e)
				atomic.AddInt32(&er.other, 1)
			}
		case <-stop:
			master.Stop()
			close(leader)
			return
		}
	}
}

func TestEtcdLockExpiredChanged(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(newTestLogger(t), 4)
	}
	client, err := NewEClient(testEtcdServers, "", "")
	assert.Nil(t, err)
	lockKey := "/NSQ-test-lock/test-nsq-cluster-unit-test-etcd-lock"
	testLockValue1 := "test-lock-value1"
	testLockValue2 := "test-lock-value2"
	er := &eventRecord{}
	er2 := &eventRecord{}
	master := NewMaster(client, lockKey, testLockValue1, 3)
	master2 := NewMaster(client, lockKey, testLockValue2, 3)
	leader := make(chan string, 1)
	leader2 := make(chan string, 1)
	stop := make(chan struct{})
	var l1 string
	go func() {
		for {
			l1 = <-leader
			t.Logf("lock leader changed to : %s in 1", l1)
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	var l2 string
	go func() {
		for {
			l2 = <-leader2
			t.Logf("lock leader changed to : %s in 2", l2)
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	go processMasterEvents(t, er, master, leader, stop)
	go processMasterEvents(t, er2, master2, leader2, stop)
	master.Start()
	master2.Start()
	masterEtcd := master.(*EtcdLock)
	masterEtcd2 := master2.(*EtcdLock)
	atomic.StoreInt32(&masterEtcd.testRefreshPaused, 1)
	atomic.StoreInt32(&masterEtcd2.testRefreshPaused, 1)
	defer atomic.StoreInt32(&masterEtcd.testRefreshPaused, 0)
	defer atomic.StoreInt32(&masterEtcd2.testRefreshPaused, 0)
	// wait ttl expired and leader changed
	time.Sleep(time.Second)
	s := time.Now()
	for {
		if atomic.LoadInt32(&er.deleted)+atomic.LoadInt32(&er2.deleted) > 0 {
			break
		}
		time.Sleep(time.Second)
		if time.Since(s) > time.Minute {
			t.Errorf("timeout waiting leader changed")
			break
		}
	}
	t.Logf("waiting leader changed cost: %s", time.Since(s))
	atomic.StoreInt32(&masterEtcd.testRefreshPaused, 0)
	atomic.StoreInt32(&masterEtcd2.testRefreshPaused, 0)
	time.Sleep(time.Second * 3)
	assert.Equal(t, true, atomic.LoadInt32(&er.added)+atomic.LoadInt32(&er2.added) > 1)

	rsp, err := client.Get(master.GetKey(), false, false)
	assert.Nil(t, err)
	assert.NotEmpty(t, rsp.Node.Value)
	rsp2, err := client.Get(master2.GetKey(), false, false)
	assert.Nil(t, err)
	assert.NotEmpty(t, rsp2.Node.Value)
	assert.Equal(t, rsp.Node.Value, rsp2.Node.Value)

	assert.Equal(t, l1, l2)
	assert.NotEmpty(t, l1, "should have leader")
	close(stop)
	time.Sleep(time.Second * 3)
}
