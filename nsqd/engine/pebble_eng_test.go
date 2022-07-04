package engine

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPebbleCheckpointDuringWrite(t *testing.T) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	eng, err := NewPebbleEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	start := time.Now()
	stopC := make(chan struct{})
	var wg sync.WaitGroup
	lastTs := time.Now().UnixNano()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopC:
				return
			default:
			}
			begin := time.Now()
			ck, err := eng.NewCheckpoint(false)
			assert.Nil(t, err)
			// test save should not block, so lastTs should be updated soon
			ckpath := path.Join(tmpDir, "newCk")
			err = ck.Save(ckpath, make(chan struct{}))
			assert.Nil(t, err)
			atomic.StoreInt64(&lastTs, time.Now().UnixNano())
			if time.Since(begin) > time.Second*5 {
				t.Logf("checkpoint too long: %v, %v", begin, time.Since(begin))
			}
			os.RemoveAll(ckpath)
			time.Sleep(time.Millisecond * 50)
		}
	}()
	bigV := make([]byte, 8000)
	var panicTimer *time.Timer
	for {
		for i := 0; i < 100; i++ {
			wb := eng.DefaultWriteBatch()
			for j := 0; j < 100; j++ {
				wb.Put([]byte("test"+strconv.Itoa(i+j)), []byte("test"+strconv.Itoa(i+j)+string(bigV)))
			}
			eng.Write(wb)
			wb.Clear()
			tn := time.Now().UnixNano()
			if atomic.LoadInt64(&lastTs)+time.Second.Nanoseconds()*30 < tn {
				t.Errorf("failed to wait checkpoint update: %v, %v", atomic.LoadInt64(&lastTs), tn)
				panicTimer = time.AfterFunc(time.Second*10, func() {
					buf := make([]byte, 1024*1024)
					runtime.Stack(buf, true)
					fmt.Printf("%s", buf)
					panic("failed")
				})
				break
			}
			if time.Since(start) > time.Minute/2 {
				break
			}
		}
		if panicTimer != nil {
			break
		}
		time.Sleep(time.Microsecond * 10)
		if time.Since(start) > time.Minute/2 {
			break
		}
	}
	close(stopC)
	t.Log("waiting stop")
	wg.Wait()
	t.Log("waiting stopped")
	if panicTimer != nil {
		panicTimer.Stop()
	}
	time.Sleep(time.Second * 2)
}

func TestPebbleReopenAndCheck(t *testing.T) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	pe, err := NewPebbleEng(cfg)
	err = pe.OpenEng()
	assert.Nil(t, err)
	wb := pe.DefaultWriteBatch()
	wb.Put([]byte("test"), []byte("test"))
	err = pe.Write(wb)
	assert.Nil(t, err)
	wb.Clear()
	ck, _ := pe.NewCheckpoint(false)
	err = ck.Save(path.Join(tmpDir, "cktmp"), make(chan struct{}))
	assert.Nil(t, err)

	err = pe.CheckDBEngForRead(path.Join(tmpDir, "cktmp"))
	assert.Nil(t, err)
	pe.CloseEng()

	pe.OpenEng()
	time.Sleep(time.Second * 10)

	pe.CloseEng()
	pe.OpenEng()

	pe.CloseAll()
	time.Sleep(time.Second)
}

func TestPebbleSharedCacheForMulti(t *testing.T) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = path.Join(tmpDir, "test")
	cfg.UseSharedCache = true
	cfg.SharedConfig = newSharedPebbleConfig(cfg.RockOptions)
	pe, err := NewPebbleEng(cfg)
	assert.Nil(t, err)
	err = pe.OpenEng()
	assert.Nil(t, err)
	defer pe.CloseAll()

	wb := pe.DefaultWriteBatch()
	wb.Put([]byte("test"), []byte("test"))
	err = pe.Write(wb)
	wb.Clear()
	assert.Nil(t, err)

	pe.eng.Flush()

	cfg2 := cfg
	cfg2.DataDir = path.Join(tmpDir, "test2")
	pe2, err := NewPebbleEng(cfg2)
	assert.Nil(t, err)
	err = pe2.OpenEng()
	assert.Nil(t, err)
	assert.Equal(t, pe.opts.Cache, pe2.opts.Cache)
	defer pe2.CloseAll()

	wb2 := pe2.DefaultWriteBatch()
	wb2.Put([]byte("test"), []byte("test2"))
	err = pe2.Write(wb2)
	assert.Nil(t, err)
	wb2.Clear()
	pe2.eng.Flush()

	v1, err := pe.GetBytes([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v1)
	v2, err := pe2.GetBytes([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("test2"), v2)

	wb = pe.DefaultWriteBatch()
	wb.Put([]byte("test"), []byte("test"))
	err = pe.Write(wb)
	assert.Nil(t, err)
	wb.Clear()
	pe.eng.Flush()

	v1, err = pe.GetBytes([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v1)
	v2, err = pe2.GetBytes([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("test2"), v2)

	time.Sleep(time.Second * 10)
}
