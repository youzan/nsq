package engine

import (
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/nsq/internal/levellogger"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		SetLogLevel(int32(levellogger.LOG_DETAIL))
	}
	ret := m.Run()
	os.Exit(ret)
}

func TestPebbleEngCheckpointData(t *testing.T) {
	testCheckpointData(t, "pebble")
}

func testCheckpointData(t *testing.T, engType string) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "checkpoint_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	ck, err := eng.NewCheckpoint(false)
	assert.Nil(t, err)
	// test save should not block, so lastTs should be updated soon
	ckpath := path.Join(tmpDir, "newCk")
	os.MkdirAll(ckpath, DIR_PERM)
	// since the open engine will  add rocksdb as subdir, we save it to the right place
	err = ck.Save(path.Join(ckpath, engType), make(chan struct{}))
	assert.Nil(t, err)

	wb := eng.DefaultWriteBatch()
	knum := 3
	for j := 0; j < knum; j++ {
		wb.Put([]byte("test"+strconv.Itoa(j)), []byte("test"+strconv.Itoa(j)))
	}
	eng.Write(wb)
	wb.Clear()

	ck2, err := eng.NewCheckpoint(false)
	assert.Nil(t, err)
	// test save should not block, so lastTs should be updated soon
	ckpath2 := path.Join(tmpDir, "newCk2")
	os.MkdirAll(ckpath2, DIR_PERM)
	err = ck2.Save(path.Join(ckpath2, engType), make(chan struct{}))
	assert.Nil(t, err)

	cfgCK := *cfg
	cfgCK.DataDir = ckpath
	engCK, err := NewKVEng(&cfgCK)
	assert.Nil(t, err)
	err = engCK.OpenEng()
	assert.Nil(t, err)
	defer engCK.CloseAll()

	cfgCK2 := *cfg
	cfgCK2.DataDir = ckpath2
	engCK2, err := NewKVEng(&cfgCK2)
	assert.Nil(t, err)
	err = engCK2.OpenEng()
	assert.Nil(t, err)
	defer engCK2.CloseAll()

	for j := 0; j < knum; j++ {
		key := []byte("test" + strconv.Itoa(j))
		origV, err := eng.GetBytes(key)
		assert.Equal(t, key, origV)
		v, err := engCK.GetBytes(key)
		assert.Nil(t, err)
		assert.Nil(t, v)
		v2, err := engCK2.GetBytes(key)
		assert.Nil(t, err)
		assert.Equal(t, key, v2)
		assert.Equal(t, origV, v2)
	}

	time.Sleep(time.Second)
}

func TestPebbleEngIterator(t *testing.T) {
	testKVIterator(t, "pebble")
}

func testKVIterator(t *testing.T, engType string) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "iterator_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	wb := eng.NewWriteBatch()
	key := []byte("test")
	wb.Put(key, key)
	eng.Write(wb)
	wb.Clear()
	v, err := eng.GetBytes(key)
	assert.Nil(t, err)
	assert.Equal(t, key, v)
	key2 := []byte("test2")
	wb.Put(key2, key2)
	eng.Write(wb)
	wb.Clear()
	v, err = eng.GetBytes(key2)
	assert.Nil(t, err)
	assert.Equal(t, key2, v)
	key3 := []byte("test3")
	wb.Put(key3, key3)
	eng.Write(wb)
	wb.Clear()
	v, err = eng.GetBytes(key3)
	assert.Nil(t, err)
	assert.Equal(t, key3, v)
	key4 := []byte("test4")
	wb.Put(key4, key4)
	eng.Write(wb)
	wb.Clear()
	v, err = eng.GetBytes(key4)
	assert.Nil(t, err)
	assert.Equal(t, key4, v)
	it, _ := eng.GetIterator(IteratorOpts{})
	defer it.Close()

	// test seek part of prefix
	it.Seek([]byte("tes"))
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Seek(key)
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Seek(key2)
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.Seek(key4)
	assert.True(t, it.Valid())
	assert.Equal(t, key4, it.Key())
	assert.Equal(t, key4, it.Value())
	it.Seek([]byte("test44"))
	assert.True(t, !it.Valid())

	it.SeekToFirst()
	// change value after iterator should not change the snapshot iterator?
	if engType != "mem" {
		// for btree, the write will be blocked while the iterator is open
		// for skiplist, we do not support snapshot
		wb.Put(key4, []byte(string(key4)+"update"))
		eng.Write(wb)
		wb.Clear()
	}

	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())
	it.Prev()
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.SeekToLast()
	assert.True(t, it.Valid())
	assert.Equal(t, key4, it.Key())
	assert.Equal(t, key4, it.Value())
	it.Prev()
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())

	if engType != "pebble" {
		it.SeekForPrev(key3)
		assert.True(t, it.Valid())
		assert.Equal(t, key3, it.Key())
		assert.Equal(t, key3, it.Value())
	}

	it.SeekForPrev([]byte("test5"))
	assert.True(t, it.Valid())
	assert.Equal(t, key4, it.Key())
	assert.Equal(t, key4, it.Value())

	it.SeekForPrev([]byte("test1"))
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Prev()
	assert.True(t, !it.Valid())
}

func TestPebbleEngSnapshotIterator(t *testing.T) {
	testKVSnapshotIterator(t, "pebble")
}

func testKVSnapshotIterator(t *testing.T, engType string) {
	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "iterator_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	wb := eng.NewWriteBatch()
	key := []byte("test")
	wb.Put(key, key)
	key2 := []byte("test2")
	wb.Put(key2, key2)
	key3 := []byte("test3")
	wb.Put(key3, key3)
	eng.Write(wb)
	wb.Clear()

	it, _ := eng.GetIterator(IteratorOpts{})
	defer it.Close()
	// modify after iterator snapshot
	wb = eng.NewWriteBatch()
	wb.Put(key2, []byte("changed"))
	wb.Put(key3, []byte("changed"))
	eng.Write(wb)
	wb.Clear()

	it.Seek(key)
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, key, it.Value())
	it.Seek(key2)
	assert.True(t, it.Valid())
	assert.Equal(t, key2, it.Key())
	assert.Equal(t, key2, it.Value())
	it.Seek(key3)
	assert.True(t, it.Valid())
	assert.Equal(t, key3, it.Key())
	assert.Equal(t, key3, it.Value())

	it2, _ := eng.GetIterator(IteratorOpts{})
	defer it2.Close()

	it2.Seek(key)
	assert.True(t, it2.Valid())
	assert.Equal(t, key, it2.Key())
	assert.Equal(t, key, it2.Value())
	it2.Seek(key2)
	assert.True(t, it2.Valid())
	assert.Equal(t, key2, it2.Key())
	assert.Equal(t, []byte("changed"), it2.Value())
	it2.Seek(key3)
	assert.True(t, it2.Valid())
	assert.Equal(t, key3, it2.Key())
	assert.Equal(t, []byte("changed"), it2.Value())
}

func TestSpecialDataSeekForPebble(t *testing.T) {
	testSpecialDataSeekForAnyType(t, "pebble")
}

func testSpecialDataSeekForAnyType(t *testing.T, engType string) {
	base := []byte{1, 0, 1, 0}
	key := append([]byte{}, base...)
	key2 := append([]byte{}, base...)
	minKey := []byte{1, 0, 1}

	SetLogger(0, nil)
	cfg := NewRockConfig()
	tmpDir, err := ioutil.TempDir("", "iterator_data")
	assert.Nil(t, err)
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	cfg.DataDir = tmpDir
	cfg.EngineType = engType
	eng, err := NewKVEng(cfg)
	assert.Nil(t, err)
	err = eng.OpenEng()
	assert.Nil(t, err)
	defer eng.CloseAll()

	wb := eng.NewWriteBatch()
	value := []byte{1}

	wb.Put(key, value)
	eng.Write(wb)
	wb.Clear()
	key2 = append(key2, []byte{1}...)
	wb.Put(key2, value)
	eng.Write(wb)
	wb.Clear()

	it, _ := eng.GetIterator(IteratorOpts{})
	defer it.Close()
	it.Seek(minKey)
	assert.True(t, it.Valid())
	assert.Equal(t, key, it.Key())
	assert.Equal(t, value, it.Value())
}
