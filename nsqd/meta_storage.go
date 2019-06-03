package nsqd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/absolute8511/bolt"
	"github.com/spaolacci/murmur3"
)

const (
	metaDataBucket = "meta_data_bucket"
)

var errMetaNotFound = errors.New("meta not found")

type IMetaStorage interface {
	PersistReader(key string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error
	RetrieveReader(key string) (diskQueueEndInfo, diskQueueEndInfo, error)
	Remove(key string)
	PersistWriter(key string, fsync bool, wend diskQueueEndInfo) error
	RetrieveWriter(key string) (diskQueueEndInfo, error)
	Sync()
	Close()
}

type fileMetaStorage struct {
	metaLock sync.Mutex
}

func checkMetaFileEnd(f *os.File) error {
	endFlag := make([]byte, len(diskMagicEndBytes))
	n, err := f.Read(endFlag)
	if err != nil {
		if err == io.EOF {
			// old meta file
		} else {
			nsqLog.Errorf("reader meta end error, need fix: %v", err.Error())
			return errInvalidMetaFileData
		}
	} else if !bytes.Equal(endFlag[:n], diskMagicEndBytes) {
		nsqLog.Errorf("reader meta end data invalid: %v need fix: %v", n, endFlag)
		return errInvalidMetaFileData
	}
	return nil
}

func (fs *fileMetaStorage) Sync() {
}

func (fs *fileMetaStorage) Close() {
}

// retrieveMetaData initializes state from the filesystem
func (fs *fileMetaStorage) RetrieveReader(fileName string) (diskQueueEndInfo, diskQueueEndInfo, error) {
	var f *os.File
	var err error

	var confirmed diskQueueEndInfo
	var queueEnd diskQueueEndInfo
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return confirmed, queueEnd, err
	}
	defer f.Close()
	_, err = fmt.Fscanf(f, "%d\n%d\n%d,%d,%d\n%d,%d,%d\n",
		&confirmed.totalMsgCnt,
		&queueEnd.totalMsgCnt,
		&confirmed.EndOffset.FileNum, &confirmed.EndOffset.Pos, &confirmed.virtualEnd,
		&queueEnd.EndOffset.FileNum, &queueEnd.EndOffset.Pos, &queueEnd.virtualEnd)
	if err != nil {
		nsqLog.Infof("fscanf new meta file err : %v", err)
		return confirmed, queueEnd, err
	}
	err = checkMetaFileEnd(f)
	if err != nil {
		nsqLog.Errorf("reader (%v) meta invalid, need fix: %v", fileName, err.Error())
	}
	return confirmed, queueEnd, err
}

func preWriteMetaEnd(f *os.File) error {
	// error can be ignored since we just make sure end with non-magic
	f.Seek(-1*int64(len(diskMagicEndBytes)), os.SEEK_END)
	_, err := f.Write(make([]byte, len(diskMagicEndBytes)))
	if err != nil {
		return err
	}
	_, err = f.Seek(0, os.SEEK_SET)
	return err
}

func writeMeta(f *os.File, perr error, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) (int, error) {
	if perr != nil {
		return 0, perr
	}
	n, err := fmt.Fprintf(f, "%d\n%d\n%d,%d,%d\n%d,%d,%d\n",
		confirmed.TotalMsgCnt(),
		queueEndInfo.totalMsgCnt,
		confirmed.EndOffset.FileNum, confirmed.EndOffset.Pos, confirmed.Offset(),
		queueEndInfo.EndOffset.FileNum, queueEndInfo.EndOffset.Pos, queueEndInfo.Offset())
	return n, err
}

func writeMetaEnd(f *os.File, perr error, pos int) (int, error) {
	if perr != nil {
		return 0, perr
	}
	// write magic end
	n, err := f.Write(diskMagicEndBytes)
	pos += n
	f.Truncate(int64(pos))
	return n, err
}

func (fs *fileMetaStorage) Remove(fileName string) {
	fs.metaLock.Lock()
	defer fs.metaLock.Unlock()
	os.Remove(fileName)
}

// persistMetaData atomically writes state to the filesystem
func (fs *fileMetaStorage) PersistReader(fileName string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error {
	var f *os.File
	var err error
	var n int
	pos := 0

	s := time.Now()
	fs.metaLock.Lock()
	defer fs.metaLock.Unlock()
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	cost1 := time.Since(s)
	err = preWriteMetaEnd(f)

	if testCrash {
		return errors.New("test crash")
	}
	cost2 := time.Since(s)
	n, err = writeMeta(f, err, confirmed, queueEndInfo)
	pos += n
	_, err = writeMetaEnd(f, err, pos)

	if err != nil {
		nsqLog.Errorf("reader (%v) meta write failed, need fix: %v", fileName, err.Error())
	} else if fsync {
		f.Sync()
	}
	f.Close()
	cost3 := time.Since(s)
	if cost3 >= slowCost {
		nsqLog.Logf("reader (%v) meta persist cost: %v,%v,%v", fileName, cost1, cost2, cost3)
	}
	return err
}

func (fs *fileMetaStorage) RetrieveWriter(fileName string) (diskQueueEndInfo, error) {
	var queueEnd diskQueueEndInfo
	return queueEnd, nil
}

func (fs *fileMetaStorage) PersistWriter(fileName string, fsync bool, queueEndInfo diskQueueEndInfo) error {
	return nil
}

type dbMetaData struct {
	EndOffset   diskQueueOffset
	VirtualEnd  BackendOffset
	TotalMsgCnt int64
}

type dbMetaStorage struct {
	dataPath string
	db       *bolt.DB
	fileMeta *fileMetaStorage
}

func NewDBMetaStorage(p string) (*dbMetaStorage, error) {
	ro := &bolt.Options{
		Timeout:  time.Second,
		ReadOnly: false,
	}
	os.MkdirAll(p, 0755)
	db, err := bolt.Open(path.Join(p, "meta.db"), 0644, ro)
	if err != nil {
		nsqLog.LogErrorf("failed to init bolt db meta storage: %v , %v ", p, err)
		return nil, err
	}
	db.NoSync = true
	err = db.Update(func(tx *bolt.Tx) error {
		_, errC := tx.CreateBucketIfNotExists([]byte(metaDataBucket))
		return errC
	})
	if err != nil {
		nsqLog.LogErrorf("failed to init bucket: %v , %v ", p, err)
		return nil, err
	}
	return &dbMetaStorage{
		db:       db,
		dataPath: p,
		fileMeta: &fileMetaStorage{},
	}, nil
}

func (dbs *dbMetaStorage) PersistReader(key string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error {
	var metaConfirmed dbMetaData
	metaConfirmed.EndOffset = confirmed.EndOffset
	metaConfirmed.VirtualEnd = confirmed.virtualEnd
	metaConfirmed.TotalMsgCnt = confirmed.totalMsgCnt
	var metaQueueEnd dbMetaData
	metaQueueEnd.EndOffset = queueEndInfo.EndOffset
	metaQueueEnd.TotalMsgCnt = queueEndInfo.totalMsgCnt
	metaQueueEnd.VirtualEnd = queueEndInfo.virtualEnd
	err := dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		newV, _ := json.Marshal(metaConfirmed)
		ck := []byte(key + "-confirmed")
		oldV := b.Get(ck)
		exists := oldV != nil
		if exists && bytes.Equal(oldV, newV) {
		} else {
			err := b.Put(ck, newV)
			if err != nil {
				return err
			}
		}
		ek := []byte(key + "-end")
		newV, _ = json.Marshal(metaQueueEnd)
		oldV = b.Get(ek)
		exists = oldV != nil
		if exists && bytes.Equal(oldV, newV) {
		} else {
			err := b.Put(ek, newV)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if fsync && err == nil {
		dbs.Sync()
	}
	if err != nil {
		nsqLog.LogErrorf("failed to save meta key %v from db: %v , %v ", key, dbs.dataPath, err)
	}
	return err
}

func (dbs *dbMetaStorage) Remove(key string) {
	dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ck := []byte(key + "-confirmed")
		b.Delete(ck)
		ek := []byte(key + "-end")
		b.Delete(ek)
		return nil
	})
	dbs.fileMeta.Remove(key)
}

func (dbs *dbMetaStorage) RetrieveReader(key string) (diskQueueEndInfo, diskQueueEndInfo, error) {
	var confirmed diskQueueEndInfo
	var queueEnd diskQueueEndInfo
	var metaConfirmed dbMetaData
	var metaEnd dbMetaData
	fallback := false
	err := dbs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ck := []byte(key + "-confirmed")
		ek := []byte(key + "-end")
		v := b.Get(ck)
		v2 := b.Get(ek)
		if v == nil && v2 == nil {
			fallback = true
			return errMetaNotFound
		}
		err := json.Unmarshal(v, &metaConfirmed)
		if err != nil {
			return err
		}
		err = json.Unmarshal(v2, &metaEnd)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		nsqLog.LogErrorf("failed to read meta key %v from db: %v , %v ", key, dbs.dataPath, err)
	}
	if fallback || err != nil {
		nsqLog.Logf("fallback to read meta key %v from file meta", key)
		return dbs.fileMeta.RetrieveReader(key)
	}
	confirmed.EndOffset = metaConfirmed.EndOffset
	confirmed.totalMsgCnt = metaConfirmed.TotalMsgCnt
	confirmed.virtualEnd = metaConfirmed.VirtualEnd
	queueEnd.EndOffset = metaEnd.EndOffset
	queueEnd.totalMsgCnt = metaEnd.TotalMsgCnt
	queueEnd.virtualEnd = metaEnd.VirtualEnd

	return confirmed, queueEnd, err
}

func (dbs *dbMetaStorage) PersistWriter(key string, fsync bool, queueEndInfo diskQueueEndInfo) error {
	return nil
}

func (dbs *dbMetaStorage) RetrieveWriter(key string) (diskQueueEndInfo, error) {
	var queueEnd diskQueueEndInfo
	return queueEnd, nil
}

func (dbs *dbMetaStorage) Sync() {
	if dbs.db != nil {
		dbs.db.Sync()
	}
	dbs.fileMeta.Sync()
}

func (dbs *dbMetaStorage) Close() {
	if dbs.db != nil {
		dbs.db.Sync()
		dbs.db.Close()
	}
	dbs.fileMeta.Close()
}

type shardedDBMetaStorage struct {
	dataPath string
	dbShards [8]*dbMetaStorage
}

func NewShardedDBMetaStorage(p string) (*shardedDBMetaStorage, error) {
	d := &shardedDBMetaStorage{
		dataPath: p,
	}
	var err error
	for i := 0; i < len(d.dbShards); i++ {
		d.dbShards[i], err = NewDBMetaStorage(path.Join(p, strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (ss *shardedDBMetaStorage) RetrieveReader(key string) (diskQueueEndInfo, diskQueueEndInfo, error) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.RetrieveReader(key)
}

func (ss *shardedDBMetaStorage) Remove(key string) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	d.Remove(key)
}

func (ss *shardedDBMetaStorage) PersistReader(key string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.PersistReader(key, fsync, confirmed, queueEndInfo)
}

func (ss *shardedDBMetaStorage) PersistWriter(key string, fsync bool, queueEndInfo diskQueueEndInfo) error {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.PersistWriter(key, fsync, queueEndInfo)
}

func (ss *shardedDBMetaStorage) RetrieveWriter(key string) (diskQueueEndInfo, error) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.RetrieveWriter(key)
}

func (ss *shardedDBMetaStorage) Sync() {
	for _, d := range ss.dbShards {
		d.Sync()
	}
}

func (ss *shardedDBMetaStorage) Close() {
	ss.Sync()
	for _, d := range ss.dbShards {
		d.Close()
	}
}
