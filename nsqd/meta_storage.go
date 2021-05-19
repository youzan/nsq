package nsqd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/bolt"
	"github.com/twmb/murmur3"
	"github.com/youzan/nsq/internal/util"
)

const (
	metaDataBucket = "meta_data_bucket"
)

var errMetaNotFound = errors.New("meta not found")
var alwaysEnableFileMetaWriter = int32(1)

func SwitchEnableFileMetaWriter(on bool) {
	if on {
		atomic.StoreInt32(&alwaysEnableFileMetaWriter, 1)
	} else {
		atomic.StoreInt32(&alwaysEnableFileMetaWriter, 0)
	}
}

type IMetaStorage interface {
	PersistReader(key string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error
	RetrieveReader(key string) (diskQueueEndInfo, diskQueueEndInfo, error)
	Remove(key string)
	PersistWriter(key string, fsync bool, wend diskQueueEndInfo) error
	RetrieveWriter(key string, readOnly bool) (diskQueueEndInfo, error)
	RemoveWriter(key string)
	LoadChannelMeta(key string) ([]*ChannelMetaInfo, error)
	SaveChannelMeta(key string, fsync bool, channels []*ChannelMetaInfo) error
	RemoveChannelMeta(key string)
	Sync()
	Close()
}

func IsMetaNotFound(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	if err == errMetaNotFound {
		return true
	}
	return false
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

func (fs *fileMetaStorage) RemoveWriter(fileName string) {
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

func (fs *fileMetaStorage) RetrieveWriter(fileName string, readOnly bool) (diskQueueEndInfo, error) {
	var qend diskQueueEndInfo
	var f *os.File
	var err error

	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return qend, err
	}
	defer f.Close()

	var totalCnt int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n",
		&totalCnt,
		&qend.EndOffset.FileNum, &qend.EndOffset.Pos, &qend.virtualEnd)
	if err != nil {
		return qend, err
	}
	atomic.StoreInt64(&qend.totalMsgCnt, totalCnt)
	err = checkMetaFileEnd(f)
	if err != nil && !readOnly {
		// recovery from tmp meta file
		tmpFileName := fmt.Sprintf("%s.tmp", fileName)
		badFileName := fmt.Sprintf("%s.%d.corrupt", fileName, rand.Int())
		nsqLog.Errorf("meta file corrupt to %v, try recover meta from file %v ", badFileName, tmpFileName)
		err2 := util.AtomicRename(fileName, badFileName)
		if err2 != nil {
			nsqLog.Warningf("%v failed rename to %v : %v", fileName, badFileName, err2.Error())
		} else {
			err2 = util.AtomicRename(tmpFileName, fileName)
			if err2 != nil {
				nsqLog.Errorf("%v failed recover to %v : %v", tmpFileName, fileName, err2.Error())
				util.AtomicRename(badFileName, fileName)
			}
		}
	}
	return qend, err
}

func (fs *fileMetaStorage) persistTmpMetaData(fileName string, writeEnd diskQueueEndInfo) error {
	tmpFileName := fmt.Sprintf("%s.tmp", fileName)
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
		atomic.LoadInt64(&writeEnd.totalMsgCnt),
		writeEnd.EndOffset.FileNum, writeEnd.EndOffset.Pos, writeEnd.Offset())

	f.Close()
	return err
}

func (fs *fileMetaStorage) PersistWriter(fileName string, fsync bool, writeEnd diskQueueEndInfo) error {
	fs.metaLock.Lock()
	defer fs.metaLock.Unlock()
	var f *os.File
	var err error
	var n int
	pos := 0
	s := time.Now()
	err = fs.persistTmpMetaData(fileName, writeEnd)
	if err != nil {
		return err
	}

	cost1 := time.Since(s)
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	cost2 := time.Since(s)
	err = preWriteMetaEnd(f)

	cost3 := time.Since(s)
	n, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
		writeEnd.totalMsgCnt,
		writeEnd.EndOffset.FileNum, writeEnd.EndOffset.Pos, writeEnd.Offset())
	pos += n

	_, err = writeMetaEnd(f, err, pos)
	if fsync && err == nil {
		f.Sync()
	}

	f.Close()
	cost4 := time.Since(s)
	if cost4 >= slowCost {
		nsqLog.Logf("writer (%v) meta persist cost: %v,%v,%v,%v", fileName, cost1, cost2, cost3, cost4)
	}
	return err
}

func (fs *fileMetaStorage) SaveChannelMeta(key string, fsync bool, channels []*ChannelMetaInfo) error {
	d, err := json.Marshal(channels)
	if err != nil {
		return err
	}
	fs.metaLock.Lock()
	defer fs.metaLock.Unlock()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", key, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(d)
	if err != nil {
		f.Close()
		return err
	}
	if fsync {
		f.Sync()
	}
	f.Close()
	return util.AtomicRename(tmpFileName, key)
}

func (fs *fileMetaStorage) LoadChannelMeta(key string) ([]*ChannelMetaInfo, error) {
	data, err := ioutil.ReadFile(key)
	if err != nil {
		if !os.IsNotExist(err) {
			nsqLog.LogErrorf("failed to read channel metadata from %s - %s", key, err)
		}
		return nil, err
	}
	channels := make([]*ChannelMetaInfo, 0)
	err = json.Unmarshal(data, &channels)
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return nil, err
	}
	return channels, nil
}

func (fs *fileMetaStorage) RemoveChannelMeta(key string) {
	fs.metaLock.Lock()
	defer fs.metaLock.Unlock()
	os.Remove(key)
}

type dbMetaData struct {
	EndOffset   diskQueueOffset
	VirtualEnd  BackendOffset
	TotalMsgCnt int64
}

type dbMetaStorage struct {
	dataPath string
	db       *bolt.DB
	readOnly bool
	fileMeta *fileMetaStorage
}

func NewDBMetaStorageForRead(p string) (*dbMetaStorage, error) {
	return newDBMetaStorage(p, true)
}

func NewDBMetaStorage(p string) (*dbMetaStorage, error) {
	return newDBMetaStorage(p, false)
}

func newDBMetaStorage(p string, readOnly bool) (*dbMetaStorage, error) {
	ro := &bolt.Options{
		Timeout:      time.Second,
		ReadOnly:     readOnly,
		FreelistType: bolt.FreelistMapType,
	}
	if !readOnly {
		os.MkdirAll(p, 0755)
	}
	db, err := bolt.Open(path.Join(p, "meta.db"), 0644, ro)
	if err != nil {
		nsqLog.LogErrorf("failed to init bolt db meta storage: %v , %v ", p, err)
		return nil, err
	}
	db.NoSync = true
	if !readOnly {
		err = db.Update(func(tx *bolt.Tx) error {
			_, errC := tx.CreateBucketIfNotExists([]byte(metaDataBucket))
			return errC
		})
		if err != nil {
			nsqLog.LogErrorf("failed to init bucket: %v , %v ", p, err)
			return nil, err
		}
	}
	return &dbMetaStorage{
		db:       db,
		dataPath: p,
		fileMeta: &fileMetaStorage{},
		readOnly: readOnly,
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
		nsqLog.Infof("failed to read meta key %v from db: %v , %v ", key, dbs.dataPath, err)
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

func (dbs *dbMetaStorage) RemoveWriter(key string) {
	dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-writer")
		b.Delete(ek)
		return nil
	})
	dbs.fileMeta.RemoveWriter(key)
}

func (dbs *dbMetaStorage) PersistWriter(key string, fsync bool, queueEndInfo diskQueueEndInfo) error {
	var metaQueueEnd dbMetaData
	metaQueueEnd.EndOffset = queueEndInfo.EndOffset
	metaQueueEnd.TotalMsgCnt = queueEndInfo.totalMsgCnt
	metaQueueEnd.VirtualEnd = queueEndInfo.virtualEnd
	err := dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-writer")
		newV, _ := json.Marshal(metaQueueEnd)
		oldV := b.Get(ek)
		exists := oldV != nil
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
	if atomic.LoadInt32(&alwaysEnableFileMetaWriter) == 1 {
		dbs.fileMeta.PersistWriter(key, fsync, queueEndInfo)
	}
	return err
}

func (dbs *dbMetaStorage) RetrieveWriter(key string, readOnly bool) (diskQueueEndInfo, error) {
	var queueEnd diskQueueEndInfo
	var metaEnd dbMetaData
	fallback := false
	err := dbs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-writer")
		v := b.Get(ek)
		if v == nil {
			fallback = true
			return errMetaNotFound
		}
		return json.Unmarshal(v, &metaEnd)
	})
	if err != nil {
		nsqLog.Infof("failed to read meta key %v from db: %v , %v ", key, dbs.dataPath, err)
	}
	if fallback || err != nil {
		nsqLog.Logf("fallback to read meta key %v from file meta", key)
		return dbs.fileMeta.RetrieveWriter(key, readOnly)
	}
	queueEnd.EndOffset = metaEnd.EndOffset
	queueEnd.totalMsgCnt = metaEnd.TotalMsgCnt
	queueEnd.virtualEnd = metaEnd.VirtualEnd
	return queueEnd, err
}

func (dbs *dbMetaStorage) SaveChannelMeta(key string, fsync bool, channels []*ChannelMetaInfo) error {
	err := dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-channelmeta")
		newV, _ := json.Marshal(channels)
		oldV := b.Get(ek)
		exists := oldV != nil
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
		nsqLog.LogErrorf("failed to save meta key %v to db: %v , %v ", key, dbs.dataPath, err)
	}
	if atomic.LoadInt32(&alwaysEnableFileMetaWriter) == 1 {
		dbs.fileMeta.SaveChannelMeta(key, fsync, channels)
	}
	return err
}

func (dbs *dbMetaStorage) LoadChannelMeta(key string) ([]*ChannelMetaInfo, error) {
	fallback := false
	var meta []*ChannelMetaInfo
	err := dbs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-channelmeta")
		v := b.Get(ek)
		if v == nil {
			fallback = true
			return errMetaNotFound
		}
		return json.Unmarshal(v, &meta)
	})
	if err != nil {
		nsqLog.Infof("failed to read meta key %v from db: %v , %v ", key, dbs.dataPath, err)
	}
	if fallback || err != nil {
		nsqLog.Logf("fallback to read meta key %v from file meta", key)
		return dbs.fileMeta.LoadChannelMeta(key)
	}
	return meta, err
}

func (dbs *dbMetaStorage) RemoveChannelMeta(key string) {
	dbs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaDataBucket))
		ek := []byte(key + "-channelmeta")
		b.Delete(ek)
		return nil
	})
	dbs.fileMeta.RemoveChannelMeta(key)
}

func (dbs *dbMetaStorage) Sync() {
	if dbs.readOnly {
		return
	}
	if dbs.db != nil {
		dbs.db.Sync()
	}
	dbs.fileMeta.Sync()
}

func (dbs *dbMetaStorage) Close() {
	if dbs.db != nil {
		if !dbs.readOnly {
			dbs.db.Sync()
		}
		dbs.db.Close()
	}
	dbs.fileMeta.Close()
}

type shardedDBMetaStorage struct {
	dataPath      string
	dbShards      [8]*dbMetaStorage
	writedbShards [32]*dbMetaStorage
}

func NewShardedDBMetaStorageForRead(p string) (*shardedDBMetaStorage, error) {
	d := &shardedDBMetaStorage{
		dataPath: p,
	}
	var err error
	for i := 0; i < len(d.dbShards); i++ {
		d.dbShards[i], err = NewDBMetaStorageForRead(path.Join(p, strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(d.writedbShards); i++ {
		d.writedbShards[i], err = NewDBMetaStorageForRead(path.Join(p, "writer", strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
	}
	return d, nil
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
	for i := 0; i < len(d.writedbShards); i++ {
		d.writedbShards[i], err = NewDBMetaStorage(path.Join(p, "writer", strconv.Itoa(i)))
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

func (ss *shardedDBMetaStorage) RemoveWriter(key string) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.writedbShards)
	d := ss.writedbShards[h]
	d.RemoveWriter(key)
}

func (ss *shardedDBMetaStorage) PersistReader(key string, fsync bool, confirmed diskQueueEndInfo, queueEndInfo diskQueueEndInfo) error {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.PersistReader(key, fsync, confirmed, queueEndInfo)
}

func (ss *shardedDBMetaStorage) PersistWriter(key string, fsync bool, queueEndInfo diskQueueEndInfo) error {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.writedbShards)
	d := ss.writedbShards[h]
	return d.PersistWriter(key, fsync, queueEndInfo)
}

func (ss *shardedDBMetaStorage) RetrieveWriter(key string, readOnly bool) (diskQueueEndInfo, error) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.writedbShards)
	d := ss.writedbShards[h]
	return d.RetrieveWriter(key, readOnly)
}

func (ss *shardedDBMetaStorage) SaveChannelMeta(key string, fsync bool, channels []*ChannelMetaInfo) error {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.SaveChannelMeta(key, fsync, channels)
}

func (ss *shardedDBMetaStorage) LoadChannelMeta(key string) ([]*ChannelMetaInfo, error) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	return d.LoadChannelMeta(key)
}

func (ss *shardedDBMetaStorage) RemoveChannelMeta(key string) {
	h := int(murmur3.Sum32([]byte(key))) % len(ss.dbShards)
	d := ss.dbShards[h]
	d.RemoveChannelMeta(key)
}

func (ss *shardedDBMetaStorage) Sync() {
	for _, d := range ss.dbShards {
		d.Sync()
	}
	for _, d := range ss.writedbShards {
		d.Sync()
	}
}

func (ss *shardedDBMetaStorage) Close() {
	ss.Sync()
	for _, d := range ss.dbShards {
		d.Close()
	}
	for _, d := range ss.writedbShards {
		d.Close()
	}
}
