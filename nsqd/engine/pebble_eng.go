package engine

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/shirou/gopsutil/mem"
)

const (
	numOfLevels = 7
)

type pebbleRefSlice struct {
	b []byte
	c io.Closer
}

func (rs *pebbleRefSlice) Free() {
	if rs.c != nil {
		rs.c.Close()
	}
}

func (rs *pebbleRefSlice) Bytes() []byte {
	if rs.b == nil {
		return nil
	}
	b := make([]byte, len(rs.b))
	copy(b, rs.b)
	return b
}

func (rs *pebbleRefSlice) Data() []byte {
	return rs.b
}

func GetRocksdbUint64(v []byte, err error) (uint64, error) {
	if err != nil {
		return 0, err
	} else if v == nil || len(v) == 0 {
		return 0, nil
	} else if len(v) != 8 {
		return 0, errIntNumber
	}

	return binary.LittleEndian.Uint64(v), nil
}

type Uint64AddMerger struct {
	buf []byte
}

func (m *Uint64AddMerger) MergeNewer(value []byte) error {
	cur, err := GetRocksdbUint64(m.buf, nil)
	if err != nil {
		return err
	}
	vint, err := GetRocksdbUint64(value, nil)
	if err != nil {
		return err
	}
	nv := cur + vint
	if m.buf == nil {
		m.buf = make([]byte, 8)
	}
	binary.LittleEndian.PutUint64(m.buf, nv)
	return nil
}

func (m *Uint64AddMerger) MergeOlder(value []byte) error {
	return m.MergeNewer(value)
}

func (m *Uint64AddMerger) Finish(flag bool) ([]byte, io.Closer, error) {
	return m.buf, nil, nil
}

func newUint64AddMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			res := &Uint64AddMerger{}
			res.MergeNewer(value)
			return res, nil
		},
		// the name should match the rocksdb default merge name
		Name: "UInt64AddOperator",
	}
}

type sharedPebbleConfig struct {
	SharedCache *pebble.Cache
}

func newSharedPebbleConfig(opt RockOptions) *sharedPebbleConfig {
	sc := &sharedPebbleConfig{}
	if opt.UseSharedCache {
		if opt.BlockCache <= 0 {
			v, err := mem.VirtualMemory()
			if err != nil {
				opt.BlockCache = 1024 * 1024 * 128 * 10
			} else {
				opt.BlockCache = int64(v.Total / 10)
				// for index cached, we double it
				opt.BlockCache *= 2
			}
		}
		sc.SharedCache = pebble.NewCache(opt.BlockCache)
	}
	return sc
}

func (sc *sharedPebbleConfig) ChangeLimiter(bytesPerSec int64) {
}

func (sc *sharedPebbleConfig) Destroy() {
	if sc.SharedCache != nil {
		sc.SharedCache.Unref()
	}
}

type PebbleEng struct {
	rwmutex     sync.RWMutex
	cfg         *RockEngConfig
	eng         *pebble.DB
	opts        *pebble.Options
	wo          *pebble.WriteOptions
	ito         *pebble.IterOptions
	wb          *pebbleWriteBatch
	engOpened   int32
	lastCompact int64
	deletedCnt  int64
	quit        chan struct{}
}

func NewPebbleEng(cfg *RockEngConfig) (*PebbleEng, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
	}

	if !cfg.ReadOnly {
		err := os.MkdirAll(cfg.DataDir, DIR_PERM)
		if err != nil {
			return nil, err
		}
	}
	lopts := make([]pebble.LevelOptions, 0)
	for l := 0; l < numOfLevels; l++ {
		compress := pebble.SnappyCompression
		if l <= cfg.MinLevelToCompress {
			compress = pebble.NoCompression
		}
		filter := bloom.FilterPolicy(10)
		opt := pebble.LevelOptions{
			Compression:    compress,
			BlockSize:      cfg.BlockSize,
			TargetFileSize: int64(cfg.TargetFileSizeBase),
			FilterPolicy:   filter,
		}
		opt.EnsureDefaults()
		lopts = append(lopts, opt)
	}

	opts := &pebble.Options{
		Levels:                      lopts,
		MaxManifestFileSize:         int64(cfg.MaxMainifestFileSize),
		MemTableSize:                cfg.WriteBufferSize,
		MemTableStopWritesThreshold: cfg.MaxWriteBufferNumber,
		LBaseMaxBytes:               int64(cfg.MaxBytesForLevelBase),
		L0CompactionThreshold:       cfg.Level0FileNumCompactionTrigger,
		MaxOpenFiles:                -1,
		MaxConcurrentCompactions:    cfg.MaxBackgroundCompactions,
		EventListener:               pebble.MakeLoggingEventListener(nil),
	}
	opts.EventListener.WALCreated = nil
	opts.EventListener.WALDeleted = nil
	opts.EventListener.FlushBegin = nil
	opts.EventListener.FlushEnd = nil
	opts.EventListener.TableCreated = nil
	opts.EventListener.TableDeleted = nil
	opts.EventListener.ManifestCreated = nil
	opts.EventListener.ManifestDeleted = nil
	if cfg.DisableWAL {
		opts.DisableWAL = true
	}
	// prefix search
	comp := *pebble.DefaultComparer
	opts.Comparer = &comp
	opts.Comparer.Split = func(a []byte) int {
		if len(a) <= 3 {
			return len(a)
		}
		return 3
	}
	cfg.EnableTableCounter = false
	db := &PebbleEng{
		cfg:  cfg,
		opts: opts,
		ito:  &pebble.IterOptions{},
		wo: &pebble.WriteOptions{
			Sync: !cfg.DisableWAL,
		},
		quit: make(chan struct{}),
	}
	if cfg.AutoCompacted {
		go db.compactLoop()
	}

	return db, nil
}

func (pe *PebbleEng) NewWriteBatch() WriteBatch {
	if pe.eng == nil {
		panic("nil engine, should only get write batch after db opened")
	}
	return newPebbleWriteBatch(pe.eng, pe.wo)
}

func (pe *PebbleEng) DefaultWriteBatch() WriteBatch {
	if pe.wb == nil {
		panic("nil write batch, should only get write batch after db opened")
	}
	return pe.wb
}

func (pe *PebbleEng) GetDataDir() string {
	return path.Join(pe.cfg.DataDir, "pebble")
}

func (pe *PebbleEng) SetCompactionFilter(ICompactFilter) {
}

func (pe *PebbleEng) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	return nil
}

func (pe *PebbleEng) compactLoop() {
	ticker := time.NewTicker(time.Hour)
	interval := (time.Hour / time.Second).Nanoseconds()
	dbLog.Infof("start auto compact loop : %v", interval)
	for {
		select {
		case <-pe.quit:
			return
		case <-ticker.C:
			if (pe.DeletedBeforeCompact() > compactThreshold) &&
				(time.Now().Unix()-pe.LastCompactTime()) > interval {
				dbLog.Infof("auto compact : %v, %v", pe.DeletedBeforeCompact(), pe.LastCompactTime())
				pe.CompactAllRange()
			}
		}
	}
}

func (pe *PebbleEng) CheckDBEngForRead(fullPath string) error {
	ro := pe.opts.Clone()
	ro.ErrorIfNotExists = true
	ro.ReadOnly = true
	//ro.Cache = nil
	db, err := pebble.Open(fullPath, ro)
	if err != nil {
		return err
	}
	db.Close()
	return nil
}

func (pe *PebbleEng) OpenEng() error {
	if !pe.IsClosed() {
		dbLog.Warningf("engine already opened: %v, should close it before reopen", pe.GetDataDir())
		return errors.New("open failed since not closed")
	}
	pe.rwmutex.Lock()
	defer pe.rwmutex.Unlock()
	if pe.cfg.UseSharedCache && pe.cfg.SharedConfig != nil {
		sc, ok := pe.cfg.SharedConfig.(*sharedPebbleConfig)
		if ok {
			pe.opts.Cache = sc.SharedCache
			dbLog.Infof("using shared cache for pebble engine")
		}
	} else {
		cache := pebble.NewCache(pe.cfg.BlockCache)
		defer cache.Unref()
		pe.opts.Cache = cache
	}
	opt := pe.opts
	if pe.cfg.ReadOnly {
		opt = pe.opts.Clone()
		opt.ErrorIfNotExists = true
		opt.ReadOnly = true
	}
	eng, err := pebble.Open(pe.GetDataDir(), opt)
	if err != nil {
		return err
	}
	pe.wb = newPebbleWriteBatch(eng, pe.wo)
	pe.eng = eng
	atomic.StoreInt32(&pe.engOpened, 1)
	dbLog.Infof("engine opened: %v", pe.GetDataDir())
	return nil
}

func (pe *PebbleEng) Write(wb WriteBatch) error {
	return wb.Commit()
}

func (pe *PebbleEng) DeletedBeforeCompact() int64 {
	return atomic.LoadInt64(&pe.deletedCnt)
}

func (pe *PebbleEng) AddDeletedCnt(c int64) {
	atomic.AddInt64(&pe.deletedCnt, c)
}

func (pe *PebbleEng) LastCompactTime() int64 {
	return atomic.LoadInt64(&pe.lastCompact)
}

func (pe *PebbleEng) CompactRange(rg CRange) {
	atomic.StoreInt64(&pe.lastCompact, time.Now().Unix())
	atomic.StoreInt64(&pe.deletedCnt, 0)
	pe.rwmutex.RLock()
	closed := pe.IsClosed()
	pe.rwmutex.RUnlock()
	if closed {
		return
	}
	pe.eng.Compact(rg.Start, rg.Limit)
}

func (pe *PebbleEng) CompactAllRange() {
	pe.CompactRange(CRange{})
}

func (pe *PebbleEng) DisableManualCompact(disable bool) {
}

func (pe *PebbleEng) GetApproximateTotalKeyNum() int {
	return 0
}

func (pe *PebbleEng) GetApproximateKeyNum(ranges []CRange) uint64 {
	return 0
}

func (pe *PebbleEng) SetOptsForLogStorage() {
	return
}

func (pe *PebbleEng) GetApproximateSizes(ranges []CRange, includeMem bool) []uint64 {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	sizeList := make([]uint64, len(ranges))
	if pe.IsClosed() {
		return sizeList
	}
	for i, r := range ranges {
		sizeList[i], _ = pe.eng.EstimateDiskUsage(r.Start, r.Limit)
	}
	return sizeList
}

func (pe *PebbleEng) IsClosed() bool {
	if atomic.LoadInt32(&pe.engOpened) == 0 {
		return true
	}
	return false
}

func (pe *PebbleEng) FlushAll() {
	if pe.cfg.DisableWAL {
		pe.eng.Flush()
	}
}

func (pe *PebbleEng) CloseEng() bool {
	pe.rwmutex.Lock()
	defer pe.rwmutex.Unlock()
	if pe.eng != nil {
		if atomic.CompareAndSwapInt32(&pe.engOpened, 1, 0) {
			if pe.wb != nil {
				pe.wb.Destroy()
			}
			if pe.cfg.DisableWAL {
				pe.eng.Flush()
			}
			pe.eng.Close()
			dbLog.Infof("engine closed: %v", pe.GetDataDir())
			return true
		}
	}
	return false
}

func (pe *PebbleEng) CloseAll() {
	select {
	case <-pe.quit:
	default:
		close(pe.quit)
	}
	pe.CloseEng()
}

func (pe *PebbleEng) GetStatistics() string {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return ""
	}
	return pe.eng.Metrics().String()
}

func (pe *PebbleEng) GetInternalStatus() map[string]interface{} {
	s := make(map[string]interface{})
	s["internal"] = pe.GetStatistics()
	return s
}

func (pe *PebbleEng) GetInternalPropertyStatus(p string) string {
	return p
}

func (pe *PebbleEng) GetBytesNoLock(key []byte) ([]byte, error) {
	val, err := pe.GetRefNoLock(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	defer val.Free()
	if val.Data() == nil {
		return nil, nil
	}
	return val.Bytes(), nil
}

func (pe *PebbleEng) GetBytes(key []byte) ([]byte, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return nil, errDBEngClosed
	}
	return pe.GetBytesNoLock(key)
}

func (pe *PebbleEng) MultiGetBytes(keyList [][]byte, values [][]byte, errs []error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		for i, _ := range errs {
			errs[i] = errDBEngClosed
		}
		return
	}
	for i, k := range keyList {
		values[i], errs[i] = pe.GetBytesNoLock(k)
	}
}

func (pe *PebbleEng) Exist(key []byte) (bool, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return false, errDBEngClosed
	}
	return pe.ExistNoLock(key)
}

func (pe *PebbleEng) ExistNoLock(key []byte) (bool, error) {
	val, err := pe.GetRefNoLock(key)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	ok := val.Data() != nil
	val.Free()
	return ok, nil
}

func (pe *PebbleEng) GetRefNoLock(key []byte) (RefSlice, error) {
	val, c, err := pe.eng.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return nil, err
	}
	return &pebbleRefSlice{b: val, c: c}, nil
}

func (pe *PebbleEng) GetRef(key []byte) (RefSlice, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return nil, errDBEngClosed
	}
	return pe.GetRefNoLock(key)
}

func (pe *PebbleEng) GetValueWithOp(key []byte,
	op func([]byte) error) error {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return errDBEngClosed
	}
	return pe.GetValueWithOpNoLock(key, op)
}

func (pe *PebbleEng) GetValueWithOpNoLock(key []byte,
	op func([]byte) error) error {
	val, err := pe.GetRef(key)
	if err != nil {
		return err
	}
	if val == nil {
		return op(nil)
	}
	defer val.Free()
	return op(val.Data())
}

func (pe *PebbleEng) DeleteFilesInRange(rg CRange) {
	return
}

func (pe *PebbleEng) GetIterator(opts IteratorOpts) (Iterator, error) {
	dbit, err := newPebbleIterator(pe, opts)
	if err != nil {
		return nil, err
	}
	return dbit, nil
}

func (pe *PebbleEng) NewCheckpoint(printToStdoutAlso bool) (KVCheckpoint, error) {
	return &pebbleEngCheckpoint{
		pe: pe,
	}, nil
}

type pebbleEngCheckpoint struct {
	pe *PebbleEng
}

func (pck *pebbleEngCheckpoint) Save(path string, notify chan struct{}) error {
	pck.pe.rwmutex.RLock()
	defer pck.pe.rwmutex.RUnlock()
	if pck.pe.IsClosed() {
		return errDBEngClosed
	}
	if notify != nil {
		time.AfterFunc(time.Millisecond*20, func() {
			close(notify)
		})
	}
	return pck.pe.eng.Checkpoint(path)
}
