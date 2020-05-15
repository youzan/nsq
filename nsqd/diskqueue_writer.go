package nsqd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/nsq/internal/util"
)

const (
	MAX_QUEUE_OFFSET_META_DATA_KEEP = 100
)

var (
	ErrInvalidOffset     = errors.New("invalid offset")
	ErrNeedFixQueueStart = errors.New("init queue start should be fixed")
	ErrNeedFixQueueEnd   = errors.New("init queue end should be fixed")
	writeBufSize         = 1024 * 128
)

func getQueueFileOffsetMeta(dataFileName string) (int64, int64, int64, error) {
	fName := dataFileName + ".offsetmeta.dat"
	f, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	cnt := int64(0)
	startPos := int64(0)
	endPos := int64(0)
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n",
		&cnt,
		&startPos, &endPos)
	if err != nil {
		nsqLog.LogErrorf("failed to read offset meta (%v): %v", fName, err)
		return 0, 0, 0, err
	}
	return cnt, startPos, endPos, nil
}

// diskQueueWriter implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueWriter struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// run-time state (also persisted to disk)
	diskWriteEnd diskQueueEndInfo
	diskReadEnd  diskQueueEndInfo
	// the start of the queue , will be set to the cleaned offset
	diskQueueStart diskQueueEndInfo
	sync.RWMutex

	// instantiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	exitFlag        int32
	needSync        bool

	writeFile     *os.File
	bufferWriter  *bufio.Writer
	bufSize       int64
	metaFlushLock sync.RWMutex
	metaStorage   IMetaStorage
	readOnly      bool
}

type extraMeta struct {
	SegOffset     diskQueueOffset
	VirtualOffset BackendOffset
	TotalMsgCnt   int64
}

func newDiskQueueWriterWithMetaStorage(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, metaS IMetaStorage) (BackendQueueWriter, error) {
	return newDiskQueueWriter(name, dataPath, maxBytesPerFile,
		minMsgSize, maxMsgSize, syncEvery, false, metaS)
}

func NewDiskQueueWriterForRead(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, metaS IMetaStorage) (BackendQueueWriter, error) {
	return newDiskQueueWriter(name, dataPath, maxBytesPerFile,
		minMsgSize, maxMsgSize, syncEvery, true, metaS)
}

// newDiskQueue instantiates a new instance of diskQueueWriter, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueWriter(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, readOnly bool, metaS IMetaStorage) (BackendQueueWriter, error) {

	d := diskQueueWriter{
		name:            name,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		maxMsgSize:      maxMsgSize,
		metaStorage:     metaS,
	}
	if d.metaStorage == nil {
		// use file as default
		d.metaStorage = &fileMetaStorage{}
	}

	d.readOnly = readOnly
	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData(readOnly)
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
		return &d, err
	}
	err = d.initQueueReadStart()
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to init queue start- %s", d.name, err)
		return &d, err
	}

	if !readOnly {
		if d.diskQueueStart.EndOffset.GreatThan(&d.diskWriteEnd.EndOffset) ||
			d.diskQueueStart.Offset() > d.diskWriteEnd.Offset() {
			nsqLog.LogErrorf("diskqueue(%s) queue start invalid: %v, write end: %v",
				d.name, d.diskQueueStart, d.diskWriteEnd)

			// check if the data file exist, the disk write meta may be lost
			_, err := os.Stat(d.fileName(d.diskWriteEnd.EndOffset.FileNum))
			if err != nil {
				if d.diskWriteEnd.EndOffset.FileNum <= 0 || d.diskWriteEnd.EndOffset.Pos > 0 {
					return &d, ErrNeedFixQueueEnd
				}
				// may at the start of segment file, so we check last segment
				_, err := os.Stat(d.fileName(d.diskWriteEnd.EndOffset.FileNum - 1))
				if err != nil {
					return &d, ErrNeedFixQueueEnd
				}
			}
			if d.diskWriteEnd.EndOffset.FileNum == 0 &&
				d.diskWriteEnd.EndOffset.Pos == 0 {
				// auto fix this case
				d.diskQueueStart.EndOffset = d.diskWriteEnd.EndOffset
			} else if d.diskWriteEnd.Offset() == 0 {
				d.diskQueueStart = d.diskWriteEnd
			} else if d.diskWriteEnd.EndOffset.FileNum == 0 {
				// still write first file, so the queue start should be init to empty
				d.diskQueueStart = diskQueueEndInfo{}
			} else {
				return &d, ErrNeedFixQueueStart
			}
		}
		d.saveExtraMeta()
	}

	return &d, nil
}

func (d *diskQueueWriter) tryFixData() error {
	d.Lock()
	defer d.Unlock()
	// queue start may be invalid after crash, so we can fix by manual to delete meta and reload it
	err := d.initQueueReadStart()
	// save anyway since already wrong, if err is ErrNeedFixQueueStart we should save
	if err == ErrNeedFixQueueStart {
		d.diskQueueStart = diskQueueEndInfo{}
	}
	d.saveExtraMeta()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (d *diskQueueWriter) SetBufSize(s int64) {
	atomic.StoreInt64(&d.bufSize, s)
}

func (d *diskQueueWriter) PutV2(data []byte) (BackendOffset, int32, diskQueueEndInfo, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, diskQueueEndInfo{}, errors.New("exiting")
	}
	offset, writeBytes, dend, werr := d.writeOne(data, false, 0)
	var e diskQueueEndInfo
	if dend != nil {
		e = *dend
	}
	d.Unlock()
	return offset, writeBytes, e, werr
}

func (d *diskQueueWriter) PutRawV2(data []byte, msgCnt int32) (BackendOffset, int32, diskQueueEndInfo, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, diskQueueEndInfo{}, errors.New("exiting")
	}
	offset, writeBytes, dend, werr := d.writeOne(data, true, msgCnt)
	var e diskQueueEndInfo
	if dend != nil {
		e = *dend
	}
	d.Unlock()
	return offset, writeBytes, e, werr
}

// Put writes a []byte to the queue
func (d *diskQueueWriter) Put(data []byte) (BackendOffset, int32, int64, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, 0, errors.New("exiting")
	}
	offset, writeBytes, dend, werr := d.writeOne(data, false, 0)
	var e diskQueueEndInfo
	if dend != nil {
		e = *dend
	}
	d.Unlock()
	return offset, writeBytes, e.TotalMsgCnt(), werr
}

func (d *diskQueueWriter) RollbackWriteV2(offset BackendOffset, diffCnt uint64) (diskQueueEndInfo, error) {
	d.Lock()
	defer d.Unlock()

	if offset < d.diskQueueStart.virtualEnd ||
		d.diskWriteEnd.TotalMsgCnt()-int64(diffCnt) < d.diskQueueStart.TotalMsgCnt() {
		nsqLog.Logf("rollback write to %v:%v invalid, less than queue start %v", offset, diffCnt, d.diskQueueStart)
		return d.diskWriteEnd, ErrInvalidOffset
	}

	if d.needSync {
		d.syncAll(true)
	}

	if offset > d.diskWriteEnd.Offset() {
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if offset < d.diskWriteEnd.Offset()-BackendOffset(d.diskWriteEnd.EndOffset.Pos) {
		nsqLog.Logf("rollback write position can not across file %v, %v, %v", offset, d.diskWriteEnd.Offset(), d.diskWriteEnd.EndOffset.Pos)
		return d.diskWriteEnd, ErrInvalidOffset
	}
	nsqLog.Logf("rollback from %v-%v, %v to %v, roll cnt: %v", d.diskWriteEnd.EndOffset.FileNum, d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.Offset(), offset, diffCnt)
	d.diskWriteEnd.EndOffset.Pos -= int64(d.diskWriteEnd.Offset() - offset)
	d.diskWriteEnd.virtualEnd = offset
	atomic.AddInt64(&d.diskWriteEnd.totalMsgCnt, -1*int64(diffCnt))

	if d.diskReadEnd.EndOffset.Pos > d.diskWriteEnd.EndOffset.Pos ||
		d.diskReadEnd.Offset() > d.diskWriteEnd.Offset() {
		d.diskReadEnd = d.diskWriteEnd
	}
	nsqLog.Logf("after rollback : %v, %v, read end: %v", d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.TotalMsgCnt(), d.diskReadEnd)
	d.truncateDiskQueueToWriteEnd()
	return d.diskWriteEnd, nil
}

func (d *diskQueueWriter) RollbackWrite(offset BackendOffset, diffCnt uint64) error {
	_, err := d.RollbackWriteV2(offset, diffCnt)
	return err
}

func (d *diskQueueWriter) ResetWriteEnd(offset BackendOffset, totalCnt int64) error {
	_, err := d.ResetWriteEndV2(offset, totalCnt)
	return err
}

func (d *diskQueueWriter) ResetWriteWithQueueStart(queueStart BackendQueueEnd) error {
	d.Lock()
	defer d.Unlock()
	nsqLog.Warningf("DISKQUEUE %v reset the queue start from %v:%v to new queue start: %v", d.name,
		d.diskQueueStart, d.diskWriteEnd, queueStart)

	d.cleanOldData()

	if queueStart.Offset() == 0 && d.diskQueueStart.Offset() == 0 {
		d.diskWriteEnd.EndOffset.FileNum = 0
		d.diskWriteEnd.EndOffset.Pos = 0
	}
	d.diskQueueStart = d.diskWriteEnd
	d.diskQueueStart.virtualEnd = queueStart.Offset()
	d.diskQueueStart.totalMsgCnt = queueStart.TotalMsgCnt()
	d.diskWriteEnd = d.diskQueueStart
	d.diskReadEnd = d.diskWriteEnd
	nsqLog.Warningf("DISKQUEUE %v new queue start : %v:%v", d.name,
		d.diskQueueStart, d.diskWriteEnd)
	d.saveExtraMeta()
	d.persistMetaData(true, d.diskWriteEnd)
	return nil
}

func (d *diskQueueWriter) prepareCleanByRetention(cleanEndInfo BackendQueueOffset,
	noRealClean bool, maxCleanOffset BackendOffset) (BackendQueueEnd, int64, int64, error) {
	if cleanEndInfo == nil {
		return nil, 0, 0, nil
	}
	d.Lock()
	defer d.Unlock()
	newStart := d.diskQueueStart
	var endInfo *diskQueueOffset
	switch e := cleanEndInfo.(type) {
	case *diskQueueOffsetInfo:
		endInfo = &e.EndOffset
	case *diskQueueEndInfo:
		endInfo = &e.EndOffset
	}
	cleanOffset := cleanEndInfo.Offset()
	if endInfo != nil {
		if endInfo.FileNum >= d.diskReadEnd.EndOffset.FileNum-1 {
			endInfo.FileNum = d.diskReadEnd.EndOffset.FileNum - 1
		}
		// it may happen while the disk is started with (filenum, offset) = (2, 0) because
		// of truncated from leader to replica
		// so we should ignore clean file less than queue start file num.
		if endInfo.FileNum <= d.diskQueueStart.EndOffset.FileNum {
			return nil, 0, 0, nil
		}
		if maxCleanOffset != BackendOffset(0) && cleanOffset > maxCleanOffset {
			nsqLog.LogWarningf("disk %v clean position %v exceed the max allowed clean end: %v", d.name, cleanOffset, maxCleanOffset)
			return nil, 0, 0, nil
		}
	} else {
		if cleanOffset >= d.diskReadEnd.Offset()-BackendOffset(d.diskReadEnd.EndOffset.Pos) {
			cleanOffset = d.diskReadEnd.Offset() - BackendOffset(d.diskReadEnd.EndOffset.Pos)
		}
		if maxCleanOffset != BackendOffset(0) && cleanOffset > maxCleanOffset {
			cleanOffset = maxCleanOffset
		}
	}
	cleanFileNum := int64(0)

	if endInfo != nil {
		cleanFileNum = endInfo.FileNum
		if cleanFileNum <= d.diskQueueStart.EndOffset.FileNum {
			return &newStart, 0, 0, nil
		}
	retryClean:
		cnt, _, endPos, err := getQueueFileOffsetMeta(d.fileName(cleanFileNum - 1))
		if err != nil {
			nsqLog.Logf("disk %v failed to get queue offset meta: %v", d.fileName(cleanFileNum), err)
			if os.IsNotExist(err) && cleanFileNum < d.diskReadEnd.EndOffset.FileNum-1 {
				cleanFileNum++
				goto retryClean
			}
			return &newStart, 0, 0, err
		}
		if maxCleanOffset != BackendOffset(0) && BackendOffset(endPos) > maxCleanOffset {
			nsqLog.LogWarningf("disk %v clean position %v exceed the max allowed clean end: %v", d.name, endPos, maxCleanOffset)
			return &newStart, 0, 0, errors.New("clean exceed the max allowed")
		}
		newStart.EndOffset.FileNum = cleanFileNum
		newStart.EndOffset.Pos = 0
		newStart.virtualEnd = BackendOffset(endPos)
		newStart.totalMsgCnt = cnt
	} else if cleanOffset >= 0 {
		checkedNum := newStart.EndOffset.FileNum
		for {
			cnt, _, endPos, err := getQueueFileOffsetMeta(d.fileName(checkedNum))
			if err != nil {
				nsqLog.LogWarningf("disk %v failed to get queue offset meta: %v", checkedNum, err)
				if os.IsNotExist(err) && checkedNum < d.diskReadEnd.EndOffset.FileNum-1 {
					checkedNum++
					continue
				}
				return &newStart, 0, 0, err
			}
			if BackendOffset(endPos) < cleanOffset {
				if checkedNum >= d.diskReadEnd.EndOffset.FileNum-1 {
					break
				}
				checkedNum++
				cleanFileNum = checkedNum
				newStart.EndOffset.FileNum = checkedNum
				newStart.EndOffset.Pos = 0
				newStart.virtualEnd = BackendOffset(endPos)
				newStart.totalMsgCnt = cnt
			} else {
				break
			}
		}
	}

	if noRealClean {
		return &newStart, 0, 0, nil
	}

	nsqLog.Infof("DISKQUEUE %v clean queue from %v, %v to new start : %v", d.name,
		d.diskQueueStart, d.diskWriteEnd, newStart)

	cleanStartFileNum := d.diskQueueStart.EndOffset.FileNum - MAX_QUEUE_OFFSET_META_DATA_KEEP - 1
	if cleanStartFileNum < 0 {
		cleanStartFileNum = 0
	}
	d.diskQueueStart = newStart
	d.saveExtraMeta()
	return &newStart, cleanStartFileNum, cleanFileNum, nil
}

func (d *diskQueueWriter) CleanOldDataByRetention(cleanEndInfo BackendQueueOffset,
	noRealClean bool, maxCleanOffset BackendOffset) (BackendQueueEnd, error) {
	newStart, cleanStartFileNum, cleanFileNum, err := d.prepareCleanByRetention(cleanEndInfo, noRealClean, maxCleanOffset)
	if err != nil {
		return nil, err
	}
	cleanMetaFileNum := cleanFileNum - MAX_QUEUE_OFFSET_META_DATA_KEEP
	for i := cleanStartFileNum; i < cleanFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil {
			if !os.IsNotExist(innerErr) {
				nsqLog.LogErrorf("diskqueue(%s) failed to remove data file %v - %s", d.name, fn, innerErr)
				continue
			}
		} else {
			nsqLog.Logf("DISKQUEUE(%s): removed data file: %v", d.name, fn)
		}

		//remove queue meta file
		if i <= cleanMetaFileNum {
			fn = d.fileName(i) + ".offsetmeta.dat"
			innerErr = os.Remove(fn)
			if innerErr != nil {
				if !os.IsNotExist(innerErr) {
					nsqLog.LogErrorf("diskqueue(%s) failed to remove offset meta data file %v - %s", d.name, fn, innerErr)
				}
			} else {
				nsqLog.Debugf("DISKQUEUE(%s): removed offset meta data file: %v", d.name, fn)
			}
		}
	}

	return newStart, nil
}

func (d *diskQueueWriter) closeCurrentFile() {
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	if d.diskReadEnd.EndOffset.GreatThan(&d.diskWriteEnd.EndOffset) {
		nsqLog.Logf("DISKQUEUE(%s): old read is greater: %v, %v", d.name,
			d.diskReadEnd, d.diskWriteEnd)
	}
	d.diskReadEnd = d.diskWriteEnd
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
}

func (d *diskQueueWriter) truncateDiskQueueToWriteEnd() {
	if d.writeFile != nil {
		d.writeFile.Truncate(d.diskWriteEnd.EndOffset.Pos)
		d.writeFile.Close()
		d.writeFile = nil
	} else {
		curFileName := d.fileName(d.diskWriteEnd.EndOffset.FileNum)
		tmpFile, err := os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			nsqLog.LogErrorf("open write queue failed: %v", err)
		} else {
			tmpFile.Truncate(d.diskWriteEnd.EndOffset.Pos)
			tmpFile.Close()
		}
	}
	d.persistMetaData(true, d.diskWriteEnd)
	cleanNum := d.diskWriteEnd.EndOffset.FileNum + 1
	for {
		fileName := d.fileName(cleanNum)
		err := os.Rename(fileName, fileName+".rolldata")
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			nsqLog.LogErrorf("truncate and remove the write file %v failed: %v", fileName, err)
		}
		nsqLog.LogWarningf("truncate queue and remove the write file %v ", fileName)
		cleanNum++
	}
}

// TryFixWriteEnd should only be used to expand current write end, for truncate use resetwrite end instead
func (d *diskQueueWriter) TryFixWriteEnd(offset BackendOffset, totalCnt int64) (diskQueueEndInfo, error) {
	d.Lock()
	defer d.Unlock()
	if offset < d.diskQueueStart.virtualEnd || totalCnt < d.diskQueueStart.TotalMsgCnt() {
		nsqLog.Logf("reset write end to %v:%v invalid, less than queue start %v", offset, totalCnt, d.diskQueueStart)
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if offset < d.diskWriteEnd.Offset() {
		// only expand end allowed
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if d.needSync {
		d.syncAll(true)
	}
	nsqLog.Logf("fix write end from %v to %v, reset to totalCnt: %v", d.diskWriteEnd.Offset(), offset, totalCnt)
	newEnd := d.diskQueueStart.Offset()
	newWriteFileNum := d.diskQueueStart.EndOffset.FileNum
	newWritePos := d.diskQueueStart.EndOffset.Pos
	for {
		f, err := os.Stat(d.fileName(newWriteFileNum))
		if err != nil {
			nsqLog.LogErrorf("stat data file error %v, %v", offset, newWriteFileNum)
			return d.diskWriteEnd, err
		}
		diff := offset - newEnd
		if newWritePos+int64(diff) <= f.Size() {
			newWritePos += int64(diff)
			newEnd = offset
			break
		}
		newEnd += BackendOffset(f.Size() - newWritePos)
		newWritePos = 0
		newWriteFileNum++
	}
	d.diskWriteEnd.EndOffset.FileNum = newWriteFileNum
	d.diskWriteEnd.EndOffset.Pos = newWritePos
	d.diskWriteEnd.virtualEnd = offset
	atomic.StoreInt64(&d.diskWriteEnd.totalMsgCnt, int64(totalCnt))
	d.diskReadEnd = d.diskWriteEnd
	d.closeCurrentFile()
	nsqLog.Logf("fix write end result : %v", d.diskWriteEnd)
	d.truncateDiskQueueToWriteEnd()
	return d.diskWriteEnd, nil
}

func (d *diskQueueWriter) ResetWriteEndV2(offset BackendOffset, totalCnt int64) (diskQueueEndInfo, error) {
	d.Lock()
	defer d.Unlock()
	if offset < d.diskQueueStart.virtualEnd || totalCnt < d.diskQueueStart.TotalMsgCnt() {
		nsqLog.Logf("reset write end to %v:%v invalid, less than queue start %v", offset, totalCnt, d.diskQueueStart)
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if offset > d.diskWriteEnd.Offset() {
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if d.needSync {
		d.syncAll(true)
	}
	nsqLog.Logf("reset write end from %v to %v, reset to totalCnt: %v", d.diskWriteEnd.Offset(), offset, totalCnt)
	if offset == 0 {
		d.closeCurrentFile()
		d.diskWriteEnd = d.diskQueueStart
		d.diskReadEnd = d.diskWriteEnd
		d.truncateDiskQueueToWriteEnd()
		return d.diskWriteEnd, nil
	}
	newEnd := d.diskWriteEnd.Offset()
	newWriteFileNum := d.diskWriteEnd.EndOffset.FileNum
	newWritePos := d.diskWriteEnd.EndOffset.Pos
	for offset < newEnd-BackendOffset(newWritePos) {
		nsqLog.Logf("reset write acrossing file %v, %v, %v, %v", offset, newEnd, newWritePos, newWriteFileNum)
		newEnd -= BackendOffset(newWritePos)
		newWriteFileNum--
		if newWriteFileNum < 0 {
			nsqLog.Logf("reset write acrossed the begin %v, %v, %v", offset, newEnd, newWriteFileNum)
			return d.diskWriteEnd, ErrInvalidOffset
		}
		f, err := os.Stat(d.fileName(newWriteFileNum))
		if err != nil {
			nsqLog.LogErrorf("stat data file error %v, %v", offset, newWriteFileNum)
			return d.diskWriteEnd, err
		}
		newWritePos = f.Size()
	}
	d.diskWriteEnd.EndOffset.FileNum = newWriteFileNum
	newWritePos -= int64(newEnd - offset)
	d.diskWriteEnd.EndOffset.Pos = newWritePos
	d.diskWriteEnd.virtualEnd = offset
	atomic.StoreInt64(&d.diskWriteEnd.totalMsgCnt, int64(totalCnt))
	d.diskReadEnd = d.diskWriteEnd
	d.closeCurrentFile()
	nsqLog.Logf("reset write end result : %v", d.diskWriteEnd)
	d.truncateDiskQueueToWriteEnd()

	return d.diskWriteEnd, nil
}

// Close cleans up the queue and persists metadata
func (d *diskQueueWriter) Close() error {
	return d.exit(false)
}

func (d *diskQueueWriter) Delete() error {
	return d.exit(true)
}

func (d *diskQueueWriter) RemoveTo(destPath string) error {
	d.Lock()
	defer d.Unlock()
	d.exitFlag = 1
	nsqLog.Logf("DISKQUEUE(%s): removing to %v", d.name, destPath)
	d.syncAll(true)
	d.closeCurrentFile()
	d.saveFileOffsetMeta()
	for i := int64(0); i <= d.diskWriteEnd.EndOffset.FileNum; i++ {
		fn := d.fileName(i)
		destFile := GetQueueFileName(destPath, d.name, i)
		innerErr := util.AtomicRename(fn, destFile)
		nsqLog.Logf("DISKQUEUE(%s): renamed data file %v to %v", d.name, fn, destFile)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		}
		fName := d.fileName(i) + ".offsetmeta.dat"
		innerErr = util.AtomicRename(fName, destFile+".offsetmeta.dat")
		nsqLog.Logf("DISKQUEUE(%s): rename offset meta file %v ", d.name, fName)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove offset meta file %v - %s", d.name, fName, innerErr)
		}
	}
	d.diskWriteEnd.EndOffset.FileNum++
	d.diskWriteEnd.EndOffset.Pos = 0
	d.diskReadEnd = d.diskWriteEnd
	destFile := fmt.Sprintf(path.Join(destPath, "%s.diskqueue.meta.writer.dat"), d.name)
	nsqLog.Logf("DISKQUEUE(%s): rename meta file to %v", d.name, destFile)
	d.metaStorage.RemoveWriter(d.metaDataFileName())
	destFile = fmt.Sprintf(path.Join(destPath, "%s.diskqueue.meta.extra.dat"), d.name)
	util.AtomicRename(d.extraMetaFileName(), destFile)
	return nil
}

func (d *diskQueueWriter) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		nsqLog.Logf("DISKQUEUE(%s): deleting", d.name)
	} else {
		nsqLog.Logf("DISKQUEUE(%s): closing", d.name)
	}

	d.syncAll(true)
	if deleted {
		return d.deleteAllFiles(deleted)
	}
	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueueWriter) Empty() error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	nsqLog.Logf("DISKQUEUE(%s): emptying", d.name)
	return d.deleteAllFiles(false)
}

func (d *diskQueueWriter) deleteAllFiles(deleted bool) error {
	d.cleanOldData()
	d.persistMetaData(true, d.diskWriteEnd)
	d.saveExtraMeta()

	if deleted {
		nsqLog.Logf("DISKQUEUE(%s): deleting meta file", d.name)
		d.metaStorage.RemoveWriter(d.metaDataFileName())
		os.Remove(d.extraMetaFileName())
	}
	return nil
}

func (d *diskQueueWriter) cleanOldData() error {
	d.closeCurrentFile()
	d.saveFileOffsetMeta()
	cleanStartFileNum := d.diskQueueStart.EndOffset.FileNum - MAX_QUEUE_OFFSET_META_DATA_KEEP - 1
	if cleanStartFileNum < 0 {
		cleanStartFileNum = 0
	}
	for i := cleanStartFileNum; i <= d.diskWriteEnd.EndOffset.FileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		nsqLog.Logf("DISKQUEUE(%s): removed data file: %v", d.name, fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		} else {
			fName := d.fileName(i) + ".offsetmeta.dat"
			innerErr := os.Remove(fName)
			nsqLog.Logf("DISKQUEUE(%s): removed offset meta file: %v", d.name, fName)
			if innerErr != nil && !os.IsNotExist(innerErr) {
				nsqLog.LogErrorf("diskqueue(%s) failed to remove offset meta file %v - %s", d.name, fName, innerErr)
			}
		}
	}

	d.diskWriteEnd.EndOffset.FileNum++
	d.diskWriteEnd.EndOffset.Pos = 0
	d.diskReadEnd = d.diskWriteEnd
	d.diskQueueStart = d.diskWriteEnd
	return nil
}

func (d *diskQueueWriter) saveFileOffsetMeta() {
	fName := d.fileName(d.diskWriteEnd.EndOffset.FileNum) + ".offsetmeta.dat"
	f, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		nsqLog.LogErrorf("diskqueue(%s) failed to save data offset meta: %v", d.name, err)
		return
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n",
		atomic.LoadInt64(&d.diskWriteEnd.totalMsgCnt),
		d.diskWriteEnd.Offset()-BackendOffset(d.diskWriteEnd.EndOffset.Pos), d.diskWriteEnd.Offset())
	if err != nil {
		f.Close()
		nsqLog.LogErrorf("diskqueue(%s) failed to save data offset meta: %v", d.name, err)
		return
	}
	f.Sync()
	f.Close()
}

func (d *diskQueueWriter) GetQueueWriteEnd() BackendQueueEnd {
	d.RLock()
	e := &diskQueueEndInfo{}
	*e = d.diskWriteEnd
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) GetQueueReadStart() BackendQueueEnd {
	d.RLock()
	e := &diskQueueEndInfo{}
	*e = d.diskQueueStart
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	e := d.internalGetQueueReadEnd()
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) internalGetQueueReadEnd() *diskQueueEndInfo {
	e := &diskQueueEndInfo{}
	*e = d.diskReadEnd
	return e
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueueWriter) writeOne(data []byte, isRaw bool, msgCnt int32) (BackendOffset, int32, *diskQueueEndInfo, error) {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.diskWriteEnd.EndOffset.FileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, 0, nil, err
		}

		nsqLog.Logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.diskWriteEnd.EndOffset.Pos > 0 {
			_, err = d.writeFile.Seek(d.diskWriteEnd.EndOffset.Pos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return 0, 0, nil, err
			}
		}
		if d.bufferWriter == nil {
			// TODO: maybe we can avoid use large buffer if sync count is small
			d.bufferWriter = bufio.NewWriterSize(d.writeFile, writeBufSize)
		} else {
			d.bufferWriter.Reset(d.writeFile)
		}
	}

	d.needSync = true
	dataLen := int32(len(data))
	if !isRaw {
		if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
			return 0, 0, nil, fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
		}

		err = binary.Write(d.bufferWriter, binary.BigEndian, dataLen)
		if err != nil {
			d.sync(true, false)
			if d.writeFile != nil {
				d.writeFile.Close()
				d.writeFile = nil
			}
			nsqLog.Logf("DISKQUEUE(%s): writeOne() failed %s", d.name, err)
			return 0, 0, nil, err
		}
	}
	_, err = d.bufferWriter.Write(data)
	if err != nil {
		d.sync(true, false)
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		nsqLog.Logf("DISKQUEUE(%s): writeOne() failed %s", d.name, err)
		return 0, 0, nil, err
	}

	writeOffset := d.diskWriteEnd.Offset()
	totalBytes := int64(dataLen)
	if !isRaw {
		totalBytes += 4
	}
	d.diskWriteEnd.EndOffset.Pos += totalBytes
	d.diskWriteEnd.virtualEnd += BackendOffset(totalBytes)
	if !isRaw {
		atomic.AddInt64(&d.diskWriteEnd.totalMsgCnt, 1)
	} else {
		atomic.AddInt64(&d.diskWriteEnd.totalMsgCnt, int64(msgCnt))
	}

	if d.diskWriteEnd.EndOffset.Pos >= d.maxBytesPerFile {
		// sync every time we start writing to a new file
		err = d.sync(false, false)
		if err != nil {
			nsqLog.LogErrorf("diskqueue(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.saveFileOffsetMeta()
		nsqLog.LogDebugf("DISKQUEUE(%s): new file write, last file: %v", d.name, d.diskWriteEnd)

		d.diskWriteEnd.EndOffset.FileNum++
		d.diskWriteEnd.EndOffset.Pos = 0
		d.diskReadEnd = d.diskWriteEnd
		d.needSync = true
	}

	return writeOffset, int32(totalBytes), &d.diskWriteEnd, err
}

func (d *diskQueueWriter) Flush(fsync bool) error {
	s := time.Now()
	var err error

	d.Lock()
	needSync := false
	if d.exitFlag == 1 {
		err = errors.New("exiting")
	} else {
		needSync = d.needSync
		if d.needSync {
			err = d.sync(fsync, false)
		}
	}
	we := d.diskWriteEnd
	d.Unlock()
	// flush persist meta out of the write lock to avoid slow down write message
	cost1 := time.Now().Sub(s)
	if needSync && err == nil {
		d.persistMetaData(fsync, we)
	}
	cost2 := time.Now().Sub(s)
	if cost2 > slowCost || cost1 > slowCost {
		nsqLog.Logf("disk writer(%s): flush cost: %v, %v", d.name, cost1, cost2)
	}
	return err
}

func (d *diskQueueWriter) FlushBuffer() bool {
	hasData := false
	d.Lock()
	if d.bufferWriter != nil && d.bufferWriter.Buffered() > 0 {
		hasData = true
		d.bufferWriter.Flush()
		if d.diskReadEnd.EndOffset.GreatThan(&d.diskWriteEnd.EndOffset) {
			nsqLog.LogWarningf("DISKQUEUE(%s): old read is greater: %v, %v", d.name,
				d.diskReadEnd, d.diskWriteEnd)
		}
		d.diskReadEnd = d.diskWriteEnd
	}
	d.Unlock()
	return hasData
}

func (d *diskQueueWriter) sync(fsync bool, metaSync bool) error {
	d.needSync = false
	if d.readOnly {
		return nil
	}
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	if d.writeFile != nil && fsync {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	if d.diskReadEnd.EndOffset.GreatThan(&d.diskWriteEnd.EndOffset) {
		nsqLog.LogWarningf("DISKQUEUE(%s): old read is greater: %v, %v", d.name,
			d.diskReadEnd, d.diskWriteEnd)
	}

	d.diskReadEnd = d.diskWriteEnd

	if metaSync {
		err := d.persistMetaData(fsync, d.diskWriteEnd)
		if err != nil {
			return err
		}
	}

	return nil
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueueWriter) syncAll(fsync bool) error {
	return d.sync(fsync, true)
}

func (d *diskQueueWriter) initQueueReadStart() error {
	// first try read from meta file
	err := d.loadExtraMeta()
	if err != nil {
		nsqLog.Infof("failed to load extra meta from file: %v", err)
	} else {
		return nil
	}
	var readStart diskQueueEndInfo
	needFix := false
	for {
		curFile := d.fileName(readStart.EndOffset.FileNum)
		_, err := os.Stat(curFile)
		if err != nil {
			needFix = true
			if os.IsNotExist(err) {
				if d.diskWriteEnd.EndOffset.FileNum == int64(0) &&
					d.diskWriteEnd.EndOffset.Pos == int64(0) {
					// init with empty
					return nil
				}
				readStart.EndOffset.FileNum++
				readStart.EndOffset.Pos = 0
				if readStart.EndOffset.FileNum > d.diskWriteEnd.EndOffset.FileNum {
					nsqLog.Errorf("topic %v no data file found to end: %v, reset read start to end", d.name, d.diskWriteEnd)
					d.diskQueueStart = d.diskWriteEnd
					return ErrNeedFixQueueStart
				}
			} else {
				return err
			}
		} else if needFix {
			_, _, _, err := getQueueFileOffsetMeta(d.fileName(readStart.EndOffset.FileNum - 1))
			if err != nil {
				if os.IsNotExist(err) {
					readStart.EndOffset.FileNum++
					readStart.EndOffset.Pos = 0
					if readStart.EndOffset.FileNum > d.diskWriteEnd.EndOffset.FileNum {
						nsqLog.Errorf("topic %v no data meta file found to end: %v, reset read start to end", d.name, d.diskWriteEnd)
						d.diskQueueStart = d.diskWriteEnd
						return ErrNeedFixQueueStart
					}
				} else {
					return err
				}
			} else {
				break
			}
		} else {
			break
		}
	}
	if !needFix {
		if readStart.EndOffset.FileNum != 0 {
			nsqLog.Warningf("topic : %v not start as 0 but no fix : %v", d.name, readStart)
		}
		d.diskQueueStart = readStart
	} else {
		cnt, _, endPos, err := getQueueFileOffsetMeta(d.fileName(readStart.EndOffset.FileNum - 1))
		if err != nil {
			return err
		}
		readStart.virtualEnd = BackendOffset(endPos)
		readStart.totalMsgCnt = cnt
		d.diskQueueStart = readStart
		nsqLog.Logf("%v init the disk queue start: %v", d.name, d.diskQueueStart)
	}
	return nil
}

func getQueueStart(dataPath string, name string) (diskQueueEndInfo, error) {
	fileName := fmt.Sprintf(path.Join(dataPath, "%s.diskqueue.meta.extra.dat"), name)
	return getQueueStartFromFile(fileName)
}

func getQueueStartFromFile(fileName string) (diskQueueEndInfo, error) {
	var startQ diskQueueEndInfo
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return startQ, err
	}
	var tmp extraMeta
	err = json.Unmarshal(data, &tmp)
	if err != nil {
		return startQ, err
	}
	startQ.EndOffset = tmp.SegOffset
	startQ.virtualEnd = tmp.VirtualOffset
	startQ.totalMsgCnt = tmp.TotalMsgCnt
	nsqLog.Infof(" %v load extra meta: %v", fileName, tmp)
	return startQ, nil
}

func (d *diskQueueWriter) loadExtraMeta() error {
	fileName := d.extraMetaFileName()
	startQ, err := getQueueStartFromFile(fileName)
	if err != nil {
		return err
	}
	d.diskQueueStart = startQ
	return nil
}

func (d *diskQueueWriter) saveExtraMeta() error {
	if d.readOnly {
		return nil
	}
	var err error

	fileName := d.extraMetaFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	var tmp extraMeta
	tmp.SegOffset = d.diskQueueStart.EndOffset
	tmp.VirtualOffset = d.diskQueueStart.Offset()
	tmp.TotalMsgCnt = d.diskQueueStart.TotalMsgCnt()
	data, _ := json.Marshal(tmp)
	err = ioutil.WriteFile(tmpFileName, data, 0644)
	if err != nil {
		return err
	}
	// atomically rename
	return util.AtomicRename(tmpFileName, fileName)
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueueWriter) retrieveMetaData(readOnly bool) error {
	fileName := d.metaDataFileName()
	qend, err := d.metaStorage.RetrieveWriter(fileName, readOnly)
	if err != nil {
		return err
	}
	d.diskWriteEnd = qend
	d.diskReadEnd = d.diskWriteEnd
	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueWriter) persistMetaData(fsync bool, writeEnd diskQueueEndInfo) error {
	if d.readOnly {
		return nil
	}
	fileName := d.metaDataFileName()
	return d.metaStorage.PersistWriter(fileName, fsync, writeEnd)
}

func (d *diskQueueWriter) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.writer.dat"), d.name)
}

func (d *diskQueueWriter) fileName(fileNum int64) string {
	return GetQueueFileName(d.dataPath, d.name, fileNum)
}

func (d *diskQueueWriter) extraMetaFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.extra.dat"), d.name)
}
