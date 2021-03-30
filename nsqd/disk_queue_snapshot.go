package nsqd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/youzan/nsq/internal/levellogger"
)

// note: the message count info is not kept in snapshot
type DiskQueueSnapshot struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	queueStart diskQueueEndInfo
	readPos    diskQueueEndInfo
	endPos     diskQueueEndInfo

	sync.RWMutex

	readFrom string
	dataPath string
	exitFlag int32

	readFile *os.File
}

// newDiskQueue instantiates a new instance of DiskQueueSnapshot, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueueSnapshot(readFrom string, dataPath string, endInfo BackendQueueEnd) *DiskQueueSnapshot {
	d := DiskQueueSnapshot{
		readFrom: readFrom,
		dataPath: dataPath,
	}

	d.UpdateQueueEnd(endInfo)

	return &d
}

func (d *DiskQueueSnapshot) ResetToStart() {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return
	}
	d.readPos = d.queueStart
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
}

func (d *DiskQueueSnapshot) getCurrentFileEnd(offset diskQueueOffset) (int64, error) {
	curFileName := d.fileName(offset.FileNum)
	f, err := os.Stat(curFileName)
	if err != nil {
		return 0, err
	}
	return f.Size(), nil
}

func (d *DiskQueueSnapshot) SetQueueStart(start BackendQueueEnd) {
	startPos, ok := start.(*diskQueueEndInfo)
	if !ok || startPos == nil {
		return
	}
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return
	}
	nsqLog.Infof("topic : %v change start from %v to %v", d.readFrom, d.queueStart, startPos)
	d.queueStart = *startPos
	if d.queueStart.EndOffset.GreatThan(&d.readPos.EndOffset) {
		d.readPos = d.queueStart
	}
}

func (d *DiskQueueSnapshot) GetQueueReadStart() BackendQueueEnd {
	d.Lock()
	defer d.Unlock()
	s := d.queueStart
	return &s
}

// Put writes a []byte to the queue
func (d *DiskQueueSnapshot) UpdateQueueEnd(e BackendQueueEnd) {
	endPos, ok := e.(*diskQueueEndInfo)
	if !ok || endPos == nil {
		return
	}
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return
	}
	if d.readPos.EndOffset.GreatThan(&endPos.EndOffset) {
		d.readPos = *endPos
	}
	d.endPos = *endPos
}

// Close cleans up the queue and persists metadata
func (d *DiskQueueSnapshot) Close() error {
	return d.exit()
}

func (d *DiskQueueSnapshot) exit() error {
	d.Lock()

	d.exitFlag = 1
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.Unlock()
	return nil
}

func (d *DiskQueueSnapshot) GetCurrentReadQueueOffset() BackendQueueOffset {
	d.Lock()
	cur := d.readPos
	var tmp diskQueueOffsetInfo
	tmp.EndOffset = cur.EndOffset
	tmp.virtualEnd = cur.virtualEnd
	d.Unlock()
	return &tmp
}

func (d *DiskQueueSnapshot) stepOffset(allowBackward bool, cur diskQueueEndInfo, step int64, maxStep diskQueueEndInfo) (diskQueueOffset, error) {
	newOffset := cur
	if cur.EndOffset.FileNum > maxStep.EndOffset.FileNum {
		return newOffset.EndOffset, fmt.Errorf("offset invalid: %v , %v", cur, maxStep)
	}
	if step == 0 {
		return newOffset.EndOffset, nil
	}
	if !allowBackward && step < 0 {
		return newOffset.EndOffset, fmt.Errorf("can not step backward")
	}
	return stepOffset(d.dataPath, d.readFrom, cur, BackendOffset(step), maxStep)
}

func (d *DiskQueueSnapshot) SkipToNext() error {
	d.Lock()
	defer d.Unlock()
	newPos := d.readPos
retry:
	if newPos.EndOffset.FileNum >= d.endPos.EndOffset.FileNum {
		return ErrReadEndOfQueue
	}

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	cnt, _, endPos, err := getQueueFileOffsetMeta(d.fileName(newPos.EndOffset.FileNum))
	newPos.EndOffset.FileNum++
	newPos.EndOffset.Pos = 0
	newPos.virtualEnd = BackendOffset(endPos)
	newPos.totalMsgCnt = cnt
	if err != nil {
		if os.IsNotExist(err) {
			nsqLog.LogWarningf("skip not exist offset meta: %v", newPos)
			goto retry
		} else {
			return err
		}
	}
	d.readPos.EndOffset = newPos.EndOffset
	d.readPos.virtualEnd = newPos.virtualEnd
	d.readPos.totalMsgCnt = newPos.totalMsgCnt
	return nil
}

// this can allow backward seek
func (d *DiskQueueSnapshot) ResetSeekTo(voffset BackendOffset, cnt int64) error {
	return d.seekTo(voffset, cnt, true)
}

func (d *DiskQueueSnapshot) SeekTo(voffset BackendOffset, cnt int64) error {
	return d.seekTo(voffset, cnt, false)
}

func (d *DiskQueueSnapshot) seekTo(voffset BackendOffset, cnt int64, allowBackward bool) error {
	d.Lock()
	defer d.Unlock()
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	var err error
	newPos := d.endPos.EndOffset
	if voffset > d.endPos.virtualEnd {
		nsqLog.LogErrorf("internal skip error : skipping overflow to : %v, %v", voffset, d.endPos)
		return ErrMoveOffsetOverflowed
	} else if voffset == d.endPos.virtualEnd {
		newPos = d.endPos.EndOffset
	} else {
		if voffset < d.queueStart.Offset() {
			nsqLog.LogWarningf("seek error : seek queue position cleaned : %v, %v", voffset, d.queueStart)
			return ErrReadQueueAlreadyCleaned
		}

		newPos, err = d.stepOffset(allowBackward, d.readPos, int64(voffset-d.readPos.virtualEnd), d.endPos)
		if err != nil {
			nsqLog.LogErrorf("internal skip error : %v, step from %v to : %v, current start: %v", err, d.readPos, voffset, d.queueStart)
			return err
		}
	}

	nsqLog.LogDebugf("===snapshot read seek from %v, %v to: %v, %v", d.readPos,
		d.readPos, newPos, voffset)
	d.readPos.EndOffset = newPos
	d.readPos.virtualEnd = voffset
	d.readPos.totalMsgCnt = cnt
	return nil
}

func (d *DiskQueueSnapshot) SeekToEnd() error {
	d.Lock()
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	d.readPos = d.endPos
	d.Unlock()
	return nil
}

func (d *DiskQueueSnapshot) ReadRaw(size int32) ([]byte, error) {
	d.Lock()
	defer d.Unlock()

	result := make([]byte, size)
	readOffset := int32(0)
	var err error
	var rn int
	if d.readPos.Offset() == d.endPos.Offset() && d.readPos.EndOffset == d.endPos.EndOffset {
		nsqLog.Logf("DISKQUEUE snapshot(%s): readRaw() read end: %v, %v",
			d.readFrom, d.readPos, d.endPos)
		return nil, io.EOF
	}

	for readOffset < size {
	CheckFileOpen:
		if d.readFile == nil {
			curFileName := d.fileName(d.readPos.EndOffset.FileNum)
			d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
			if err != nil {
				if d.readPos.Offset() == d.endPos.Offset() && d.readPos.EndOffset == d.endPos.EndOffset {
					if os.IsNotExist(err) {
						return nil, io.EOF
					}
				}
				return result, err
			}
			nsqLog.LogDebugf("DISKQUEUE snapshot(%s): readRaw() opened %s", d.readFrom, curFileName)
			if d.readPos.EndOffset.Pos > 0 {
				_, err = d.readFile.Seek(d.readPos.EndOffset.Pos, 0)
				if err != nil {
					d.readFile.Close()
					d.readFile = nil
					return result, err
				}
			}
		}
		if d.readPos.EndOffset.FileNum > d.endPos.EndOffset.FileNum {
			nsqLog.LogErrorf("%v read raw error : overflow to : %v, %v", d.readFrom, d.readPos, d.endPos)
			return result, ErrMoveOffsetOverflowed
		}
		if d.readPos.virtualEnd+BackendOffset(len(result))-BackendOffset(readOffset) > d.endPos.virtualEnd {
			nsqLog.LogErrorf("%v read raw error : overflow to : %v, %v, %v %v", d.readFrom, d.readPos, readOffset, size, d.endPos)
			return result, ErrMoveOffsetOverflowed
		}
		rn, err = io.ReadFull(d.readFile, result[readOffset:])
		readOffset += int32(rn)
		oldPos := d.readPos
		d.readPos.virtualEnd += BackendOffset(rn)
		d.readPos.EndOffset.Pos = d.readPos.EndOffset.Pos + int64(rn)
		if err != nil {
			isEnd := err == io.EOF || err == io.ErrUnexpectedEOF
			if isEnd && d.readPos.EndOffset.FileNum < d.endPos.EndOffset.FileNum {
				d.handleReachEnd()
				nsqLog.Logf("DISKQUEUE snapshot(%s): readRaw() read end, try next: %v",
					d.readFrom, d.readPos)
				goto CheckFileOpen
			}
			if isEnd && readOffset == size {
				break
			}
			nsqLog.LogWarningf("DISKQUEUE snapshot(%s): readRaw() read failed: %v, at %v, %v, readed: %v",
				d.readFrom, err.Error(), d.readPos, readOffset, rn)
			d.readFile.Close()
			d.readFile = nil
			return result, err
		}
		if nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("===snapshot read move forward: %v to  %v", oldPos,
				d.readPos)
		}
	}
	return result, nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueueSnapshot) ReadOne() ReadResult {
	d.Lock()
	defer d.Unlock()

	var result ReadResult
	var msgSize int32
	result.Offset = BackendOffset(0)
	if d.readPos == d.endPos {
		result.Err = io.EOF
		return result
	}

CheckFileOpen:

	result.Offset = d.readPos.Offset()
	if d.readFile == nil {
		curFileName := d.fileName(d.readPos.EndOffset.FileNum)
		d.readFile, result.Err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if result.Err != nil {
			if d.readPos.Offset() == d.endPos.Offset() && d.readPos.EndOffset == d.endPos.EndOffset {
				if os.IsNotExist(result.Err) {
					result.Err = io.EOF
				}
			}
			return result
		}

		nsqLog.Debugf("DISKQUEUE(%s): readOne() opened %s", d.readFrom, curFileName)

		if d.readPos.EndOffset.Pos > 0 {
			_, result.Err = d.readFile.Seek(d.readPos.EndOffset.Pos, 0)
			if result.Err != nil {
				d.readFile.Close()
				d.readFile = nil
				return result
			}
		}
	}

	if d.readPos.EndOffset.FileNum > d.endPos.EndOffset.FileNum {
		result.Err = errors.New("exceed end of queue")
		return result
	}
	result.Err = binary.Read(d.readFile, binary.BigEndian, &msgSize)
	if result.Err != nil {
		if result.Err == io.EOF && d.readPos.EndOffset.FileNum < d.endPos.EndOffset.FileNum {
			d.handleReachEnd()
			goto CheckFileOpen
		}
		nsqLog.LogWarningf("DISKQUEUE(%s): readOne() read failed %v  at %v, end: %v",
			d.readFrom, result.Err, d.readPos, d.endPos)

		d.readFile.Close()
		d.readFile = nil
		return result
	}

	if msgSize <= 0 || msgSize > MAX_POSSIBLE_MSG_SIZE {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		result.Err = fmt.Errorf("invalid message read size (%d) at offset: %v", msgSize, d.readPos)
		return result
	}

	result.Data = make([]byte, msgSize)
	_, result.Err = io.ReadFull(d.readFile, result.Data)
	if result.Err != nil {
		d.readFile.Close()
		d.readFile = nil
		return result
	}

	result.Offset = d.readPos.Offset()

	totalBytes := int64(4 + msgSize)
	result.MovedSize = BackendOffset(totalBytes)

	oldPos := d.readPos
	d.readPos.EndOffset.Pos = d.readPos.EndOffset.Pos + totalBytes
	result.CurCnt = atomic.AddInt64(&d.readPos.totalMsgCnt, 1)
	d.readPos.virtualEnd += BackendOffset(totalBytes)
	nsqLog.LogDebugf("=== read move forward: %v to %v", oldPos,
		d.readPos)

	return result
}

func (d *DiskQueueSnapshot) CheckDiskQueueReadToEndOK(offset int64, seekCnt int64, endOffset BackendOffset) (int64, int64, error) {
	localErr := d.SeekTo(BackendOffset(offset), seekCnt)
	if localErr != nil {
		return offset, seekCnt, localErr
	}
	// read until end since it may have multi in the last batch
	lastOffset := offset
	lastCnt := seekCnt
	for {
		r := d.ReadOne()
		if r.Err != nil {
			// should not have eof since it will break after last read
			nsqLog.Warningf("check read failed at: %v, err: %s", lastOffset, r.Err)
			return lastOffset, lastCnt, r.Err
		} else {
			if r.Offset+r.MovedSize == endOffset {
				lastOffset = int64(endOffset)
				lastCnt = r.CurCnt
				break
			}
			if r.Offset+r.MovedSize > endOffset {
				err := fmt.Errorf("check read failed, unexpected end offset: %v, %v, %v", r.Offset, r.MovedSize, endOffset)
				nsqLog.Warningf("%s", err)
				return lastOffset, lastCnt, err
			}
			lastOffset = int64(r.Offset + r.MovedSize)
			lastCnt = r.CurCnt
		}
	}
	return lastOffset, lastCnt, nil
}

func (d *DiskQueueSnapshot) handleReachEnd() {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.readPos.EndOffset.FileNum++
	d.readPos.EndOffset.Pos = 0
}

func (d *DiskQueueSnapshot) fileName(fileNum int64) string {
	return GetQueueFileName(d.dataPath, d.readFrom, fileNum)
}
