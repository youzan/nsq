package nsqd

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

var bp sync.Pool
var peekPool sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
	peekPool.New = func() interface{} {
		return make([]Message, MaxWaitingDelayed)
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}

func peekBufPoolGet() []Message {
	return peekPool.Get().([]Message)
}

func peekBufPoolPut(b []Message) {
	peekPool.Put(b)
}

var (
	bufioReaderPool      sync.Pool
	bufioWriter2kPool    sync.Pool
	bufioWriter4kPool    sync.Pool
	bufioWriter8kPool    sync.Pool
	bufioSmallWriterPool sync.Pool
)

func bufioWriterPool(size int) *sync.Pool {
	switch size {
	case 100:
		return &bufioSmallWriterPool
	case 2 << 10:
		return &bufioWriter2kPool
	case 4 << 10:
		return &bufioWriter4kPool
	case 8 << 10:
		return &bufioWriter8kPool
	}
	return nil
}

func NewBufioReader(r io.Reader) *bufio.Reader {
	if v := bufioReaderPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReader(r)
}

func PutBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	bufioReaderPool.Put(br)
}

func newBufioWriterSize(w io.Writer, size int) *bufio.Writer {
	pool := bufioWriterPool(size)
	if pool != nil {
		if v := pool.Get(); v != nil {
			bw := v.(*bufio.Writer)
			bw.Reset(w)
			return bw
		}
	}
	return bufio.NewWriterSize(w, size)
}

func putBufioWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	if pool := bufioWriterPool(bw.Available()); pool != nil {
		pool.Put(bw)
	}
}
