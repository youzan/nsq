package gorpc

import (
	"bufio"
	"compress/flate"
	"encoding/gob"
	"io"
)

// RegisterType registers the given type to send via rpc.
//
// The client must register all the response types the server may send.
// The server must register all the request types the client may send.
//
// There is no need in registering base Go types such as int, string, bool,
// float64, etc. or arrays, slices and maps containing base Go types.
//
// There is no need in registering argument and return value types
// for functions and methods registered via Dispatcher.

var flatePoolInstance = newFlatePool(32)

type flatePool struct {
	deflateChan chan *flate.Writer
	inflateChan chan io.ReadCloser
}

func newFlatePool(poolSize int) *flatePool {
	return &flatePool{
		deflateChan: make(chan *flate.Writer, poolSize),
		inflateChan: make(chan io.ReadCloser, poolSize),
	}
}

func (self *flatePool) GetWriter(w io.Writer) *flate.Writer {
	var fw *flate.Writer
	select {
	case fw = <-self.deflateChan:
		fw.Reset(w)
	default:
		fw, _ = flate.NewWriter(w, flate.BestSpeed)
	}
	return fw
}

func (self *flatePool) PutWriter(w *flate.Writer) {
	if w == nil {
		return
	}
	select {
	case self.deflateChan <- w:
	default:
	}
}

func (self *flatePool) GetReader(r io.Reader) io.ReadCloser {
	var fr io.ReadCloser
	select {
	case fr = <-self.inflateChan:
		fr.(flate.Resetter).Reset(r, nil)
	default:
		fr = flate.NewReader(r)
	}
	return fr
}

func (self *flatePool) PutReader(r io.ReadCloser) {
	if r == nil {
		return
	}
	select {
	case self.inflateChan <- r:
	default:
	}
}

func RegisterType(x interface{}) {
	gob.Register(x)
}

type wireRequest struct {
	ID      uint64
	Request interface{}
}

type wireResponse struct {
	ID       uint64
	Response interface{}
	Error    string
}

type messageEncoder struct {
	e  *gob.Encoder
	bw *bufio.Writer
	zw *flate.Writer
	ww *bufio.Writer
}

func (e *messageEncoder) Close() error {
	if e.zw != nil {
		err := e.zw.Close()
		flatePoolInstance.PutWriter(e.zw)
		return err
	}
	return nil
}

func (e *messageEncoder) Flush() error {
	if e.zw != nil {
		if err := e.ww.Flush(); err != nil {
			return err
		}
		if err := e.zw.Flush(); err != nil {
			return err
		}
	}
	if err := e.bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (e *messageEncoder) Encode(msg interface{}) error {
	return e.e.Encode(msg)
}

func newMessageEncoder(w io.Writer, bufferSize int, enableCompression bool, s *ConnStats) *messageEncoder {
	w = newWriterCounter(w, s)
	bw := bufio.NewWriterSize(w, bufferSize)

	ww := bw
	var zw *flate.Writer
	if enableCompression {
		zw = flatePoolInstance.GetWriter(bw)
		ww = bufio.NewWriterSize(zw, bufferSize)
	}

	return &messageEncoder{
		e:  gob.NewEncoder(ww),
		bw: bw,
		zw: zw,
		ww: ww,
	}
}

type messageDecoder struct {
	d  *gob.Decoder
	zr io.ReadCloser
}

func (d *messageDecoder) Close() error {
	if d.zr != nil {
		err := d.zr.Close()
		flatePoolInstance.PutReader(d.zr)
		return err
	}
	return nil
}

func (d *messageDecoder) Decode(msg interface{}) error {
	return d.d.Decode(msg)
}

func newMessageDecoder(r io.Reader, bufferSize int, enableCompression bool, s *ConnStats) *messageDecoder {
	r = newReaderCounter(r, s)
	br := bufio.NewReaderSize(r, bufferSize)

	rr := br
	var zr io.ReadCloser
	if enableCompression {
		zr = flatePoolInstance.GetReader(br)
		rr = bufio.NewReaderSize(zr, bufferSize)
	}

	return &messageDecoder{
		d:  gob.NewDecoder(rr),
		zr: zr,
	}
}
