package engine

import (
	"bytes"
	"fmt"
	"runtime"
)

type IteratorGetter interface {
	GetIterator(IteratorOpts) (Iterator, error)
}

type Iterator interface {
	Next()
	Prev()
	Valid() bool
	Seek([]byte)
	SeekForPrev([]byte)
	SeekToFirst()
	SeekToLast()
	Close()
	RefKey() []byte
	Key() []byte
	RefValue() []byte
	Value() []byte
	NoTimestamp(vt byte)
}
type emptyIterator struct {
}

func (eit *emptyIterator) Valid() bool {
	return false
}

func (eit *emptyIterator) Next() {
}
func (eit *emptyIterator) Prev() {
}
func (eit *emptyIterator) Seek([]byte) {
}
func (eit *emptyIterator) SeekForPrev([]byte) {
}
func (eit *emptyIterator) SeekToFirst() {
}
func (eit *emptyIterator) SeekToLast() {
}
func (eit *emptyIterator) Close() {
}
func (eit *emptyIterator) RefKey() []byte {
	return nil
}
func (eit *emptyIterator) Key() []byte {
	return nil
}
func (eit *emptyIterator) RefValue() []byte {
	return nil
}
func (eit *emptyIterator) Value() []byte {
	return nil
}
func (eit *emptyIterator) NoTimestamp(vt byte) {
}

type Range struct {
	Min  []byte
	Max  []byte
	Type uint8
}

type Limit struct {
	Offset int
	Count  int
}

type IteratorOpts struct {
	Range
	Limit
	Reverse   bool
	IgnoreDel bool
	WithSnap  bool
}

// note: all the iterator use the prefix iterator flag. Which means it may skip the keys for different table
// prefix.
func NewDBRangeLimitIteratorWithOpts(ig IteratorGetter, opts IteratorOpts) (rit *RangeLimitedIterator, err error) {
	var dbit Iterator
	dbit, err = ig.GetIterator(opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := recover(); e != nil {
			dbit.Close()
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			err = fmt.Errorf("init iterator panic: %s:%v", buf, e)
		}
	}()
	if !opts.Reverse {
		rit = NewRangeLimitIterator(dbit, &opts.Range,
			&opts.Limit)
	} else {
		rit = NewRevRangeLimitIterator(dbit, &opts.Range,
			&opts.Limit)
	}
	return
}

// note: all the iterator use the prefix iterator flag. Which means it may skip the keys for different table
// prefix.
func NewDBRangeIteratorWithOpts(ig IteratorGetter, opts IteratorOpts) (rit *RangeLimitedIterator, err error) {
	var dbit Iterator
	dbit, err = ig.GetIterator(opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := recover(); e != nil {
			dbit.Close()
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			err = fmt.Errorf("init iterator panic: %s:%v", buf, e)
		}
	}()
	if !opts.Reverse {
		rit = NewRangeIterator(dbit, &opts.Range)
	} else {
		rit = NewRevRangeIterator(dbit, &opts.Range)
	}
	return
}

type RangeLimitedIterator struct {
	Iterator
	l Limit
	r Range
	// maybe step should not auto increase, we need count for actually element
	step    int
	reverse bool
}

func (it *RangeLimitedIterator) Valid() bool {
	if it.l.Offset < 0 {
		return false
	}
	if it.l.Count >= 0 && it.step >= it.l.Count {
		return false
	}
	if !it.Iterator.Valid() {
		return false
	}

	if !it.reverse {
		if it.r.Max != nil {
			r := bytes.Compare(it.Iterator.RefKey(), it.r.Max)
			if it.r.Type&RangeROpen > 0 {
				return !(r >= 0)
			} else {
				return !(r > 0)
			}
		}
	} else {
		if it.r.Min != nil {
			r := bytes.Compare(it.Iterator.RefKey(), it.r.Min)
			if it.r.Type&RangeLOpen > 0 {
				return !(r <= 0)
			} else {
				return !(r < 0)
			}
		}
	}
	return true
}

func (it *RangeLimitedIterator) Next() {
	it.step++
	if !it.reverse {
		it.Iterator.Next()
	} else {
		it.Iterator.Prev()
	}
}

func NewRangeLimitIterator(i Iterator, r *Range, l *Limit) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, l, false)
}
func NewRevRangeLimitIterator(i Iterator, r *Range, l *Limit) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, l, true)
}
func NewRangeIterator(i Iterator, r *Range) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, false)
}
func NewRevRangeIterator(i Iterator, r *Range) *RangeLimitedIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, true)
}
func rangeLimitIterator(i Iterator, r *Range, l *Limit, reverse bool) *RangeLimitedIterator {
	it := &RangeLimitedIterator{
		Iterator: i,
		l:        *l,
		r:        *r,
		reverse:  reverse,
		step:     0,
	}
	if l.Offset < 0 {
		return it
	}
	if !reverse {
		if r.Min == nil {
			it.Iterator.SeekToFirst()
		} else {
			it.Iterator.Seek(r.Min)
			if r.Type&RangeLOpen > 0 {
				if it.Iterator.Valid() && bytes.Compare(it.Iterator.RefKey(), r.Min) <= 0 {
					it.Iterator.Next()
				}
			}
		}
	} else {
		if r.Max == nil {
			it.Iterator.SeekToLast()
		} else {
			it.Iterator.SeekForPrev(r.Max)
			if !it.Iterator.Valid() {
				it.Iterator.SeekToFirst()
				if it.Iterator.Valid() && bytes.Compare(it.Iterator.RefKey(), r.Max) == 1 {
					dbLog.Infof("iterator seek to last key %v should not great than seek to max %v", it.Iterator.RefKey(), r.Max)
				}
			}
			if r.Type&RangeROpen > 0 {
				if it.Iterator.Valid() && bytes.Compare(it.Iterator.RefKey(), r.Max) >= 0 {
					it.Iterator.Prev()
				}
			}
		}
	}
	for i := 0; i < l.Offset; i++ {
		if !it.Valid() {
			break
		}
		if !it.reverse {
			it.Iterator.Next()
		} else {
			it.Iterator.Prev()
		}
	}
	return it
}
