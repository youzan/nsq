package engine

import (
	"github.com/cockroachdb/pebble"
)

type pebbleIterator struct {
	*pebble.Iterator
	db           *PebbleEng
	opt          *pebble.IterOptions
	snap         *pebble.Snapshot
	removeTsType byte
}

// low_bound is inclusive
// upper bound is exclusive
func newPebbleIterator(db *PebbleEng, opts IteratorOpts) (*pebbleIterator, error) {
	db.rwmutex.RLock()
	if db.IsClosed() {
		db.rwmutex.RUnlock()
		return nil, errDBEngClosed
	}
	upperBound := opts.Max
	lowerBound := opts.Min
	if opts.Type&RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}

	opt := &pebble.IterOptions{}
	opt.LowerBound = lowerBound
	opt.UpperBound = upperBound
	dbit := &pebbleIterator{
		db:  db,
		opt: opt,
	}

	if opts.WithSnap {
		dbit.snap = db.eng.NewSnapshot()
		dbit.Iterator = dbit.snap.NewIter(opt)
	} else {
		dbit.Iterator = db.eng.NewIter(opt)
	}
	return dbit, nil
}

func (it *pebbleIterator) Next() {
	it.Iterator.Next()
}

func (it *pebbleIterator) Prev() {
	it.Iterator.Prev()
}

func (it *pebbleIterator) Seek(key []byte) {
	it.Iterator.SeekGE(key)
}

func (it *pebbleIterator) SeekForPrev(key []byte) {
	it.Iterator.SeekLT(key)
}

func (it *pebbleIterator) SeekToFirst() {
	it.Iterator.First()
}

func (it *pebbleIterator) SeekToLast() {
	it.Iterator.Last()
}

func (it *pebbleIterator) Valid() bool {
	if it.Iterator.Error() != nil {
		return false
	}
	return it.Iterator.Valid()
}

// the bytes returned will be freed after next
func (it *pebbleIterator) RefKey() []byte {
	return it.Iterator.Key()
}

func (it *pebbleIterator) Key() []byte {
	v := it.Iterator.Key()
	vv := make([]byte, len(v))
	copy(vv, v)
	return vv
}

// the bytes returned will be freed after next
func (it *pebbleIterator) RefValue() []byte {
	v := it.Iterator.Value()
	return v
}

func (it *pebbleIterator) Value() []byte {
	v := it.RefValue()
	vv := make([]byte, len(v))
	copy(vv, v)
	return vv
}

func (it *pebbleIterator) NoTimestamp(vt byte) {
	it.removeTsType = vt
}

func (it *pebbleIterator) Close() {
	if it.Iterator != nil {
		it.Iterator.Close()
	}
	if it.snap != nil {
		it.snap.Close()
	}
	it.db.rwmutex.RUnlock()
}
