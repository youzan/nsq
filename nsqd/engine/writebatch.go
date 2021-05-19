package engine

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

type WriteBatch interface {
	Destroy()
	Clear()
	DeleteRange(start, end []byte)
	Delete(key []byte)
	Put(key []byte, value []byte)
	Commit() error
}

type pebbleWriteBatch struct {
	wb *pebble.Batch
	wo *pebble.WriteOptions
	db *pebble.DB
}

func newPebbleWriteBatch(db *pebble.DB, wo *pebble.WriteOptions) *pebbleWriteBatch {
	return &pebbleWriteBatch{
		wb: db.NewBatch(),
		wo: wo,
		db: db,
	}
}

func (wb *pebbleWriteBatch) Destroy() {
	wb.wb.Close()
}

func (wb *pebbleWriteBatch) Clear() {
	wb.wb.Close()
	wb.wb = wb.db.NewBatch()
	// TODO: reuse it
	//wb.wb.Reset()
}

func (wb *pebbleWriteBatch) DeleteRange(start, end []byte) {
	wb.wb.DeleteRange(start, end, wb.wo)
}

func (wb *pebbleWriteBatch) Delete(key []byte) {
	wb.wb.Delete(key, wb.wo)
}

func (wb *pebbleWriteBatch) Put(key []byte, value []byte) {
	wb.wb.Set(key, value, wb.wo)
}

func (wb *pebbleWriteBatch) Commit() error {
	if wb.db == nil || wb.wo == nil {
		return errors.New("nil db or options")
	}
	return wb.db.Apply(wb.wb, wb.wo)
}
