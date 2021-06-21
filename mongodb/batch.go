package mongodb

import (
	tmdb "github.com/tendermint/tm-db"
)

type mongoDBBatch struct {
	db    *MongoDB
}

var _ tmdb.Batch = (*mongoDBBatch)(nil)

func newMongoDBBatch(db *MongoDB) *mongoDBBatch {
	return &mongoDBBatch{
		db:    db,
	}
}

// Set implements Batch.
func (b *mongoDBBatch) Set(key, value []byte) error {
	// TODO:
	return tmdb.ErrKeyEmpty
}

// Delete implements Batch.
func (b *mongoDBBatch) Delete(key []byte) error {
	// TODO:
	return tmdb.ErrKeyEmpty
}

// Write implements Batch.
func (b *mongoDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *mongoDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *mongoDBBatch) write(sync bool) error {
	// TODO:
	return tmdb.ErrBatchClosed
}

// Close implements Batch.
func (b *mongoDBBatch) Close() error {
	// TODO:
	return nil
}

