package mongodb

import (
	"context"

	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoWriteOp struct {
	wm    mongo.WriteModel
	key   []byte
	value []byte
}
type mongoDBBatch struct {
	collection   *mongo.Collection
	batch        []*mongoWriteOp
	writeTracker tmdb.TrackWriteListener
}

var _ tmdb.Batch = (*mongoDBBatch)(nil)

func newMongoDBBatch(collection *mongo.Collection, tracker tmdb.TrackWriteListener) *mongoDBBatch {
	return &mongoDBBatch{
		collection:   collection,
		batch:        []*mongoWriteOp{},
		writeTracker: tracker,
	}
}

// Set implements Batch.
func (b *mongoDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}
	if value == nil {
		return tmdb.ErrValueNil
	}

	var bsonval bson.D
	err := bson.Unmarshal(value, &bsonval)
	if err != nil {
		bsonval = bson.D{
			{"value", value},
		}
	}
	op := mongoWriteOp{
		key:   key,
		value: value,
		wm: mongo.NewUpdateOneModel().SetFilter(
			bson.D{{"key", makekey(key)}},
		).SetUpdate(bson.D{{
			"$set", bsonval,
		}}).SetUpsert(true),
	}

	b.batch = append(b.batch, &op)

	return nil
}

// Delete implements Batch.
func (b *mongoDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}

	op := mongoWriteOp{
		key:   key,
		value: nil,
		wm: mongo.NewDeleteOneModel().SetFilter(
			bson.D{{"key", makekey(key)}},
		),
	}

	b.batch = append(b.batch, &op)

	return nil
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
	wms := []mongo.WriteModel{}
	for _, op := range b.batch {
		wms = append(wms, op.wm)
	}
	_, err := b.collection.BulkWrite(context.Background(), wms)
	if b.writeTracker != nil {
		for _, op := range b.batch {
			delete := false
			if op.value == nil {
				delete = true
			}
			b.writeTracker.OnWrite(op.key, op.value, delete)
		}
	}

	b.batch = []*mongoWriteOp{}
	return err
}

// Close implements Batch.
func (b *mongoDBBatch) Close() error {
	b.batch = []*mongoWriteOp{}
	return nil
}
