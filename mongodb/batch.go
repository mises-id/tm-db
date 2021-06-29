package mongodb

import (
	"context"
	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoDBBatch struct {
	collection *mongo.Collection
	batch      []mongo.WriteModel
}

var _ tmdb.Batch = (*mongoDBBatch)(nil)

func newMongoDBBatch(collection *mongo.Collection) *mongoDBBatch {
	return &mongoDBBatch{
		collection: collection,
		batch:      []mongo.WriteModel{},
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

	b.batch = append(b.batch, mongo.NewUpdateOneModel().SetFilter(bson.D{{"key", key}}).SetUpdate(bson.D{{
		"$set", bson.D{{"value", value}},
	}}).SetUpsert(true))

	return nil
}

// Delete implements Batch.
func (b *mongoDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}

	b.batch = append(b.batch, mongo.NewDeleteOneModel().SetFilter(bson.D{{"key", key}}))

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
	_, err := b.collection.BulkWrite(context.Background(), b.batch)
	b.batch = []mongo.WriteModel{}
	return err
}

// Close implements Batch.
func (b *mongoDBBatch) Close() error {
	b.batch = []mongo.WriteModel{}
	return nil
}

