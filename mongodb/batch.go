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
	index *IndexDoc
}
type mongoDBBatch struct {
	mongoDb      *mongo.Database
	indexDb      tmdb.DB
	indexBatch   tmdb.Batch
	batchs       map[string][]*mongoWriteOp
	writeTracker tmdb.TrackWriteListener
}

var _ tmdb.Batch = (*mongoDBBatch)(nil)

func newMongoDBBatch(database *mongo.Database, indexDb tmdb.DB, tracker tmdb.TrackWriteListener) *mongoDBBatch {
	return &mongoDBBatch{
		mongoDb:      database,
		indexDb:      indexDb,
		indexBatch:   indexDb.NewBatch(),
		batchs:       map[string][]*mongoWriteOp{},
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

	index, bsonval := decodeValue(value)
	indexValue, err := bson.Marshal(&index)
	if err != nil {
		return err
	}
	err = b.indexBatch.Set(key, indexValue)
	if err != nil {
		return err
	}

	if index.CollectionName != "" {
		batch, ok := b.batchs[index.CollectionName]
		if !ok {
			batch = []*mongoWriteOp{}
		}
		op := mongoWriteOp{
			key:   key,
			value: value,
			index: index,
			wm: mongo.NewUpdateOneModel().SetFilter(
				bson.D{
					{"node_key", index.NodeKey},
					{"node_version", index.NodeVersion},
				},
			).SetUpdate(bson.D{{
				"$set", bsonval,
			}}).SetUpsert(true),
		}
		b.batchs[index.CollectionName] = append(batch, &op)
	}
	return nil
}

// Delete implements Batch.
func (b *mongoDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return tmdb.ErrKeyEmpty
	}
	return b.indexBatch.Delete(key)

}

// Write implements Batch.
func (b *mongoDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *mongoDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *mongoDBBatch) doWrite(collection *mongo.Collection, batch []*mongoWriteOp, sync bool, callback bool) error {
	wms := []mongo.WriteModel{}
	for _, op := range batch {
		wms = append(wms, op.wm)
	}
	_, err := collection.BulkWrite(context.Background(), wms)
	if b.writeTracker != nil {
		for _, op := range batch {
			delete := false
			if op.value == nil {
				delete = true
			}
			if callback {
				b.writeTracker.OnWrite(op.key, op.value, delete)
			}

		}
	}
	return err
}
func (b *mongoDBBatch) write(sync bool) error {
	for collectionName, batch := range b.batchs {
		collection := b.mongoDb.Collection(collectionName)
		err := b.doWrite(collection, batch, sync, false)
		if err != nil {
			return err
		}
	}

	var err error
	if sync {
		err = b.indexBatch.Write()
	} else {
		err = b.indexBatch.WriteSync()
	}

	b.indexBatch = b.indexDb.NewBatch()

	return err
}

// Close implements Batch.
func (b *mongoDBBatch) Close() error {

	b.batchs = map[string][]*mongoWriteOp{}
	return b.indexBatch.Close()
}
