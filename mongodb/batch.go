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
	value, err := b.indexDb.Get(key)
	if err == nil && value != nil {
		index := IndexDoc{}
		err = bson.Unmarshal(value, &index)
		if err == nil {
			if index.CollectionName != "" {
				batch, ok := b.batchs[index.CollectionName]
				if !ok {
					batch = []*mongoWriteOp{}
				}
				op := mongoWriteOp{
					key:   key,
					value: nil,
					index: &index,
					wm: mongo.NewDeleteOneModel().SetFilter(
						bson.D{
							{"node_key", index.NodeKey},
							{"node_version", index.NodeVersion},
						},
					),
				}
				b.batchs[index.CollectionName] = append(batch, &op)
			}
		}
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

func (b *mongoDBBatch) doWrite(collection *mongo.Collection, batch []*mongoWriteOp, sync bool) error {
	wms := []mongo.WriteModel{}
	for _, op := range batch {
		wms = append(wms, op.wm)
	}
	_, err := collection.BulkWrite(context.Background(), wms)

	return err
}
func (b *mongoDBBatch) write(sync bool) error {

	var err error
	if sync {
		err = b.indexBatch.Write()
	} else {
		err = b.indexBatch.WriteSync()
	}
	if err != nil {
		return err
	}
	b.indexBatch = b.indexDb.NewBatch()

	for collectionName, batch := range b.batchs {
		collection := b.mongoDb.Collection(collectionName)
		err := b.doWrite(collection, batch, sync)
		if err != nil {
			return err
		}
	}

	if b.writeTracker != nil {
		for _, batch := range b.batchs {
			for _, op := range batch {
				if op.value == nil {
					b.writeTracker.OnDelete(op.key)
				} else {
					b.writeTracker.OnWrite(op.key, op.value)
				}

			}
		}

	}
	b.batchs = map[string][]*mongoWriteOp{}

	return nil
}

// Close implements Batch.
func (b *mongoDBBatch) Close() error {

	b.batchs = map[string][]*mongoWriteOp{}
	return b.indexBatch.Close()
}
