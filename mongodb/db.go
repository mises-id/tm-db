package mongodb

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB the mongo db implement
type MongoDB struct {
	client     *mongo.Client
	collection *mongo.Collection
}

var _ tmdb.DB = (*MongoDB)(nil)

// NewDB new db instance
func NewDB(uri string, dbName string, collection string) (*MongoDB, error) {
	const connectTimeOut = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeOut)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	database := &MongoDB{
		client:     client,
		collection: client.Database(dbName).Collection(collection),
	}
	return database, nil
}

func makekey(key []byte) string {
	return string(key)
}

// Get implements DB.
func (db *MongoDB) Get(key []byte) ([]byte, error) {
	filter := bson.M{"key": makekey(key)}
	single := db.collection.FindOne(context.Background(), filter)

	rawResult, err := single.DecodeBytes()
	if err == nil {
		var bsonvalret bson.D
		err = bson.Unmarshal(rawResult, &bsonvalret)

		if val, ok := bsonvalret.Map()["value"]; ok {
			return val.(primitive.Binary).Data, nil
		}
		return rawResult, nil
	}
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return nil, err
}

// Has implements DB.
func (db *MongoDB) Has(key []byte) (bool, error) {
	filter := bson.D{{"key", makekey(key)}}
	result := db.collection.FindOne(context.Background(), filter)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, result.Err()
	}

	return true, nil
}

// Set implements DB.
func (db *MongoDB) Set(key []byte, value []byte) error {
	var bsonval bson.D
	err := bson.Unmarshal(value, &bsonval)
	if err != nil {
		bsonval = bson.D{
			{"value", value},
		}
	}
	update := bson.D{
		{"$set", bsonval},
	}

	filter := bson.D{{"key", makekey(key)}}

	opts := &options.UpdateOptions{}
	opts.SetUpsert(true)

	_, err = db.collection.UpdateOne(context.Background(), filter, update, opts)

	return err
}

// SetSync implements DB.
func (db *MongoDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *MongoDB) Delete(key []byte) error {
	filter := bson.D{{"key", makekey(key)}}
	_, err := db.collection.DeleteOne(context.Background(), filter)
	return err
}

// DeleteAll implements DB.
func (db *MongoDB) DeleteAll() error {
	_, err := db.collection.DeleteMany(context.Background(), bson.M{})
	return err
}

// DeleteSync implements DB.
func (db *MongoDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// DB implements DB.
func (db *MongoDB) DB() *mongo.Collection {
	return db.collection
}

// Close implements DB.
func (db *MongoDB) Close() error {
	return db.client.Disconnect(context.Background())
}

// Print implements DB.
func (db *MongoDB) Print() error {
	// TODO:

	return nil
}

// Stats implements DB.
func (db *MongoDB) Stats() map[string]string {
	stats := make(map[string]string)
	count, err := db.collection.EstimatedDocumentCount(context.Background())
	if err == nil {
		stats["mongodb.docs"] = fmt.Sprintf("%d", count)
	}
	return stats
}

// NewBatch implements DB.
func (db *MongoDB) NewBatch() tmdb.Batch {
	return newMongoDBBatch(db.collection)
}

// Iterator implements DB.
func (db *MongoDB) Iterator(start, end []byte) (tmdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}
	cond := bson.M{}
	if start != nil {
		cond["$gte"] = makekey(start)
	}
	if end != nil {
		cond["$lt"] = makekey(end)
	}
	filter := bson.M{"key": cond}

	findOptions := options.Find()
	findOptions.SetSort(bson.M{"key": 1})
	fmt.Printf("Mongo Iterator %s %s", hex.EncodeToString(start), hex.EncodeToString(end))

	cursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}

	itr := newMongoDBIterator(cursor, start, end)
	if itr.Key() == nil {
		itr.Next()
	}
	return itr, nil
}

// ReverseIterator implements DB.
func (db *MongoDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}
	cond := bson.M{}
	if start != nil {
		cond["$gte"] = makekey(start)
	}
	if end != nil {
		cond["$lt"] = makekey(end)
	}
	filter := bson.M{"key": cond}

	findOptions := options.Find()
	findOptions.SetSort(bson.M{"key": -1})

	cursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}

	itr := newMongoDBIterator(cursor, start, end)
	if itr.Key() == nil {
		itr.Next()
	}
	return itr, nil
}
