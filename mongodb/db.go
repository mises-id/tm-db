package mongodb

import (
	"context"
	"fmt"
	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type MongoDB struct {
	client *mongo.Client
	collection *mongo.Collection
}

type Doc struct {
	Key string
	Value string
}

var _ tmdb.DB = (*MongoDB)(nil)

func NewDB(uri string, dbName string, collection string) (*MongoDB, error) {
	const connectTimeOut = 10*time.Second
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
		client: client,
		collection: client.Database(dbName).Collection(collection),
	}
	return database, nil
}

// Get implements DB.
func (db *MongoDB) Get(key []byte) ([]byte, error) {
	var result Doc
	filter := bson.M{"key": key}
	err := db.collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	return []byte(result.Value), nil
}

// Has implements DB.
func (db *MongoDB) Has(key []byte) (bool, error) {
	var result Doc
	filter := bson.D{{"key", key}}
	err := db.collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Set implements DB.
func (db *MongoDB) Set(key []byte, value []byte) error {
	update := bson.D{
		{"$set", bson.D{
			{"value", value},
		}},
	}
	filter := bson.D{{"key", key}}

	opts := &options.UpdateOptions{}
	opts.SetUpsert(true)

	_, err := db.collection.UpdateOne(context.Background(), filter, update, opts)

	return err
}

// SetSync implements DB.
func (db *MongoDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *MongoDB) Delete(key []byte) error {
	filter := bson.D{{"key", key}}
	_, err := db.collection.DeleteOne(context.Background(), filter)
	return err
}

func (db *MongoDB) DeleteAll() error {
	_, err := db.collection.DeleteMany(context.Background(), bson.M{})
	return err
}

// DeleteSync implements DB.
func (db *MongoDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

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
	filter := bson.M{"key": bson.M{
		"$gte": start,
		"$lt": end,
	}}

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"key", 1}})

	cursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}

	return newMongoDBIterator(cursor, start, end), nil
}

// ReverseIterator implements DB.
func (db *MongoDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}
	filter := bson.M{"key": bson.M{
		"$gte": start,
		"$lt": end,
	}}

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"key", -1}})

	cursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}

	return newMongoDBIterator(cursor, start, end), nil
}

