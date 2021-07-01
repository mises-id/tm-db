package mongodb

import (
	"context"
	"fmt"
	"time"

	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// type Doc struct {
// 	Key string
// }

var _ tmdb.DB = (*MongoDB)(nil)

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

// Get implements DB.
func (db *MongoDB) Get(key []byte) ([]byte, error) {
	//var result Doc
	filter := bson.M{"key": key}
	single := db.collection.FindOne(context.Background(), filter)
	// err := single.Decode(&result)
	// if err == nil && result.Value != "" {

	// 	return []byte(result.Value), nil
	// }

	rawResult, err := single.DecodeBytes()
	if err == nil {
		var bsonvalret bson.D
		err = bson.Unmarshal(rawResult, &bsonvalret)

		if val, ok := bsonvalret.Map()["value"]; ok {
			//fmt.Printf("Underlying Type: %T\n", val)
			//fmt.Printf("Underlying Value: %v\n", val)
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
	filter := bson.D{{"key", key}}
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

	filter := bson.D{{"key", key}}

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
		"$lt":  end,
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
		"$lt":  end,
	}}

	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"key", -1}})

	cursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, err
	}

	return newMongoDBIterator(cursor, start, end), nil
}
