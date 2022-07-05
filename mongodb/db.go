package mongodb

import (
	"context"
	"strings"
	"time"

	db "github.com/tendermint/tm-db"
	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB the mongo db implement
type MongoDB struct {
	client       *mongo.Client
	mongoDB      *mongo.Database
	indexDB      tmdb.DB
	collections  map[string]*mongo.Collection
	writeTracker tmdb.TrackWriteListener
}

var _ tmdb.DB = (*MongoDB)(nil)

type IndexDoc struct {
	//Key            string
	Value          []byte
	CollectionName string
	NodeDocKeys    string
	NodeKey        []byte
	NodeVersion    int64
}

// NewDB new db instance
func NewDB(uri string, dbName string, dir string) (*MongoDB, error) {
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
	db := client.Database(dbName)

	collections := make(map[string]*mongo.Collection)

	indexDB, err := tmdb.NewDB(dbName, tmdb.GoLevelDBBackend, dir)
	if err != nil {
		return nil, err
	}
	database := &MongoDB{
		client:      client,
		mongoDB:     db,
		indexDB:     indexDB,
		collections: collections,
	}
	return database, nil
}

func makeKey(key []byte) string {
	return string(key)
}
func decodeValue(value []byte) (*IndexDoc, bson.D) {
	index := IndexDoc{}
	var bsonval bson.D
	err := bson.Unmarshal(value, &bsonval)
	if err != nil {
		bsonval = bson.D{
			{"value", value},
		}
		index.Value = value
	} else {
		valKey, okKey := bsonval.Map()["node_key"]
		valVersion, okVersion := bsonval.Map()["node_version"]
		if okKey && okVersion {
			nodeKey := valKey.(primitive.Binary).Data
			parts := strings.Split(string(nodeKey), "-")
			index.CollectionName = parts[0]
			index.NodeKey = nodeKey
			index.NodeVersion = valVersion.(int64)
			index.NodeDocKeys = ""
			for _, ele := range bsonval {
				if len(index.NodeDocKeys) > 0 {
					index.NodeDocKeys += "," + ele.Key
				} else {
					index.NodeDocKeys = ele.Key
				}
			}
		} else {
			bsonval = bson.D{
				{"value", value},
			}
			index.Value = value
		}

	}
	return &index, bsonval
}

func (db *MongoDB) GetIndex(key []byte) (*IndexDoc, error) {
	value, err := db.indexDB.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	index := IndexDoc{}
	err = bson.Unmarshal(value, &index)
	if err != nil {
		return nil, err
	}
	return &index, nil
}

func (db *MongoDB) SetIndex(key []byte, value []byte) (*IndexDoc, error) {

	index, _ := decodeValue(value)
	indexValue, err := bson.Marshal(&index)
	if err != nil {
		return nil, err
	}
	err = db.indexDB.Set(key, indexValue)
	if err != nil {
		return nil, err
	}

	return index, nil
}
func (db *MongoDB) GetCollection(name string) *mongo.Collection {
	collection, ok := db.collections[name]
	if !ok {
		collection = db.mongoDB.Collection(name)
		db.collections[name] = collection
	}
	return collection
}

// Get implements DB.
func (db *MongoDB) Get(key []byte) ([]byte, error) {
	index, err := db.GetIndex(key)
	if err != nil {
		return nil, err
	}
	return db.GetRaw(index)
}

func (db *MongoDB) GetRaw(index *IndexDoc) ([]byte, error) {
	if index == nil {
		return nil, nil
	}
	if index.CollectionName == "" {
		return index.Value, nil
	}

	collection := db.GetCollection(index.CollectionName)

	filter := bson.D{
		{"node_key", index.NodeKey},
		{"node_version", index.NodeVersion},
	}
	single := collection.FindOne(context.Background(), filter)
	if single.Err() != nil {
		if single.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, single.Err()
	}

	rawResult, err := single.DecodeBytes()
	if err != nil {
		return nil, err
	}

	var bsonVal bson.D
	err = bson.Unmarshal(rawResult, &bsonVal)
	if err != nil {
		return nil, err
	}
	bsonOrigin := bson.D{}
	docKeys := strings.Split(index.NodeDocKeys, ",")

	for _, docKey := range docKeys {
		val, ok := bsonVal.Map()[docKey]
		if ok {
			bsonOrigin = append(bsonOrigin, primitive.E{Key: docKey, Value: val})
		}

	}

	return bson.Marshal(bsonOrigin)
}

// Has implements DB.
func (db *MongoDB) Has(key []byte) (bool, error) {
	index, err := db.GetIndex(key)
	if err != nil {
		return false, err
	}
	if index == nil {
		return false, nil
	}
	return true, nil
}

// Set implements DB.
func (db *MongoDB) Set(key []byte, value []byte) error {

	index, err := db.SetIndex(key, value)
	if err != nil {
		return err
	}
	if index.CollectionName == "" {
		return nil
	}

	var bsonval bson.D
	err = bson.Unmarshal(value, &bsonval)
	if err != nil {
		return err
	}
	update := bson.D{
		{"$set", bsonval},
	}

	filter := bson.D{
		{"node_key", index.NodeKey},
		{"node_version", index.NodeVersion},
	}

	opts := &options.UpdateOptions{}
	opts.SetUpsert(true)

	collection := db.GetCollection(index.CollectionName)

	_, err = collection.UpdateOne(context.Background(), filter, update, opts)

	if db.writeTracker != nil {
		_ = db.writeTracker.OnWrite(key, value, false)
	}
	return err
}

// SetSync implements DB.
func (db *MongoDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *MongoDB) Delete(key []byte) error {
	index, err := db.GetIndex(key)
	if err != nil {
		return err
	}
	if index == nil {
		return nil
	}

	err = db.indexDB.Delete(key)
	if err != nil {
		return err
	}
	if index.CollectionName != "" {
		filter := bson.D{{"key", makeKey(key)}}
		collection := db.GetCollection(index.CollectionName)
		_, err = collection.DeleteOne(context.Background(), filter)
		if err != nil {
			return err
		}
	}
	if db.writeTracker != nil {
		db.writeTracker.OnWrite(key, nil, true)
	}
	return err
}

// DeleteAll implements DB.
func (db *MongoDB) DeleteAll() error {
	itr, err := db.indexDB.Iterator(nil, nil)
	if err != nil {
		return err
	}
	var keys [][]byte
	for itr.Valid() {
		key := itr.Key()
		keys = append(keys, key)
		itr.Next()
	}
	for _, key := range keys {
		err = db.indexDB.DeleteSync(key)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteSync implements DB.
func (db *MongoDB) DeleteSync(key []byte) error {

	return db.Delete(key)
}

// Raw implements RawDB.
func (db *MongoDB) Raw() interface{} {
	return db.mongoDB
}

// TrackWrite implements TrackableDB.
func (db *MongoDB) TrackWrite(listener db.TrackWriteListener) {
	db.writeTracker = listener
}

// Close implements DB.
func (db *MongoDB) Close() error {
	_ = db.indexDB.Close()
	return db.client.Disconnect(context.Background())
}

// Print implements DB.
func (db *MongoDB) Print() error {
	// TODO:

	return nil
}

// Stats implements DB.
func (db *MongoDB) Stats() map[string]string {
	stats := db.indexDB.Stats()

	return stats
}

// NewBatch implements DB.
func (db *MongoDB) NewBatch() tmdb.Batch {
	return newMongoDBBatch(db.mongoDB, db.indexDB, db.writeTracker)
}

// Iterator implements DB.
func (db *MongoDB) Iterator(start, end []byte) (tmdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}

	cursor, err := db.indexDB.Iterator(start, end)
	if err != nil {
		return nil, err
	}

	itr := newMongoDBIterator(db, cursor, start, end)
	// if itr.Key() == nil {
	// 	itr.Next()
	// }
	return itr, nil
}

// ReverseIterator implements DB.
func (db *MongoDB) ReverseIterator(start, end []byte) (tmdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, tmdb.ErrKeyEmpty
	}
	cursor, err := db.indexDB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}

	itr := newMongoDBIterator(db, cursor, start, end)
	// if itr.Key() == nil {
	// 	itr.Next()
	// }
	return itr, nil
}
