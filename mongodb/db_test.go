package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tm-db/internal/dbtest"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	dbUri        = "mongodb://root:example@localhost:27017"
	dbName       = "test"
	dbCollection = "collection"
)

func BenchmarkMongoDBRandomReadsWrites(b *testing.B) {
	db, err := NewDB(dbUri, dbName, dbCollection)
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	dbtest.BenchmarkRandomReadsWrites(b, db)
}

func BenchmarkMongoDBRandomBatchWrites(b *testing.B) {
	db, err := NewDB(dbUri, dbName, dbCollection)
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	batch := db.NewBatch()
	dbtest.BenchmarkRandomBatchWrites(b, db, batch)
}

func TestMongoDBSetBson(t *testing.T) {
	db, err := NewDB(dbUri, dbName, dbCollection)
	require.NoError(t, err)
	defer db.Close()

	db.DeleteAll()
	key1 := []byte("key-2")
	var bsonval = bson.D{{"custom", "value-3"}}
	bsonbytes, _ := bson.Marshal(bsonval)

	db.Set(key1, bsonbytes)
	value2, err := db.Get(key1)
	require.NoError(t, err)

	var bsonvalret bson.D
	err = bson.Unmarshal(value2, &bsonvalret)
	require.NoError(t, err)
	require.Equal(t, "value-3", bsonvalret.Map()["custom"])
}

func TestMongoDBSetByte(t *testing.T) {
	db, err := NewDB(dbUri, dbName, dbCollection)
	require.NoError(t, err)
	defer db.Close()

	db.DeleteAll()
	key1 := []byte("key-1")
	db.Set(key1, []byte("value-1"))
	value1, _ := db.Get(key1)
	require.Equal(t, []byte("value-1"), value1)
}
