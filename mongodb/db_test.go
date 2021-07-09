package mongodb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tm-db/internal/dbtest"
	"go.mongodb.org/mongo-driver/bson"
)

func BenchmarkMongoDBRandomReadsWrites(b *testing.B) {
	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	dbtest.BenchmarkRandomReadsWrites(b, db)
}

func BenchmarkMongoDBRandomBatchWrites(b *testing.B) {
	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	batch := db.NewBatch()
	dbtest.BenchmarkRandomBatchWrites(b, db, batch)
}

func TestMongoDBSetBson(t *testing.T) {

	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	key1 := []byte("key-2")
	var bsonval = bson.D{{"custom", "value-3"}}
	bsonbytes, _ := bson.Marshal(bsonval)

	db.Set(key1, bsonbytes)
	value2, err := db.Get(key1)
	require.NoError(t, err)
	//require.Equal(t, "value-3", value2)
	var bsonvalret bson.D
	err = bson.Unmarshal(value2, &bsonvalret)
	require.NoError(t, err)
	require.Equal(t, "value-3", bsonvalret.Map()["custom"])
}

func TestMongoDBSetByte(t *testing.T) {

	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	key1 := []byte("key-1")
	db.Set(key1, []byte("value-1"))
	value1, _ := db.Get(key1)
	require.Equal(t, []byte("value-1"), value1)
}

func mockDBWithStuff(t *testing.T, db *MongoDB) {
	// Under "key" prefix
	require.NoError(t, db.PrefixSet([]byte("key"), []byte("key1"), []byte("value1")))
	require.NoError(t, db.PrefixSet([]byte("key"), []byte("key2"), []byte("value2")))
	require.NoError(t, db.PrefixSet([]byte("key"), []byte("key3"), []byte("value3")))

	require.NoError(t, db.Set([]byte("something"), []byte("someval")))
	require.NoError(t, db.Set([]byte("k"), []byte("kval")))
	require.NoError(t, db.Set([]byte("ke"), []byte("keval")))
	require.NoError(t, db.Set([]byte("kee"), []byte("keeval")))
}

func TestMongoDBIterator(t *testing.T) {

	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	mockDBWithStuff(t, db)

	pstart := []byte("key")
	pend := []byte("key1000000")
	itr, err := db.PrefixIterator(pstart, pstart, pend)
	require.NoError(t, err)
	require.NoError(t, itr.Error())
	dbtest.Valid(t, itr, true)
	dbtest.Domain(t, itr, pstart, pend)
	dbtest.Item(t, itr, []byte("key1"), []byte("value1"))
	dbtest.Next(t, itr, true)
	dbtest.Item(t, itr, []byte("key2"), []byte("value2"))
	dbtest.Next(t, itr, true)
	dbtest.Item(t, itr, []byte("key3"), []byte("value3"))
	dbtest.Next(t, itr, false)
	dbtest.Invalid(t, itr)
	itr.Close()

}

func TestMongoDBReverseIterator(t *testing.T) {

	db, err := NewDB(os.Getenv("MONGO_URL"), "test", "collection")
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	mockDBWithStuff(t, db)
	pstart := []byte("key")
	pend := []byte("key1000000")
	itr, err := db.PrefixReverseIterator(pstart, pstart, pend)
	require.NoError(t, err)
	require.NoError(t, itr.Error())
	dbtest.Valid(t, itr, true)
	dbtest.Domain(t, itr, pstart, pend)
	dbtest.Item(t, itr, []byte("key3"), []byte("value3"))
	dbtest.Next(t, itr, true)
	dbtest.Item(t, itr, []byte("key2"), []byte("value2"))
	dbtest.Next(t, itr, true)
	dbtest.Item(t, itr, []byte("key1"), []byte("value1"))
	dbtest.Next(t, itr, false)
	dbtest.Invalid(t, itr)
	itr.Close()

}
