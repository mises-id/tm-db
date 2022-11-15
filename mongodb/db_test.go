package mongodb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	tmdb "github.com/tendermint/tm-db"
	"github.com/tendermint/tm-db/goleveldb"
	"github.com/tendermint/tm-db/internal/dbtest"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	dbURI  = os.Getenv("MONGO_URL")
	dbName = "test"
	dbPath = "collection"
)

func BenchmarkMongoDBBsonReadsWrites(b *testing.B) {
	db, err := NewDB(dbURI, dbName, dbPath)
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	b.StopTimer()

	// create dummy data
	const numItems = int64(1000000)
	internal := map[int64]bson.D{}
	for i := 0; i < int(numItems); i++ {
		internal[int64(i)] = bson.D{
			{"node_value", int64(i)},
			{"node_key", []byte(fmt.Sprintf("test-%d", i))},
			{"node_version", int64(i)},
		}
	}

	// fmt.Println("ok, starting")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		// Write something
		{
			idx := rand.Int63n(numItems) // nolint: gosec
			val := internal[idx]
			idxBytes := dbtest.Int642Bytes(idx)
			valBytes, err := bson.Marshal(&val)
			if err != nil {
				b.Fatal(b, err)
			}
			// fmt.Printf("Set %X -> %X\n", idxBytes, valBytes)
			err = db.Set(idxBytes, valBytes)
			if err != nil {
				// require.NoError() is very expensive (according to profiler), so check manually
				b.Fatal(b, err)
			}
		}

		// Read something
		{
			idx := rand.Int63n(numItems) // nolint: gosec
			bsonVal := internal[idx]
			idxBytes := dbtest.Int642Bytes(idx)
			valBytes, err := db.Get(idxBytes)
			if err != nil {
				// require.NoError() is very expensive (according to profiler), so check manually
				b.Fatal(b, err)
			}
			// fmt.Printf("Get %X -> %X\n", idxBytes, valBytes)
			valGot, okKey := bsonVal.Map()["node_value"]
			valExp := idx
			if !okKey {
				b.Fatal(b, err)
			}
			if valGot == 0 {
				if !bytes.Equal(valBytes, nil) {
					b.Errorf("Expected %v for %v, got %X", nil, idx, valBytes)
					break
				}
			} else {
				if valExp != valGot.(int64) {
					b.Errorf("Expected %v for %v, got %v", valExp, idx, valGot)
					break
				}
			}
		}

	}
}

func BenchmarkMongoDBRandomReadsWrites(b *testing.B) {
	db, err := NewDB(dbURI, dbName, dbPath)
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	dbtest.BenchmarkRandomReadsWrites(b, db)
}

func BenchmarkMongoDBRandomBatchWrites(b *testing.B) {
	db, err := NewDB(dbURI, dbName, dbPath)
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	batch := db.NewBatch()
	dbtest.BenchmarkRandomBatchWrites(b, db, batch)
}

func golevelDBCreator(name, dir string) (tmdb.DB, error) {
	return goleveldb.NewDB(name, dir)
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	tmdb.RegisterDBCreator(tmdb.GoLevelDBBackend, golevelDBCreator, true)
	if dbURI == "" {
		dbURI = "mongodb://127.0.0.1:27017"
	}
	dbPath, _ = ioutil.TempDir("", dbPath)
	os.Exit(m.Run())
}

func TestMongoDBSetBson(t *testing.T) {

	db, err := NewDB(dbURI, dbName, dbPath)
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

	var bsonvalret bson.D
	err = bson.Unmarshal(value2, &bsonvalret)
	require.NoError(t, err)
	require.Equal(t, "value-3", bsonvalret.Map()["custom"])
}

func TestMongoDBSetByte(t *testing.T) {

	db, err := NewDB(dbURI, dbName, dbPath)
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
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")))
	require.NoError(t, db.Set([]byte("key2"), []byte("value2")))
	require.NoError(t, db.Set([]byte("key3"), []byte("value3")))

	require.NoError(t, db.Set([]byte("something"), []byte("someval")))
	require.NoError(t, db.Set([]byte("k"), []byte("kval")))
	require.NoError(t, db.Set([]byte("ke"), []byte("keval")))
	require.NoError(t, db.Set([]byte("kee"), []byte("keeval")))
}

func TestMongoDBIterator(t *testing.T) {

	db, err := NewDB(dbURI, dbName, dbPath)
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	mockDBWithStuff(t, db)

	pstart := []byte("key")
	pend := []byte("kez")
	itr, err := db.Iterator(pstart, pend)
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

	db, err := NewDB(dbURI, dbName, dbPath)
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	mockDBWithStuff(t, db)
	pstart := []byte("key")
	pend := []byte("kez")
	itr, err := db.ReverseIterator(pstart, pend)
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

func TestMongoDBBatchSetBson(t *testing.T) {

	db, err := NewDB(dbURI, dbName, dbPath)
	require.NoError(t, err)
	defer db.Close()
	err = db.DeleteAll()
	require.NoError(t, err)
	batch := db.NewBatch()
	defer batch.Close()
	key1 := []byte("key-2")
	var bsonval = bson.D{{"custom", "value-3"}}
	bsonbytes, _ := bson.Marshal(bsonval)

	batch.Set(key1, bsonbytes)
	batch.WriteSync()

	value2, err := db.Get(key1)
	require.NoError(t, err)
	fmt.Println(value2)

	var bsonvalret bson.D
	err = bson.Unmarshal(value2, &bsonvalret)
	require.NoError(t, err)
	require.Equal(t, "value-3", bsonvalret.Map()["custom"])
	fmt.Println(bsonvalret)
}
