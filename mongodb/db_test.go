package mongodb

import (
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tm-db/internal/dbtest"
	"testing"
)

func BenchmarkMongoDBRandomReadsWrites(b *testing.B) {
	db, err := NewDB("mongodb://localhost:27017", "test", "collection")
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	dbtest.BenchmarkRandomReadsWrites(b, db)
}

func BenchmarkMongoDBRandomBatchWrites(b *testing.B) {
	db, err := NewDB("mongodb://localhost:27017", "test", "collection")
	require.Nil(b, err)
	defer db.Close()

	db.DeleteAll()
	batch := db.NewBatch()
	dbtest.BenchmarkRandomBatchWrites(b, db, batch)
}