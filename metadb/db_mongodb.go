// +build mongodb

package metadb

import (
	tmdb "github.com/tendermint/tm-db"
	"github.com/tendermint/tm-db/mongodb"
)

func mongoDBCreator(name, dir string) (tmdb.DB, error) {
	return mongodb.NewDB(name, dir, name)
}

func init() { registerDBCreator(tmdb.MongoDBBackend, mongoDBCreator, true) }
