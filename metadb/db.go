package metadb

import (
	tmdb "github.com/tendermint/tm-db"
)

func registerDBCreator(backend tmdb.BackendType, creator tmdb.DbCreator, force bool) {
	tmdb.RegisterDBCreator(backend, creator, force)
}
