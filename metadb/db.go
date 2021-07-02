package metadb

import (
	tmdb "github.com/tendermint/tm-db"
)

func registerDBCreator(backend tmdb.BackendType, creator tmdb.DbCreator, force bool) {
	tmdb.RegisterDBCreator(backend, creator, force)
}
func newDB(name string, backend tmdb.BackendType, dir string) (tmdb.DB, error) {
	return tmdb.NewDB(name, backend, dir)
}
