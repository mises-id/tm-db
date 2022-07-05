//

package metadb

import (
	"os"

	tmdb "github.com/tendermint/tm-db"
	"github.com/tendermint/tm-db/mongodb"
)

func mongoDBCreator(name, dir string) (tmdb.DB, error) {
	return mongodb.NewDB(os.Getenv("MONGO_URL"), name, dir)
}

func init() { registerDBCreator(tmdb.MongoDBBackend, mongoDBCreator, true) }
