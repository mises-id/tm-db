package mongodb

import (
	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
)

type mongoDBIterator struct {
	db        *MongoDB
	source    tmdb.Iterator
	start     []byte
	end       []byte
	isInvalid bool
}

var _ tmdb.Iterator = (*mongoDBIterator)(nil)

func newMongoDBIterator(db *MongoDB, source tmdb.Iterator, start, end []byte) *mongoDBIterator {
	itr := mongoDBIterator{
		db:        db,
		source:    source,
		start:     start,
		end:       end,
		isInvalid: false,
	}
	return &itr
}

// Domain implements Iterator.
func (itr *mongoDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *mongoDBIterator) Valid() bool {

	// // Once invalid, forever invalid.
	// if itr.isInvalid {
	// 	return false
	// }

	// // If source errors, invalid.
	// if err := itr.Error(); err != nil {
	// 	itr.isInvalid = true
	// 	return false
	// }

	return itr.source.Valid()
}

// Key implements Iterator.
func (itr *mongoDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertIsValid()

	return itr.source.Key()
}

// Value implements Iterator.
func (itr *mongoDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertIsValid()

	var index IndexDoc
	err := bson.Unmarshal(itr.source.Value(), &index)
	if err != nil {
		panic(err)
	}
	if index.CollectionName == "" {
		return cp(index.Value)
	}

	value, err := itr.db.GetRaw(&index)
	if err != nil {
		panic(err)
	}

	return cp(value)
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Next implements Iterator.
func (itr *mongoDBIterator) Next() {
	itr.assertIsValid()
	itr.source.Next()
}

// Error implements Iterator.
func (itr *mongoDBIterator) Error() error {
	return itr.source.Error()
}

// Close implements Iterator.
func (itr *mongoDBIterator) Close() error {
	return itr.source.Close()
}

func (itr mongoDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
