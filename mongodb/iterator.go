package mongodb

import (
	"context"

	tmdb "github.com/tendermint/tm-db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoDBIterator struct {
	source    *mongo.Cursor
	start     []byte
	end       []byte
	isInvalid bool
}

var _ tmdb.Iterator = (*mongoDBIterator)(nil)

func newMongoDBIterator(source *mongo.Cursor, start, end []byte) *mongoDBIterator {
	itr := mongoDBIterator{
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

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source errors, invalid.
	if err := itr.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	// if itr.source.ID() == 0 {
	// 	itr.isInvalid = true
	// 	return false
	// }

	// If source is invalid, invalid.
	// if !itr.source.TryNext(context.Background()) {
	// 	itr.isInvalid = true
	// 	return false
	// }

	return true
}

// Key implements Iterator.
func (itr *mongoDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertIsValid()
	//var result Doc

	err := itr.source.Err()
	if err != nil {
		panic(err)
	}
	var bsonvalret bson.D
	err = bson.Unmarshal(itr.source.Current, &bsonvalret)

	if key, ok := bsonvalret.Map()["key"]; ok {
		return cp([]byte(key.(string)))
	}

	return nil
}

// Value implements Iterator.
func (itr *mongoDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertIsValid()
	//var result Doc

	err := itr.source.Err()
	if err != nil {
		panic(err)
	}

	var bsonvalret bson.D
	err = bson.Unmarshal(itr.source.Current, &bsonvalret)

	if val, ok := bsonvalret.Map()["value"]; ok {
		return cp(val.(primitive.Binary).Data)
	}

	return cp(itr.source.Current)
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Next implements Iterator.
func (itr *mongoDBIterator) Next() {
	itr.assertIsValid()
	if !itr.source.Next(context.Background()) {
		itr.isInvalid = true
	}
}

// Error implements Iterator.
func (itr *mongoDBIterator) Error() error {
	return itr.source.Err()
}

// Close implements Iterator.
func (itr *mongoDBIterator) Close() error {
	return itr.source.Close(context.Background())
}

func (itr mongoDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
