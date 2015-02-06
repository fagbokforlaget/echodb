// Collection data structure for database
package db

import (
	"encoding/json"
	"fmt"
	"github.com/fagbokforlaget/echodb/dbcore"
	"github.com/fagbokforlaget/echodb/dbwebsocket"
    "github.com/applift/gouuid"
	"os"
	"path"
	"strconv"
)

const (
	INDEX_FILE = "_idx"
)

type Collection struct {
	db    *Database
	name  string
	parts []*dbcore.Partition
}

// Hash a string using sdbm algorithm.
func StrHash(str string) int {
    var hash int
    for _, c := range str {
        hash = int(c) + (hash << 6) + (hash << 16) - hash
    }
    if hash < 0 {
        return -hash
    }
    return hash
}

// Generate id for documents
func GenerateUUID4() *uuid.UUID {
    u4, err := uuid.NewV4()
    if err != nil {
        return u4
    }
    return u4
}

// Bootstrap collections and loads necessary things
func OpenCollection(db *Database, name string) (*Collection, error) {
	collection := &Collection{db: db, name: name}
	return collection, collection.bootstrap()
}

func (col *Collection) bootstrap() error {
	if err := os.MkdirAll(path.Join(col.db.path, col.name), 0700); err != nil {
		return err
	}
	col.parts = make([]*dbcore.Partition, col.db.numParts)

	for i := 0; i < col.db.numParts; i++ {
		var err error
		if col.parts[i], err = dbcore.OpenPartition(
			path.Join(col.db.path, col.name, col.name+"."+strconv.Itoa(i)),
			path.Join(col.db.path, col.name, INDEX_FILE+"."+strconv.Itoa(i))); err != nil {
			return err
		}
	}
	return nil
}

func (col *Collection) close() error {
	for i := 0; i < col.db.numParts; i++ {
		col.parts[i].Lock.Lock()
		col.parts[i].Close()
		col.parts[i].Lock.Unlock()
	}
	return nil
}

// Counts number of documents in collection
func (col *Collection) Count() int {
	col.db.access.RLock()
	defer col.db.access.RUnlock()

	count := 0
	for _, part := range col.parts {
		part.Lock.RLock()
		count += part.ApproxDocCount()
		part.Lock.RUnlock()
	}
	return count
}

// Insert a document into the collection.
func (col *Collection) Insert(doc map[string]interface{}) (id string, err error) {
    _, ok := doc["_id"]
    if ok == false {
        id = fmt.Sprint(GenerateUUID4())
	    doc["_id"] = id
    } else {
        id = doc["_id"].(string)
    }

	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}

	hashKey := StrHash(id)

	partNum := hashKey % col.db.numParts
	col.db.access.RLock()
	part := col.parts[partNum]
	// Put document data into collection
	part.Lock.Lock()
	if _, err = part.Insert(hashKey, []byte(docJS)); err != nil {
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return
	}
	// If another thread is updating the document in the meanwhile, let it take over index maintenance
	if err = part.LockUpdate(hashKey); err != nil {
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return id, nil
	}
	part.UnlockUpdate(hashKey)
	part.Lock.Unlock()
	col.db.access.RUnlock()

	emitDoc(col.name, "create", doc)
	return
}

// Retrieve a document by ID.
func (col *Collection) FindById(id string) (doc map[string]interface{}, err error) {
	col.db.access.RLock()
	defer col.db.access.RUnlock()

    hashKey := StrHash(id)

	part := col.parts[hashKey%col.db.numParts]
	part.Lock.RLock()
	docB, err := part.Read(hashKey)
	part.Lock.RUnlock()
	if err != nil {
		return
	}
	err = json.Unmarshal(docB, &doc)
	return

}

// Cursor to all records in collection
func (col *Collection) All() chan map[string]interface{} {
	count := col.Count()
	c := make(chan map[string]interface{}, count)
	if count == 0 {
		close(c)
		return c
	}
	partDiv := count / col.db.numParts
	for i := 0; i < col.db.numParts; i++ {
		part := col.parts[i]
		for j := 0; j < partDiv; j++ {
			for d := range part.All(j, partDiv) {
				doc := make(map[string]interface{})
				json.Unmarshal(d.Data, &doc)
				c <- doc
			}
		}
	}
	close(c)
	return c
}

// Update a document
func (col *Collection) Update(id string, doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("Updating %d: input doc may not be nil", id)
	}
	docJS, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	col.db.access.RLock()
    hashKey := StrHash(id)
	part := col.parts[hashKey%col.db.numParts]
	part.Lock.Lock()
	// Place lock, read back original document and update
	if err := part.LockUpdate(hashKey); err != nil {
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	originalB, err := part.Read(hashKey)
	if err != nil {
		part.UnlockUpdate(hashKey)
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	var original map[string]interface{}
	if err = json.Unmarshal(originalB, &original); err != nil {
		fmt.Printf("Will not attempt to unindex document %d during update\n", id)
	}
	if err = part.Update(hashKey, []byte(docJS)); err != nil {
		part.UnlockUpdate(hashKey)
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	part.UnlockUpdate(hashKey)
	part.Lock.Unlock()
	col.db.access.RUnlock()

	emitDoc(col.name, "update", doc)
	return nil
}

// Delete a document
func (col *Collection) Delete(id string) error {
	col.db.access.RLock()
    hashKey := StrHash(id)
	part := col.parts[hashKey%col.db.numParts]
	part.Lock.Lock()
	// Place lock, read back original document and delete document
	if err := part.LockUpdate(hashKey); err != nil {
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	originalB, err := part.Read(hashKey)
	if err != nil {
		part.UnlockUpdate(hashKey)
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	var original map[string]interface{}
	if err = json.Unmarshal(originalB, &original); err != nil {
		fmt.Printf("Will not attempt to unindex document %d during delete\n", id)
	}
	if err = part.Delete(hashKey); err != nil {
		part.UnlockUpdate(hashKey)
		part.Lock.Unlock()
		col.db.access.RUnlock()
		return err
	}
	part.UnlockUpdate(hashKey)
	part.Lock.Unlock()
	col.db.access.RUnlock()
	emitDoc(col.name, "delete", map[string]interface{}{"_id": id})
	return nil
}

func emitDoc(name, action string, doc map[string]interface{}) {
	emit := map[string]interface{}{"__action": action, "__doc": doc}
	emitDocJS, err := json.Marshal(emit)
	if err != nil {
		return
	}
	dbwebsocket.Emit(name, emitDocJS)
}
