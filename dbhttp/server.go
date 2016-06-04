package dbhttp

import (
	"encoding/json"
	"fmt"
	"github.com/fagbokforlaget/echodb/db"
	"github.com/fagbokforlaget/echodb/dbwebsocket"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"log"
	"net/http"
	"time"
)

func simpleLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		next.ServeHTTP(w, r)
		t2 := time.Now()

		log.Println(r.RemoteAddr, r.Method, r.URL, 200, t2.Sub(t1))
	})
}

func recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic: %+v", err)
				http.Error(w, http.StatusText(500), 500)
			}
		}()

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func setHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// JSON Response

type Response map[string]interface{}

func (r Response) String() (s string) {
	b, err := json.Marshal(r)
	if err != nil {
		s = ""
		return
	}
	s = string(b)
	return
}

func send(w http.ResponseWriter, r *http.Request, json Response) {
	fmt.Fprint(w, json)
}

//Controllers

// root
func rootController(w http.ResponseWriter, r *http.Request) {
	send(w, r, Response{"success": true, "message": "echodb http server is running!"})
}

// list all of collections
func collectionsController(w http.ResponseWriter, r *http.Request) {
	cols := echodb.Collections()
	send(w, r, Response{"success": true, "collections": cols})
}

// get a collection by name
func collectionController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	col := echodb.Get(params["name"])

	send(w, r, Response{"success": true, "count": fmt.Sprintf("%v", col.Count())})
}

// create a collection
func newCollectionController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	status := false
	err := echodb.Create(params["name"])
	if err == nil {
		status = true
	}

	send(w, r, Response{"success": status})
}

// delete a collection
func deleteCollectionController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	status := false
	err := echodb.Delete(params["name"])
	if err == nil {
		status = true
	}
	send(w, r, Response{"success": status})
}

// list documents
func documentsController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	col := echodb.Get(params["name"])

	if col == nil {
		http.Error(w, http.StatusText(404), 404)
		return
	}

	docs := make([]map[string]interface{}, 0)
	for doc := range col.All() {
		docs = append(docs, doc)
	}
	mj, je := json.Marshal(docs)
	if je != nil {
		fmt.Fprint(w, nil)
		return
	}
	fmt.Fprint(w, string(mj))
}

// read document
func documentController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	col := echodb.Get(params["name"])
	// status := false

	id := params["id"]

	doc, err := col.FindById(id)
	if err == nil {
		// status = true
	}
	send(w, r, doc)
}

// read document
func newDocumentController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	var doc map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&doc)
	if err != nil {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	col := echodb.Get(params["name"])

	if col == nil {
		http.Error(w, http.StatusText(404), 404)
		return
	}

	id, docErr := col.Insert(doc)
	if docErr != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	send(w, r, Response{"_id": id})
}

// update document
func updateDocumentController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	var doc map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&doc)
	if err != nil {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	col := echodb.Get(params["name"])

	if col == nil {
		http.Error(w, http.StatusText(404), 404)
		return
	}

	id := params["id"]

	docErr := col.Update(id, doc)
	if docErr != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	send(w, r, doc)
}

// delete document
func deleteDocumentController(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	col := echodb.Get(params["name"])

	if col == nil {
		http.Error(w, http.StatusText(404), 404)
		return
	}

	docErr := col.Delete(params["id"])
	if docErr != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	send(w, r, Response{"_id": params["id"]})
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	dbwebsocket.ServeWs(params["name"], w, r)
}

// ROUTER
func router() {
	stdChain := alice.New(simpleLogger, recoverHandler, setHeaders)

	r := mux.NewRouter()

	r.Handle("/", stdChain.Then(http.HandlerFunc(rootController)))

	// collection routers
	r.Handle("/colls", stdChain.Then(http.HandlerFunc(collectionsController))).Methods("GET")
	r.Handle("/colls/{name}", stdChain.Then(http.HandlerFunc(collectionController))).Methods("GET")
	r.Handle("/colls", stdChain.Then(http.HandlerFunc(newCollectionController))).Methods("POST")
	r.Handle("/colls/{name}", stdChain.Then(http.HandlerFunc(deleteCollectionController))).Methods("DELETE")

	// document routers
	r.Handle("/colls/{name}/docs", stdChain.Then(http.HandlerFunc(documentsController))).Methods("GET")
	r.Handle("/colls/{name}/docs/{id}", stdChain.Then(http.HandlerFunc(documentController))).Methods("GET")
	r.Handle("/colls/{name}/docs", stdChain.Then(http.HandlerFunc(newDocumentController))).Methods("POST")
	r.Handle("/colls/{name}/docs/{id}", stdChain.Then(http.HandlerFunc(updateDocumentController))).Methods("PUT")
	r.Handle("/colls/{name}/docs/{id}", stdChain.Then(http.HandlerFunc(deleteDocumentController))).Methods("DELETE")

	r.Handle("/ws/{name}", http.HandlerFunc(serveWs)).Methods("GET")

	http.Handle("/", r)
	return
}

var echodb *db.Database

// main function
func Start() {
	echodb, _ = db.OpenDatabase("/tmp/echodb")

	router()
	port := ":8001"
	log.Println("[HTTP Server]", port)
	http.ListenAndServe(port, nil)
}
