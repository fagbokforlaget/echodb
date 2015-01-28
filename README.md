EchoDB
===================
Fault-tolerrent "data-on-wire" NoSQL datastore.

* Fault-tolerrant NoSQL [done]
* MMAP based datastore (mostly based on gommap and tiedot wrapper)
  [done]
* Hashtable based indexer (based on tiedot implementation) [done]
* Simple HTTP API to manage collections [done]
* Data on wire (using websocket) [almost done]
* Query Engine [Not Implemented]
* and yes, it's written in 48hrs during
  [GopherGala](http://gophergala.com/) 2015

Install
===================
```
mkdir echodb
cd echodb
export GOPATH=`pwd`
go get github.com/fagbokforlaget/echodb
./bin/echodb
```

HTTP server runs at http://localhost:8001 please see
[server.go](dbhttp/server.go)

Dependencies
======================
```
go get github.com/justinas/alice
go get github.com/gorilla/mux
go get github.com/gorilla/websocket
```

Database API
======================
```
Database.Create(collectionName)
Database.Get(collectionName)
Database.Delete(collectionName)

Collection.FindById(id)
Collection.All()
Collection.Read(id)
Collection.Delete(id)
Collection.Update(id, payload)
```


Current status
==================
Highly experimental

