package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"

	"github.com/gocql/gocql"
)

type keys struct {
}

var session *gocql.Session

func keyDistributionHandler(w http.ResponseWriter, r *http.Request) {
	var key string
	var count int64
	iter := *session.Query(`SELECT key, count FROM et_totals`).Iter()
	for iter.Scan(&key, &count) {
		//fmt.Fprintf(w, key+" : "+count+"<br/>")
		fmt.Fprintf(w, "%s : %d\n", key, count)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello World!")
}

func main() {
	var err error
	fmt.Println(runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())
	http.HandleFunc("/", handler)
	http.HandleFunc("/keydistribution", keyDistributionHandler)
	cluster := gocql.NewCluster("localhost:49171")
	cluster.Keyspace = "distribution"
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	http.ListenAndServe(":9000", nil)
}
