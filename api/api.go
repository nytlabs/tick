package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"

	"github.com/gocql/gocql"
)

type response struct {
	status string
	data   interface{}
}

type keys struct {
	Status string        `json:"status"`
	Keys   []interface{} `json:"keys"`
}

type keyCount struct {
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
}

var session *gocql.Session

func keyDistributionHandler(w http.ResponseWriter, r *http.Request) {
	var k, typ string
	var c, total int64
	var kl []interface{}
	var resp keys
	e := session.Query(`SELECT type, count FROM total_events where type=?`, "event_tracker").Consistency(gocql.One).Scan(&typ, &total)
	if e != nil {
		log.Println(e)
		return
	}
	fmt.Println(total)
	iter := *session.Query(`SELECT key, count FROM distribution`).Iter()
	for iter.Scan(&k, &c) {
		p := float64(c) * 100 / float64(total)
		ctp := keyCount{c, p}
		elem := make(map[string]keyCount)
		elem[k] = ctp
		kl = append(kl, elem)
	}
	resp.Status = "200"
	resp.Keys = kl
	b, err := json.Marshal(resp)
	if err != nil {
		log.Println(err)
		fmt.Fprintf(w, "Error")
	} else {
		fmt.Fprintf(w, string(b[:]))
	}

}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "नमस्ते जगत!")
}

func main() {
	var err error
	fmt.Println(runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())
	http.HandleFunc("/", handler)
	http.HandleFunc("/keydistribution", keyDistributionHandler)
	cluster := gocql.NewCluster("tickdb")
	cluster.Keyspace = "distribution"
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	http.ListenAndServe(":8888", nil)
}
