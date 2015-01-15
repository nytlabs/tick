package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/gocql/gocql"
)

var session *gocql.Session

func main() {
	var err error
	mux := pat.New()
	mux.Get("/", http.HandlerFunc(namaste))
	mux.Get("/distribution/:stream", http.HandlerFunc(getDistribution))
	http.Handle("/", mux)
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "tick"
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	log.Println("Listening...")
	http.ListenAndServe(":8888", nil)
}
func getDistribution(w http.ResponseWriter, r *http.Request) {

	type typeCount struct {
		Count   int64   `json:"count"`
		Percent float64 `json:"percent"`
	}
	type typeInfo struct {
		B typeCount `json:"boolean"`
		N typeCount `json:"number"`
		S typeCount `json:"string"`
	}

	type key struct {
		Key      string   `json:"key"`
		Count    int64    `json:"count"`
		Percent  float64  `json:"percent_appears"`
		TypeInfo typeInfo `json:"type_info"`
	}

	type distResponse struct {
		TotalEvents int64 `json:"total_events"`
		Keys        []key `json:"keys"`
	}
	var total, c, bc, nc, sc int64 //counts
	var k string                   //key or attribute in the distribution
	var kl []key                   //list of keys
	log.Println("Getting Distribution")
	params := r.URL.Query()
	stream := params.Get(":stream")
	// get total events
	e := session.Query(`SELECT count FROM total_events where stream=?`, stream).Consistency(gocql.One).Scan(&total)
	if e != nil {
		log.Println("Error getting total events")
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}
	iter := *session.Query(`SELECT key, count, typ_bool_count, typ_num_count, typ_str_count FROM distribution WHERE stream=?`, stream).Iter()
	for iter.Scan(&k, &c, &bc, &nc, &sc) {
		pb := float64(bc) * 100 / float64(c)
		pn := float64(nc) * 100 / float64(c)
		ps := float64(sc) * 100 / float64(c)
		typBool := typeCount{bc, pb}
		typNum := typeCount{nc, pn}
		typStr := typeCount{sc, ps}

		typInfo := typeInfo{typBool, typNum, typStr}
		p := float64(c) * 100 / float64(total)
		elem := key{k, c, p, typInfo}
		kl = append(kl, elem)

	}
	resp := distResponse{total, kl}
	b, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(b)
	//w.Write([]byte("Hello " + name))
}

func namaste(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "नमस्ते जगत!")
}
