package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/gocql/gocql"
	"github.com/nytlabs/gojsonexplode"
)

var cluster *gocql.ClusterConfig
var session *gocql.Session

func loop(inChan chan *nsq.Message) {
	count := 0
	unmarshaled := make(map[string]interface{})
	outChan := make(chan string, 1)
	countChan := make(chan string, 1)
	for i := 0; i < 5; i++ {
		go insertMap(outChan)
	}
	go insertTotal(countChan)
	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case m := <-inChan:
			exploded, err := gojsonexplode.Explodejson(m.Body, "||")
			if err != nil {
				log.Println(err)
				continue
			}
			countChan <- "event_tracker"
			err = json.Unmarshal(exploded, &unmarshaled)
			if err != nil {
				log.Println(err)
				continue
			}
			for k := range unmarshaled {
				outChan <- k
			}

			count++
			m.Finish()
		case <-tick.C:
			fmt.Printf("inserted %d events\n", count)
		}
	}
}

func insertMap(inChan chan string) {
	/*
		session, err := cluster.CreateSession()
		if err != nil {
			log.Println(err)
		}
		defer session.Close()
	*/
	for {
		select {
		case m := <-inChan:
			err := session.Query("UPDATE et_totals set count=count+1 WHERE key=?", m).Exec()
			if err != nil {
				log.Println(m + " : Is the EOF here?")
				log.Println(err)
			}
		}
	}
}

func insertTotal(inChan chan string) {
	/*
		session, err := cluster.CreateSession()
		if err != nil {
			log.Println(err)
		}
		defer session.Close()
	*/
	for {
		select {
		case m := <-inChan:
			err := session.Query("UPDATE event_count set count=count+1 WHERE type=?", m).Exec()
			if err != nil {
				log.Println(err)
				log.Println("Error here?")
			}
		}
	}
}

func insertToTimeSeries(inchan chan map[string]interface{}) {

}

func main() {
	var reader *nsq.Consumer
	var err error
	runtime.GOMAXPROCS(runtime.NumCPU())
	inChan := make(chan *nsq.Message, 1)
	lookup := "localhost:4161"
	//lookup := "ec2-50-17-119-19.compute-1.amazonaws.com:4161"
	conf := nsq.NewConfig()
	conf.Set("maxInFlight", 1000)
	cluster = gocql.NewCluster("localhost:49156")
	cluster.Keyspace = "distribution"
	cluster.Consistency = gocql.One
	session, err = cluster.CreateSession()
	if err != nil {
		log.Println(err)
		log.Println("why?")
	}
	//defer session.Close()
	reader, err = nsq.NewConsumer("page", "tick#ephemeral", conf)
	if err != nil {
		log.Println(err)
		log.Println("why")
	}
	reader.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		inChan <- m
		return nil
	}))
	err = reader.ConnectToNSQLookupd(lookup)
	if err != nil {
		log.Println(err)
	}
	loop(inChan)
	<-reader.StopChan
	session.Close()
}
