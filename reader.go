package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/gocql/gocql"
	"github.com/nytlabs/gojsonexplode"
)

func loop(inChan chan *nsq.Message) {
	count := 0
	unmarshaled := make(map[string]interface{})
	outChan := make(chan string, 1)
	go insertMap(outChan)
	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case m := <-inChan:
			exploded, err := gojsonexplode.Explodejson(m.Body, "||")
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = json.Unmarshal(exploded, &unmarshaled)
			if err != nil {
				fmt.Println(err)
				continue
			}
			for k := range unmarshaled {
				outChan <- k
			}

			count++
			m.Finish()
		case <-tick.C:
			fmt.Println(count)
		}
	}
}

func insertMap(inChan chan string) {
	cluster := gocql.NewCluster("localhost:49171")
	cluster.Keyspace = "distribution"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println(err)
	}
	defer session.Close()
	for {
		select {
		case m := <-inChan:
			err = session.Query("UPDATE et_totals set count=count+1 WHERE key=?", m).Exec()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func main() {
	var reader *nsq.Consumer
	var err error
	inChan := make(chan *nsq.Message, 1)
	lookup := "localhost:4161"
	conf := nsq.NewConfig()
	conf.Set("maxInFlight", 1000)
	reader, err = nsq.NewConsumer("page", "tick#ephemeral", conf)
	if err != nil {
		fmt.Println(err)
	}
	reader.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		inChan <- m
		return nil
	}))
	err = reader.ConnectToNSQLookupd(lookup)
	if err != nil {
		fmt.Println(err)
	}
	go loop(inChan)
	<-reader.StopChan
}
