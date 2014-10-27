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
	outChan := make(chan string, 100)
	countChan := make(chan string, 100)
	//for i := 0; i < 50; i++ {
	go insertMap(outChan)
	//}
	//for i := 0; i < 10; i++ {
	go insertTotal(countChan)
	//}
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

func insertdata(string) {
}

func insertMap(inChan chan string) {
	tick := time.NewTicker(10 * time.Second)
	var insertmap map[string]int
	insertmap = make(map[string]int)
	for {
		select {
		case m := <-inChan:
			insertmap[m] = insertmap[m] + 1
		case <-tick.C:
			//loop through map and insert data here
			for k, v := range insertmap {
				err := session.Query("UPDATE et_totals set count=count+? WHERE key=?", v, k).Exec()
				if err != nil {
					log.Println(k + " : Is the EOF here?")
					log.Println(err)
				} else {
					delete(insertmap, k)
				}
			}
		}

	}
}

func insertTotal(inChan chan string) {
	tick := time.NewTicker(10 * time.Second)
	var insertmap map[string]int
	insertmap = make(map[string]int)
	for {
		select {
		case m := <-inChan:
			insertmap[m] = insertmap[m] + 1
		case <-tick.C:
			//loop through map and insert data here
			for k, v := range insertmap {
				err := session.Query("UPDATE event_count set count=count+? WHERE type=?", v, k).Exec()
				if err != nil {
					log.Println(k + " : Is the EOF here?")
					log.Println(err)
				} else {
					delete(insertmap, k)
				}
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
	inChan := make(chan *nsq.Message, 100)
	lookup := "10.238.154.138:4150"
	//lookup := "ec2-50-17-119-19.compute-1.amazonaws.com:4161"
	conf := nsq.NewConfig()
	conf.Set("maxInFlight", 1000)
	cluster = gocql.NewCluster("10.152.146.16")
	cluster.Keyspace = "distribution"
	cluster.Consistency = gocql.One
	session, err = cluster.CreateSession()
	if err != nil {
		log.Println(err)
		log.Println("why?")
	}
	defer session.Close()
	reader, err = nsq.NewConsumer("page", "tick#ephemeral", conf)
	if err != nil {
		log.Println(err)
		log.Println("why")
	}
	reader.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		inChan <- m
		return nil
	}))
	err = reader.ConnectToNSQD(lookup)
	if err != nil {
		log.Println(err)
	}
	loop(inChan)
	<-reader.StopChan
	//session.Close()
}
