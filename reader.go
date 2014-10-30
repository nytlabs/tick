package main

import (
	"encoding/json"
	"flag"
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
var stream *string

func loop(inChan chan *nsq.Message) {
	count := 0
	unmarshaled := make(map[string]interface{})
	outChan := make(chan string, 100)
	countChan := make(chan string, 100)
	tsChan := make(chan struct {
		k string
		v string
	}, 100)
	go insertMap(outChan)
	go insertTotal(countChan)
	go insertData(tsChan)
	tick := time.NewTicker(30 * time.Second)
	for {
		select {
		case m := <-inChan:
			exploded, err := gojsonexplode.Explodejson(m.Body, ".")
			if err != nil {
				log.Println(err)
				continue
			}
			countChan <- *stream
			err = json.Unmarshal(exploded, &unmarshaled)
			if err != nil {
				log.Println(err)
				continue
			}
			for k, v := range unmarshaled {
				outChan <- k
				tsChan <- struct {
					k string
					v string
				}{k, v}
			}

			count++
			m.Finish()
		case <-tick.C:
			fmt.Printf("inserted %d events\n", count)
		}
	}
}

func insertData(inChan chan struct {
	k string
	v string
}) {

	tick := time.NewTicker(30 * time.Second)
	var insertmap map[string]int
	insertmap = make(map[string]int)
	for {
		select {
		case m := <-inChan:
			insertmap[m] = insertmap[m] + 1
		case <-tick.C:
			//loop through map and insert data here
			for k, v := range insertmap {
				err := session.Query("UPDATE dist_over_time set count=count+? and event_time=? WHERE attr_name=? AND attr_val=?", c, t, k).Exec()
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
	tick := time.NewTicker(30 * time.Second)
	var insertmap map[string]int
	insertmap = make(map[string]int)
	for {
		select {
		case m := <-inChan:
			insertmap[m] = insertmap[m] + 1
		case <-tick.C:
			//loop through map and insert data here
			for k, v := range insertmap {
				err := session.Query("UPDATE total_events set count=count+? WHERE type=?", v, k).Exec()
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

	//parse command line flags
	stream = flag.String("stream", "page", "one of the streams available in NSQ")
	channel := flag.String("channel", "tick", "channel for the tick app")
	ephemeral := flag.Bool("ephemeral", true, "is the channel ephemeral. True by default")

	flag.Parse()

	if *ephemeral {
		*channel = *channel + "#ephemeral"
	}

	inChan := make(chan *nsq.Message, 1000)
	lookup := "10.238.154.138:4150"
	//lookup := "ec2-50-17-119-19.compute-1.amazonaws.com:4161"
	conf := nsq.NewConfig()
	//conf.Set("maxInFlight", 1000)
	conf.MaxInFlight = 1000
	cluster = gocql.NewCluster("10.152.146.16")
	cluster.Keyspace = "tick"
	cluster.Consistency = gocql.One
	session, err = cluster.CreateSession()
	if err != nil {
		log.Println(err)
		log.Println("Error initiating session with database")
	}
	defer session.Close()
	reader, err = nsq.NewConsumer(*stream, *channel, conf)
	if err != nil {
		log.Println(err)
		log.Println("Error connecting to nsq")
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
}
