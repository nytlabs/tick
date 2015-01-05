package main

import (
	"encoding/json"
	"flag"
	//"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/gocql/gocql"
	"github.com/nytlabs/gojsonexplode"
)

var cluster *gocql.ClusterConfig
var session *gocql.Session
var stream *string
var keymap map[string]bool

func loop(inChan chan *nsq.Message) {
	count := 0
	unmarshaled := make(map[string]interface{})
	outChan := make(chan string, 1000)
	countChan := make(chan string, 1000)
	tsChan := make(chan struct {
		k string
		v interface{}
	}, 65535)
	go insertMap(outChan)
	go insertTotal(countChan)
	go insertData(tsChan)
	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case m := <-inChan:
			exploded, err := gojsonexplode.Explodejson(m.Body, "_")
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
				if !strings.ContainsAny(k, "  .") {
					outChan <- k
					tsChan <- struct {
						k string
						v interface{}
					}{k, v}
				}
			}

			count++
			m.Finish()
		case <-tick.C:
			log.Printf("read %d events\n", count)
		}
	}
}

func insertData(inChan chan struct {
	k string
	v interface{}
}) {

	//tick := time.NewTicker(5000 * time.Millisecond)
	var insertmap map[string]map[string]int
	insertmap = make(map[string]map[string]int)
	//count := 0
	for {
		val := ""
		select {
		case m := <-inChan:
			//fmt.Println(m.k, m.v)
			switch j := m.v.(type) {
			case bool:
				val = strconv.FormatBool(j)
			case int:
				val = strconv.Itoa(j)
			case float64:
				val = strconv.FormatFloat(j, 'f', 2, 64)
			case string:
				val = j
			default:
				//do not worry about this
			}

			var ok bool
			if val != "" {
				_, ok = insertmap[m.k]
				if !ok {
					innermap := make(map[string]int)
					innermap[val] = 1
					insertmap[m.k] = innermap
				} else {
					insertmap[m.k][val] = insertmap[m.k][val] + 1

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
				err := session.Query("UPDATE tick.total_events set count=count+? WHERE type=?", v, k).Exec()
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
				// get current minute
				err := session.Query("UPDATE tick.distribution set count=count+? WHERE key=?", v, k).Exec()
				if err != nil {
					log.Println(err)
				} else {
					delete(insertmap, k)
				}
			}
		}

	}
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
	keymap = make(map[string]bool)

	inChan := make(chan *nsq.Message, 100)
	lookup := "10.238.154.138:4150"
	//lookup := "ec2-50-17-119-19.compute-1.amazonaws.com:4161"
	conf := nsq.NewConfig()
	//conf.Set("maxInFlight", 1000)
	conf.MaxInFlight = 1000
	conf.MsgTimeout = 10 * time.Second
	cluster = gocql.NewCluster("10.152.146.16")
	cluster.Keyspace = "tick"
	cluster.Consistency = gocql.One
	cluster.Timeout = 1 * time.Second
	cluster.NumConns = 1
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
