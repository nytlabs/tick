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
	outChan := make(chan string, 100)
	countChan := make(chan string, 100)
	/*tsChan := make(chan struct {
		k string
		v interface{}
	}, 100)*/
	go insertMap(outChan)
	go insertTotal(countChan)
	//for i := 0; i < 3; i++ {
	//go insertData(tsChan)
	//}
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
			for k, _ := range unmarshaled {
				if !strings.ContainsAny(k, "  .") {
					outChan <- k
					/*
						tsChan <- struct {
							k string
							v interface{}
						}{k, v}
					*/
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

	//tick := time.NewTicker(10 * time.Second)
	//var insertmap map[string]map[string]int
	//insertmap = make(map[string]map[string]int)
	batch := gocql.NewBatch(gocql.CounterBatch)
	stmt := "UPDATE tick.dist_over_time set count=count+? WHERE event_time=? AND attr_name=? AND attr_value=?"
	count := 0
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
			now := time.Now()
			t := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)
			batch.Query(stmt, 1, t, m.k, val)
			count += 1
			if count >= 10000 {
				err := session.ExecuteBatch(batch)
				if err != nil {
					log.Println(err)
				}
				//log.Println("Inserted batch")
				//log.Println(count)
				count = 0
				batch = gocql.NewBatch(gocql.CounterBatch)
			}
		}

	}
}

/*
func batchData(inChan chan struct {
	k string
	v string
}) {
	insertmap := make(map[string]map[string]int)
	count := 0
	select {
	case m := <-inChan:
		_, ok := insertmap[m.k]
		if !ok {
			innermap := make(map[string]int)
			innermap[m.v] = 1
			insertmap[m.k] = innermap
		} else {
			insertmap[m.k][val] = insertmap[m.k][m.v] + 1
		}
		count += 1
		if count >= 100 {
			batchChan <- insertmap
			insertmap = make(map[string]map[string]int)
		}
	}
}
*/
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

	inChan := make(chan *nsq.Message, 1000)
	lookup := "10.238.154.138:4150"
	//lookup := "ec2-50-17-119-19.compute-1.amazonaws.com:4161"
	conf := nsq.NewConfig()
	//conf.Set("maxInFlight", 1000)
	conf.MaxInFlight = 1000
	cluster = gocql.NewCluster("10.152.146.16")
	cluster.Keyspace = "tick"
	cluster.Consistency = gocql.One
	cluster.Timeout = 1 * time.Second
	cluster.NumConns = 10
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
