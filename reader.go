package main

import (
	"fmt"

	"github.com/bitly/go-nsq"
	"github.com/nytlabs/gojsonexplode"
)

func loop(inChan chan *nsq.Message) {
	i := 0
	for msg := range inChan {
		exploded, err := gojsonexplode.Explodejson(msg.Body, "||")
		if err != nil {
			fmt.Println(err)
			msg.Finish()
		}
		i = i + 1
		fmt.Println(i)
		msg.Finish()
	}
}

func main() {
	var reader *nsq.Consumer
	var err error
	inChan := make(chan *nsq.Message, 1)
	lookup := "localhost:4161"
	conf := nsq.NewConfig()
	conf.Set("maxInFlight", 1000)
	reader, err = nsq.NewConsumer("page", "tick", conf)
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
