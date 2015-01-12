package main

import (
	"bufio"
	"fmt"
	"net/http"

	"github.com/bitly/go-nsq"
)

func main() {
	conf := nsq.NewConfig()
	w, _ := nsq.NewProducer("127.0.0.1:4150", conf)
	defer w.Stop()

	req, err := http.NewRequest("GET", "https://stream.datasift.com/ae6fadbd4b28204993ecba9b46aee379", nil)
	req.SetBasicAuth("cascadeproject", "fa4b6e7579964163786081436e9bf11c")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// handle error
		fmt.Println("could not continue")
		return
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	for {
		var err error
		var isPrefix bool
		var line []byte
		var buff []byte
		line, isPrefix, err = r.ReadLine()
		buff = append(buff, line...)
		if err != nil {
			fmt.Println(err)
			return
		}
		for isPrefix {
			fmt.Println(string(line[:]))
			fmt.Println("wow, that is one long line")
			line, isPrefix, _ = r.ReadLine()
			buff = append(buff, line...)
		}
		fmt.Println(string(line[:]))
		fmt.Println("\n")
		e := w.Publish("fbmessages", buff)
		if e != nil {
			fmt.Println(e)
		}
	}
}
