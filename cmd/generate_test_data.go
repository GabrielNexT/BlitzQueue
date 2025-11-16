package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type Data struct {
	Data     string `json:"data"`
	Priority *int   `json:"priority,omitempty"`
}

const amountOfMessages = 1e6
const topicName = "test-topic"
const topicPartition = 0

var dataList = make([]Data, amountOfMessages)
var jsonList = make([]string, amountOfMessages)
var rng = rand.New(rand.NewSource(0))

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func main() {

	for i := 0; i < amountOfMessages; i++ {
		str := randSeq(200)
		data := Data{
			Data: str,
		}
		msg, err := json.Marshal(data)
		if err != nil {
			log.Fatal(err)
		}
		// This should be a list of jsons
		jsonList[i] = fmt.Sprintf("[%s]", msg)
		dataList[i] = data
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topicName, topicPartition)

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	//Send messages to Kafka
	now := time.Now()
	for _, msg := range jsonList {
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(msg)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
	elapse := time.Since(now)
	log.Println("wrote", amountOfMessages, "messages in", elapse)

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	// Send messages tp Blitzqueue
	now = time.Now()
	client := &http.Client{}
	for _, msg := range jsonList {
		req, err := http.NewRequest(http.MethodPost, "http://localhost:52525/queue/teste/push", bytes.NewBuffer([]byte(msg)))
		if err != nil {
			log.Fatalf("Error creating request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("Error sending request: %v", err)
		}
		if resp.StatusCode != http.StatusOK { // Expecting 201 Created for successful POST
			log.Fatalf("Unexpected status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			log.Fatalf("Error closing response body: %v", err)
		}
	}
	elapse = time.Since(now)
	log.Println("wrote", amountOfMessages, "messages in", elapse)
}
