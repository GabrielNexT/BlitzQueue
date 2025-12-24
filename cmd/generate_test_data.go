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

	amqp "github.com/rabbitmq/amqp091-go"
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
const rabbitQueueName = "test-queue"

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

	sendToKafka()
	sendToRabbitMQ()
	sendToBlitzQueue()
}

func sendToKafka() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topicName, topicPartition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	now := time.Now()
	for _, msg := range jsonList {
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(msg)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
	elapsed := time.Since(now)
	log.Println("[Kafka] wrote", amountOfMessages, "messages in", elapsed)
}

func sendToRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("failed to open a channel:", err)
	}
	defer ch.Close()

	// Enable publisher confirms
	err = ch.Confirm(false)
	if err != nil {
		log.Fatal("failed to enable confirms:", err)
	}

	q, err := ch.QueueDeclare(
		rabbitQueueName, // name
		false,           // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.Fatal("failed to declare a queue:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	now := time.Now()
	for _, msg := range jsonList {
		confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(msg),
			})
		if err != nil {
			log.Fatal("failed to publish a message:", err)
		}
		// Wait for RabbitMQ to confirm the message was received
		confirmed, err := confirmation.WaitContext(ctx)
		if err != nil {
			log.Fatal("failed to wait for confirmation:", err)
		}
		if !confirmed {
			log.Fatal("message was not confirmed")
		}
	}
	elapsed := time.Since(now)
	log.Println("[RabbitMQ] wrote", amountOfMessages, "messages in", elapsed)
}

func sendToBlitzQueue() {
	client := &http.Client{}

	now := time.Now()
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
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("Unexpected status code: %d", resp.StatusCode)
		}
		err = resp.Body.Close()
		if err != nil {
			log.Fatalf("Error closing response body: %v", err)
		}
	}
	elapsed := time.Since(now)
	log.Println("[BlitzQueue] wrote", amountOfMessages, "messages in", elapsed)
}
