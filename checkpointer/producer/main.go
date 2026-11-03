// Command producer publishes sample user events to Kafka for the Flink checkpointer example.
//
// Each message is JSON keyed by user_id:
//
//	{"user_id":"3","name":"Carol","favorite_food":"ramen","ts":1730592000000}
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type UserEvent struct {
	UserID       string `json:"user_id"`
	Name         string `json:"name"`
	FavoriteFood string `json:"favorite_food"`
	Timestamp    int64  `json:"ts"`
}

type user struct {
	id   string
	name string
}

var users = []user{
	{"1", "Alice"}, {"2", "Bob"}, {"3", "Carol"}, {"4", "Dave"}, {"5", "Eve"},
	{"6", "Frank"}, {"7", "Grace"}, {"8", "Heidi"}, {"9", "Ivan"}, {"10", "Judy"},
}

var foods = []string{"sushi", "ramen", "pizza", "curry", "tacos", "udon", "burger", "pho"}

func main() {
	brokers := flag.String("brokers", "localhost:29092", "comma-separated Kafka bootstrap servers")
	topic := flag.String("topic", "user-events", "Kafka topic")
	count := flag.Int("count", 0, "number of messages to send (0 = until interrupted)")
	interval := flag.Duration("interval", 500*time.Millisecond, "delay between messages")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:        *topic,
		Balancer:     &kafka.Hash{}, // same user_id -> same partition
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for sent := 0; *count == 0 || sent < *count; {
		select {
		case <-ctx.Done():
			log.Printf("stopping after %d messages", sent)
			return
		case <-ticker.C:
		}

		u := users[rand.IntN(len(users))]
		event := UserEvent{
			UserID:       u.id,
			Name:         u.name,
			FavoriteFood: foods[rand.IntN(len(foods))],
			Timestamp:    time.Now().UnixMilli(),
		}
		value, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("marshal event: %v", err)
		}

		if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(u.id), Value: value}); err != nil {
			if ctx.Err() != nil {
				continue
			}
			log.Printf("write message: %v", err)
			continue
		}
		sent++
		log.Printf("sent #%d %s", sent, value)
	}
	log.Printf("done: sent %d messages", *count)
}
