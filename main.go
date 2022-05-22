package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	var broker, listenAddr string
	flag.StringVar(&broker, "b", "localhost:50278", "Kafka broker")
	flag.StringVar(&listenAddr, "l", ":8080", "HTTP listen address")
	flag.Parse()

	// Connect to the Redpanda broker and consume the user topic
	seeds := []string{broker}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics("user"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	users := NewUserStore()
	// Start serving HTTP requests
	httpShutdown := serveHttp(listenAddr, users)

	// Run our consume loop in a separate Go routine
	ctx := context.Background()
	go consume(ctx, cl, users)

	// Shutdown gracefully
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)

	<-sigs
	fmt.Println("received interrupt signal; closing client")
	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Close()
		ctx, cancel := context.WithTimeout(ctx, time.Second*2)
		defer cancel()
		httpShutdown(ctx)
	}()

	select {
	case <-sigs:
		fmt.Println("received second interrupt signal; quitting without waiting for graceful close")
	case <-done:
	}
}

func consume(ctx context.Context, cl *kgo.Client, users *UserStore) {
	for {
		fetches := cl.PollFetches(ctx)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				fmt.Printf("%s (p=%d): %s\n", string(record.Key), record.Partition, string(record.Value))
				users.Set(string(record.Key), string(record.Value))
			}
		})
	}
}

func serveHttp(addr string, users *UserStore) func(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		email := r.URL.Query().Get("email")
		fmt.Printf("http: %s: /user?email=%s\n", r.Method, email)
		u, ok := users.Get(email)
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, err := w.Write([]byte(u))
		if err != nil {
			panic(err)
		}
	})

	s := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	return s.Shutdown
}

type UserStore struct {
	l sync.RWMutex
	u map[string]string
}

func NewUserStore() *UserStore {
	return &UserStore{u: map[string]string{}}
}

func (u *UserStore) Get(email string) (string, bool) {
	u.l.RLock()
	defer u.l.RUnlock()
	user, ok := u.u[email]
	return user, ok
}

func (u *UserStore) Set(email string, user string) {
	u.l.Lock()
	defer u.l.Unlock()
	u.u[email] = user
}
