package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	var broker, listenAddr, group string
	flag.StringVar(&broker, "b", "localhost:65363", "Kafka broker")
	flag.StringVar(&listenAddr, "l", ":8080", "HTTP listen address")
	flag.StringVar(&group, "g", "go-kafka-state", "The Kafka ConsumerGroup")
	flag.Parse()

	// Connect to the Redpanda broker and consume the user topic
	seeds := []string{broker}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics("user"),
		kgo.ConsumerGroup(group),
		kgo.Balancers(NewRangeBalancer(listenAddr)),
		kgo.AdjustFetchOffsetsFn(func(ctx context.Context, m map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			for k, v := range m {
				for i := range v {
					m[k][i] = kgo.NewOffset().At(-2).WithEpoch(-1)
				}
			}
			return m, nil
		}),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	users := NewUserStore()
	// Start serving HTTP requests
	httpShutdown := serveHttp(listenAddr, users, cl, group)

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

				// Handle tombstones and continue with next record
				if len(record.Value) == 0 {
					users.Delete(string(record.Key))
					continue
				}

				// Update state
				u := User{}
				err := json.Unmarshal(record.Value, &u)
				if err != nil {
					panic(err)
				}
				users.Set(string(record.Key), u)
			}
		})
	}
}

func fetchGroup(ctx context.Context, group string, kClient *kgo.Client) map[string]string {
	members := map[string]string{}
	resp := kClient.RequestSharded(ctx, &kmsg.DescribeGroupsRequest{Groups: []string{group}})
	for _, m := range resp[0].Resp.(*kmsg.DescribeGroupsResponse).Groups[0].Members {
		metadata := kmsg.ConsumerMemberMetadata{}
		metadata.ReadFrom(m.ProtocolMetadata)
		assign := kmsg.ConsumerMemberAssignment{}
		assign.ReadFrom(m.MemberAssignment)
		for _, owned := range assign.Topics {
			for _, p := range owned.Partitions {
				members[owned.Topic+"-"+strconv.Itoa(int(p))] = string(metadata.UserData)
			}
		}
	}
	return members
}

func serveHttp(addr string, users *UserStore, kClient *kgo.Client, group string) func(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/groupinfo", func(w http.ResponseWriter, r *http.Request) {
		m := fetchGroup(r.Context(), group, kClient)
		b, _ := json.Marshal(m)
		w.Write(b)
		return
	})
	mux.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		email := r.URL.Query().Get("email")
		fmt.Printf("http: %s /user?email=%s\n", r.Method, email)
		switch r.Method {
		case http.MethodGet:
			u, ok := users.Get(email)
			if !ok {
				http.NotFound(w, r)
				return
			}
			b, err := json.Marshal(u)
			if err != nil {
				panic(err)
			}
			_, err = w.Write(b)
			if err != nil {
				panic(err)
			}
			return
		case http.MethodPut:
			u := User{}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			if json.Unmarshal(b, &u) != nil {
				panic(err)
			}
			if email == "" {
				email = u.Email
			}
			v, err := json.Marshal(u)
			if err != nil {
				panic(err)
			}
			res := kClient.ProduceSync(r.Context(), &kgo.Record{Key: []byte(email), Value: v, Topic: "user"})
			if err := res.FirstErr(); err != nil {
				http.Error(w, "failed to update user", http.StatusInternalServerError)
			}
			w.WriteHeader(http.StatusOK)
			return
		case http.MethodDelete:
			res := kClient.ProduceSync(r.Context(), &kgo.Record{Key: []byte(email), Value: []byte{}, Topic: "user"})
			if err := res.FirstErr(); err != nil {
				http.Error(w, "failed to update user", http.StatusInternalServerError)
			}
			w.WriteHeader(http.StatusOK)
			return
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

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type UserStore struct {
	l sync.RWMutex
	u map[string]User
}

func NewUserStore() *UserStore {
	return &UserStore{u: map[string]User{}}
}

func (u *UserStore) Get(email string) (User, bool) {
	u.l.RLock()
	defer u.l.RUnlock()
	user, ok := u.u[email]
	return user, ok
}

func (u *UserStore) Set(email string, user User) {
	u.l.Lock()
	defer u.l.Unlock()
	u.u[email] = user
}

func (u *UserStore) Delete(email string) {
	u.l.Lock()
	defer u.l.Unlock()
	delete(u.u, email)
}
