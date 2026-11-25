package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type options struct {
	gatewayURL, messageAPIURL, websocketBaseURL, origin      string
	users, connectionsPerUser, connectConcurrency, groupSize int
	roomID                                                   string
	duration, singleInterval, groupInterval, liveInterval    time.Duration
	verbose                                                  bool
}

type simulator struct {
	options options
	http    *http.Client
	stats   counters
	mu      sync.Mutex
	conns   []*simulatedConn
	users   []string
	readers sync.WaitGroup
}

func main() {
	opts := parseFlags()
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(root, opts.duration)
	defer cancel()
	sim := &simulator{options: opts, http: &http.Client{Timeout: 5 * time.Second}}
	started := time.Now()
	if err := sim.run(ctx); err != nil {
		log.Fatal(err)
	}
	sim.printResult(time.Since(started))
}

func parseFlags() options {
	var o options
	flag.StringVar(&o.gatewayURL, "gateway", "http://127.0.0.1:7000", "Gateway base URL")
	flag.StringVar(&o.messageAPIURL, "message-api", "http://127.0.0.1:7100", "Message API base URL")
	flag.StringVar(&o.websocketBaseURL, "ws-base", "", "optional ws://host:port override")
	flag.StringVar(&o.origin, "origin", "http://localhost", "WebSocket Origin")
	flag.IntVar(&o.users, "users", 20, "simulated users")
	flag.IntVar(&o.connectionsPerUser, "connections-per-user", 1, "connections per user")
	flag.IntVar(&o.connectConcurrency, "connect-concurrency", 20, "connection concurrency")
	flag.IntVar(&o.groupSize, "group-size", 10, "group members")
	flag.StringVar(&o.roomID, "room", "sim-room", "room ID")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "simulation duration")
	flag.DurationVar(&o.singleInterval, "single-interval", 500*time.Millisecond, "0 disables singles")
	flag.DurationVar(&o.groupInterval, "group-interval", 2*time.Second, "0 disables groups")
	flag.DurationVar(&o.liveInterval, "live-interval", time.Second, "0 disables live")
	flag.BoolVar(&o.verbose, "verbose", false, "print errors")
	flag.Parse()
	if o.users < 2 {
		o.users = 2
	}
	if o.connectionsPerUser < 1 {
		o.connectionsPerUser = 1
	}
	if o.connectConcurrency < 1 {
		o.connectConcurrency = 1
	}
	if o.groupSize < 1 || o.groupSize > o.users {
		o.groupSize = o.users
	}
	if o.duration <= 0 {
		o.duration = 30 * time.Second
	}
	return o
}

func (s *simulator) run(ctx context.Context) error {
	s.users = make([]string, s.options.users)
	for i := range s.users {
		s.users[i] = userID(i)
	}
	s.connectAll(ctx)
	if s.stats.connected.Load() == 0 {
		return errNoConnections
	}
	var workers sync.WaitGroup
	s.startPeriodicSender(ctx, &workers, s.options.singleInterval, s.sendSingle)
	s.startPeriodicSender(ctx, &workers, s.options.groupInterval, s.sendGroup)
	s.startPeriodicSender(ctx, &workers, s.options.liveInterval, s.sendLive)
	<-ctx.Done()
	s.closeAll()
	workers.Wait()
	s.readers.Wait()
	return nil
}
