package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var errNoConnections = errors.New("no WebSocket connection was established")

type counters struct {
	connected, received, acked      atomic.Int64
	singleSent, groupSent, liveSent atomic.Int64
	requestFails, connFails         atomic.Int64
}

func userID(index int) string {
	return fmt.Sprintf("sim-user-%04d", index+1)
}

func randomSuffix() string {
	var value [4]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func (s *simulator) logError(format string, args ...any) {
	if s.options.verbose {
		log.Printf(format, args...)
	}
}

func (s *simulator) printResult(elapsed time.Duration) {
	fmt.Println("Socket Server Simulation")
	fmt.Printf("elapsed:             %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("target_connections:  %d\n", s.options.users*s.options.connectionsPerUser)
	fmt.Printf("connected:           %d\n", s.stats.connected.Load())
	fmt.Printf("connection_failures: %d\n", s.stats.connFails.Load())
	fmt.Printf("single_sent:         %d\n", s.stats.singleSent.Load())
	fmt.Printf("group_sent:          %d\n", s.stats.groupSent.Load())
	fmt.Printf("live_sent:           %d\n", s.stats.liveSent.Load())
	fmt.Printf("messages_received:   %d\n", s.stats.received.Load())
	fmt.Printf("acks_sent:           %d\n", s.stats.acked.Load())
	fmt.Printf("request_failures:    %d\n", s.stats.requestFails.Load())
}
