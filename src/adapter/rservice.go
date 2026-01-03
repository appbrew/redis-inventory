package adapter

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v4"
)

// ScanOptions options for scanning keyspace
type ScanOptions struct {
	Pattern   string
	ScanCount int
	Throttle  int
}

// NewRedisService creates RedisService
func NewRedisService(client radix.Client, redisURL string) *RedisService {
	return &RedisService{
		client:   client,
		redisURL: redisURL,
	}
}

// RedisService implementation for iteration over redis
type RedisService struct {
	client   radix.Client
	redisURL string
	mu       sync.RWMutex
}

// IsConnectionError checks if the error is a connection-related error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "closed") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "timeout")
}

// Reconnect attempts to create a new Redis connection
func (s *RedisService) Reconnect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close existing client if possible
	if s.client != nil {
		s.client.Close()
	}

	// Create new client
	newClient, err := (radix.PoolConfig{}).New(context.Background(), "tcp", s.redisURL)
	if err != nil {
		return err
	}

	s.client = newClient
	return nil
}

// ScanKeys scans keys asynchroniously and sends them to the returned channel
func (s *RedisService) ScanKeys(ctx context.Context, options ScanOptions) <-chan string {
	resultChan := make(chan string)

	scanOpts := radix.ScannerConfig{
		Command: "SCAN",
		Count:   options.ScanCount,
	}

	if options.Pattern != "*" && options.Pattern != "" {
		scanOpts.Pattern = options.Pattern
	}

	go func() {
		defer close(resultChan)
		var key string
		s.mu.RLock()
		radixScanner := scanOpts.New(s.client)
		s.mu.RUnlock()
		for radixScanner.Next(ctx, &key) {
			resultChan <- key
			if options.Throttle > 0 {
				time.Sleep(time.Nanosecond * time.Duration(options.Throttle))
			}
		}
	}()

	return resultChan
}

// GetKeysCount returns number of keys in the current database
func (s *RedisService) GetKeysCount(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keysCount int64
	err := s.client.Do(context.Background(), radix.Cmd(&keysCount, "DBSIZE"))
	if err != nil {
		return 0, err
	}

	return keysCount, nil
}

// GetMemoryUsage returns memory usage of given key
func (s *RedisService) GetMemoryUsage(ctx context.Context, key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res int64
	err := s.client.Do(context.Background(), radix.Cmd(&res, "MEMORY", "USAGE", key))
	if err != nil {
		return 0, err
	}

	return res, nil
}
