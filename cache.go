package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// CacheConfig holds cache configuration
type CacheConfig struct {
	RedisURL    string
	EnableRedis bool
	DefaultTTL  time.Duration
}

// Cache provides caching functionality with Redis or in-memory fallback
type Cache struct {
	redis      *redis.Client
	useRedis   bool
	defaultTTL time.Duration

	// In-memory fallback
	mu      sync.RWMutex
	memData map[string]cacheEntry
}

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
}

// NewCache creates a new cache instance
func NewCache(cfg CacheConfig) *Cache {
	c := &Cache{
		defaultTTL: cfg.DefaultTTL,
		memData:    make(map[string]cacheEntry),
	}

	if cfg.EnableRedis && cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Printf("WARN: invalid redis URL, using in-memory cache: %v", err)
			return c
		}

		client := redis.NewClient(opts)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := client.Ping(ctx).Err(); err != nil {
			log.Printf("WARN: redis connection failed, using in-memory cache: %v", err)
			return c
		}

		c.redis = client
		c.useRedis = true
		log.Printf("INFO: connected to Redis for caching")
	}

	// Start cleanup goroutine for in-memory cache
	if !c.useRedis {
		go c.cleanupLoop()
	}

	return c
}

func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.memData {
			if now.After(v.expiresAt) {
				delete(c.memData, k)
			}
		}
		c.mu.Unlock()
	}
}

// Get retrieves a value from cache
func (c *Cache) Get(ctx context.Context, key string, dest interface{}) bool {
	if c.useRedis {
		data, err := c.redis.Get(ctx, key).Bytes()
		if err != nil {
			return false
		}
		return json.Unmarshal(data, dest) == nil
	}

	// In-memory fallback
	c.mu.RLock()
	entry, ok := c.memData[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(entry.expiresAt) {
		return false
	}

	return json.Unmarshal(entry.data, dest) == nil
}

// Set stores a value in cache
func (c *Cache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if ttl == 0 {
		ttl = c.defaultTTL
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if c.useRedis {
		return c.redis.Set(ctx, key, data, ttl).Err()
	}

	// In-memory fallback
	c.mu.Lock()
	c.memData[key] = cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(ttl),
	}
	c.mu.Unlock()

	return nil
}

// Delete removes a key from cache
func (c *Cache) Delete(ctx context.Context, key string) error {
	if c.useRedis {
		return c.redis.Del(ctx, key).Err()
	}

	c.mu.Lock()
	delete(c.memData, key)
	c.mu.Unlock()
	return nil
}

// InvalidateDashboard clears dashboard cache
func (c *Cache) InvalidateDashboard(ctx context.Context) {
	// Delete all dashboard cache keys
	for days := 1; days <= 365; days++ {
		_ = c.Delete(ctx, dashboardCacheKey(days))
	}
}

func dashboardCacheKey(days int) string {
	return "dashboard:" + string(rune(days))
}