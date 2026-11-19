package gocache

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gocache/consistenthash"
	"gocache/lru"
	"gocache/singleflight"
)

// stringVal 实现 lru.Value
type stringVal string

func (s stringVal) Len() int {
	return len(s)
}

// BenchmarkLRUGet 压测单机 LRU 读取吞吐与堆内存分配
func BenchmarkLRUGet(b *testing.B) {
	cache := lru.New(1000000, nil)
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		cache.Add(strconv.Itoa(i), stringVal("test_value_"+strconv.Itoa(i)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i % numKeys)
		_, _ = cache.Get(key)
	}
}

// BenchmarkLRUAdd 压测单机 LRU 写入与淘汰吞吐
func BenchmarkLRUAdd(b *testing.B) {
	// 容量限制为 50000 字节，高频触发 LRU 淘汰
	cache := lru.New(50000, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i % 1000)
		cache.Add(key, stringVal("benchmark_payload_val"))
	}
}

// BenchmarkCacheParallelGet 压测多协程并发读取
func BenchmarkCacheParallelGet(b *testing.B) {
	c := cache{cacheBytes: 1000000}
	const numKeys = 5000
	for i := 0; i < numKeys; i++ {
		c.add(strconv.Itoa(i), ByteView{b: []byte("parallel_cache_value")})
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := strconv.Itoa(r.Intn(numKeys))
			_, _ = c.get(k)
		}
	})
}

// BenchmarkSingleflight 压测热点 key 并发请求的合并效果
func BenchmarkSingleflight(b *testing.B) {
	var g singleflight.Group
	var dbHit int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("hot_burst_key", func() (interface{}, error) {
				// 模拟下游 DB 查询耗时
				atomic.AddInt64(&dbHit, 1)
				time.Sleep(10 * time.Millisecond)
				return "db_value", nil
			})
		}
	})

	b.Logf("Total Singleflight invocations: %d, Actual DB Hits: %d", b.N, dbHit)
}

// BenchmarkConsistentHashGet 压测哈希环的二分路由性能
func BenchmarkConsistentHashGet(b *testing.B) {
	ch := consistenthash.New(50, nil)
	ch.Add("10.0.0.1:8001", "10.0.0.2:8002", "10.0.0.3:8003", "10.0.0.4:8004", "10.0.0.5:8005")

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("bench_key_%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ch.Get(keys[i%1000])
	}
}
