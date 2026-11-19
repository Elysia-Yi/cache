package gocache

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// TestGetter 验证 GetterFunc 接口型函数的适配能力
func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Fatalf("callback failed: expect %s, got %s", expect, v)
	}
}

// TestGroupGet 验证 Group 的本地缓存命中与未命中回源机制
func TestGroupGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("scores", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			t.Logf("[Mock DB] search key: %s", key)
			if v, ok := db[key]; ok {
				loadCounts[key]++
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		// 首次获取必然未命中，触发回源
		view, err := gee.Get(k)
		if err != nil || view.String() != v {
			t.Fatalf("failed to get value of %s: %v", k, err)
		}
		// 再次获取应命中本地缓存，不再增加 loadCounts
		_, err = gee.Get(k)
		if err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss on second request, loadCount=%d", k, loadCounts[k])
		}
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknown should be empty, but got %s", view)
	}
}

// TestGroupSingleflight 验证高并发下通过 Group.Get 访问未缓存 Key 时的防击穿效果
func TestGroupSingleflight(t *testing.T) {
	var dbQueries int32

	group := NewGroup("slow_db", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			atomic.AddInt32(&dbQueries, 1)
			// 模拟慢查询
			time.Sleep(30 * time.Millisecond)
			return []byte("data_for_" + key), nil
		}))

	const concurrentCount = 50
	var wg sync.WaitGroup
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func() {
			defer wg.Done()
			val, err := group.Get("hot_key")
			if err != nil {
				t.Errorf("Get hot_key failed: %v", err)
			}
			if val.String() != "data_for_hot_key" {
				t.Errorf("unexpected value: %s", val.String())
			}
		}()
	}

	wg.Wait()

	// 50 个协程并发，回源只应发生 1 次
	if dbQueries != 1 {
		t.Fatalf("防击穿失效: 期望底层 DB 查询 1 次，实际发生了 %d 次", dbQueries)
	}
}
