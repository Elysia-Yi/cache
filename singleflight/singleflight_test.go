package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSingleflightDo 验证同一 key 的并发请求只触发一次底层调用
func TestSingleflightDo(t *testing.T) {
	var g Group
	var calls int32 // 记录底层实际函数被调用的次数

	const concurrentCount = 100
	var wg sync.WaitGroup
	wg.Add(concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		go func() {
			defer wg.Done()
			v, err := g.Do("hot_key", func() (interface{}, error) {
				// 模拟慢查询
				atomic.AddInt32(&calls, 1)
				time.Sleep(50 * time.Millisecond)
				return "hot_value", nil
			})

			if err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
			if v.(string) != "hot_value" {
				t.Errorf("expected hot_value, got: %v", v)
			}
		}()
	}

	wg.Wait()

	// 无论并发多少，底层加载只应执行 1 次
	if calls != 1 {
		t.Fatalf("Singleflight 防击穿失效: 期望执行 1 次底层加载，实际执行了 %d 次", calls)
	}
}

// TestSingleflightDiffKeys 验证不同 key 之间互不干扰，可并发执行
func TestSingleflightDiffKeys(t *testing.T) {
	var g Group
	var calls int32

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = g.Do("key1", func() (interface{}, error) {
			atomic.AddInt32(&calls, 1)
			time.Sleep(20 * time.Millisecond)
			return "val1", nil
		})
	}()

	go func() {
		defer wg.Done()
		_, _ = g.Do("key2", func() (interface{}, error) {
			atomic.AddInt32(&calls, 1)
			time.Sleep(20 * time.Millisecond)
			return "val2", nil
		})
	}()

	wg.Wait()

	if calls != 2 {
		t.Fatalf("不同 key 期望执行 2 次，实际执行了 %d 次", calls)
	}
}

// TestSingleflightError 验证错误信息能够正确广播给所有并发等待的协程
func TestSingleflightError(t *testing.T) {
	var g Group
	expectedErr := errors.New("db connection timeout")

	var wg sync.WaitGroup
	const count = 10
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			_, err := g.Do("err_key", func() (interface{}, error) {
				time.Sleep(10 * time.Millisecond)
				return nil, expectedErr
			})
			if !errors.Is(err, expectedErr) {
				t.Errorf("expected error %v, got %v", expectedErr, err)
			}
		}()
	}

	wg.Wait()
}
