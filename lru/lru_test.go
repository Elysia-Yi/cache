package lru

import (
	"testing"
)

// stringValue 实现 Value 接口，便于单元测试
type stringValue string

func (d stringValue) Len() int {
	return len(d)
}

// TestGet 测试基础的存取功能
func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add("key1", stringValue("1234"))

	if val, ok := lru.Get("key1"); !ok || string(val.(stringValue)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

// TestRemoveOldest 测试内存达到上限时的 LRU 淘汰机制
func TestRemoveOldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	// 每个 entry 占用 len(k)+len(v)：k1=10、k2=10、k3=4，
	// 容量取 k1+k2=20，因此加入 k3 时淘汰 k1
	cap := int64(len(k1) + len(v1) + len(k2) + len(v2))
	lru := New(cap, nil)
	lru.Add(k1, stringValue(v1))
	lru.Add(k2, stringValue(v2))
	lru.Add(k3, stringValue(v3))

	if _, ok := lru.Get(k1); ok || lru.Len() != 2 {
		t.Fatalf("RemoveOldest key1 failed, current len: %d", lru.Len())
	}
	if val, ok := lru.Get(k2); !ok || string(val.(stringValue)) != v2 {
		t.Fatalf("key2 should still exist")
	}
	if val, ok := lru.Get(k3); !ok || string(val.(stringValue)) != v3 {
		t.Fatalf("key3 should exist")
	}
}

// TestOnEvicted 测试淘汰回调函数是否正确触发
func TestOnEvicted(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, value Value) {
		keys = append(keys, key)
	}

	lru := New(int64(10), callback)
	lru.Add("k1", stringValue("12345678")) // 2 + 8 = 10 刚好满
	lru.Add("k2", stringValue("12345678")) // 触发淘汰 k1

	if len(keys) != 1 || keys[0] != "k1" {
		t.Fatalf("Call OnEvicted failed, expected k1 evicted, got %v", keys)
	}
}

// TestUpdateValue 测试更新已存在 Key 时内存增减是否准确
func TestUpdateValue(t *testing.T) {
	lru := New(int64(100), nil)
	lru.Add("key", stringValue("old"))
	expectedBytes := int64(len("key") + len("old"))
	if lru.UsedBytes() != expectedBytes {
		t.Fatalf("Expected used bytes %d, got %d", expectedBytes, lru.UsedBytes())
	}

	lru.Add("key", stringValue("new_longer_value"))
	expectedBytes = int64(len("key") + len("new_longer_value"))
	if lru.UsedBytes() != expectedBytes {
		t.Fatalf("Expected used bytes %d, got %d", expectedBytes, lru.UsedBytes())
	}
}
