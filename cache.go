package gocache

import (
	"sync"

	"gocache/lru"
)

// cache 在 lru.Cache 之上加了读写锁，实现并发安全的缓存层。
type cache struct {
	mu         sync.RWMutex // 保护 lru
	lru        *lru.Cache   // 底层 LRU 引擎
	cacheBytes int64        // 最大内存上限（字节）
}

// add 写入键值，首次写入时才创建底层 LRU 实例。
func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

// get 读取键值。命中时需要把节点移到链表头部，会修改链表指针，因此必须用写锁。
func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		return ByteView{}, false
	}

	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), true
	}
	return ByteView{}, false
}

// len 返回当前缓存的元素个数。
func (c *cache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lru == nil {
		return 0
	}
	return c.lru.Len()
}

// usedBytes 返回当前缓存已使用的内存总字节数。
func (c *cache) usedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lru == nil {
		return 0
	}
	return c.lru.UsedBytes()
}
