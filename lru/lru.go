package lru

// Value 要求缓存对象报告自身占用的字节数。
type Value interface {
	Len() int
}

// entry 是双向链表节点；同时保存 key，便于淘汰时以 O(1) 从 map 中删除。
type entry struct {
	key   string
	value Value
	prev  *entry
	next  *entry
}

// Cache 是非并发安全的 LRU 淘汰引擎，并发安全由外层封装的锁负责。
type Cache struct {
	maxBytes int64 // 最大内存字节数，0 表示不限制
	nbytes   int64 // 当前已用字节数（含 key 与 value）

	items map[string]*entry // key -> 节点，O(1) 定位

	// head/tail 为哨兵节点，省去插入删除时的空指针与边界判断。
	head *entry // 最近访问的节点在 head.next
	tail *entry // 最久未访问的节点在 tail.prev

	// OnEvicted 是淘汰或删除时的回调，可为 nil
	OnEvicted func(key string, value Value)
}

// New 创建 LRU 实例；maxBytes 为 0 表示不限制，onEvicted 可为 nil。
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	c := &Cache{
		maxBytes:  maxBytes,
		items:     make(map[string]*entry),
		head:      &entry{},
		tail:      &entry{},
		OnEvicted: onEvicted,
	}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

// Get 返回 key 对应的值；命中时会把节点移到链表头部，因此并非只读操作。
func (c *Cache) Get(key string) (value Value, ok bool) {
	if c.items == nil {
		return nil, false
	}
	if node, exists := c.items[key]; exists {
		c.moveToHead(node)
		return node.value, true
	}
	return nil, false
}

// Add 写入或更新键值，随后按 maxBytes 淘汰最久未访问的节点。
func (c *Cache) Add(key string, value Value) {
	if c.items == nil {
		c.items = make(map[string]*entry)
	}

	if node, exists := c.items[key]; exists {
		c.nbytes += int64(value.Len()) - int64(node.value.Len())
		node.value = value
		c.moveToHead(node)
	} else {
		node := &entry{
			key:   key,
			value: value,
		}
		c.addToHead(node)
		c.items[key] = node
		c.nbytes += int64(len(key)) + int64(value.Len())
	}

	for c.maxBytes > 0 && c.nbytes > c.maxBytes {
		c.RemoveOldest()
	}
}

// RemoveOldest 淘汰链表尾部（最久未访问）的节点。
func (c *Cache) RemoveOldest() {
	if c.Len() == 0 {
		return
	}
	oldest := c.tail.prev
	c.removeNode(oldest)
	delete(c.items, oldest.key)

	c.nbytes -= int64(len(oldest.key)) + int64(oldest.value.Len())

	if c.OnEvicted != nil {
		c.OnEvicted(oldest.key, oldest.value)
	}
}

// Remove 主动删除指定的 key。
func (c *Cache) Remove(key string) {
	if c.items == nil {
		return
	}
	if node, exists := c.items[key]; exists {
		c.removeNode(node)
		delete(c.items, key)
		c.nbytes -= int64(len(node.key)) + int64(node.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(node.key, node.value)
		}
	}
}

// Len 返回当前缓存的键值对数量。
func (c *Cache) Len() int {
	return len(c.items)
}

// UsedBytes 返回当前实际占用的字节数。
func (c *Cache) UsedBytes() int64 {
	return c.nbytes
}

// Clear 清空整个缓存。
func (c *Cache) Clear() {
	if c.OnEvicted != nil {
		for _, node := range c.items {
			c.OnEvicted(node.key, node.value)
		}
	}
	c.items = make(map[string]*entry)
	c.head.next = c.tail
	c.tail.prev = c.head
	c.nbytes = 0
}

// addToHead 把节点插入哨兵 head 之后。
func (c *Cache) addToHead(node *entry) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

// removeNode 从链表中摘除节点。
func (c *Cache) removeNode(node *entry) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

// moveToHead 把已有节点移到链表头部。
func (c *Cache) moveToHead(node *entry) {
	c.removeNode(node)
	c.addToHead(node)
}
