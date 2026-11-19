package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Hash 把字节切片映射为 32 位无符号整数，便于替换自定义哈希算法。
type Hash func(data []byte) uint32

// Map 维护由虚拟节点构成的一致性哈希环，把 key 路由到具体节点。
type Map struct {
	hash     Hash              // 哈希函数，默认 crc32.ChecksumIEEE
	replicas int               // 每个真实节点派生的虚拟节点数
	ring     []uint32          // 所有虚拟节点哈希值，升序排列
	hashMap  map[uint32]string // 虚拟节点哈希 -> 真实节点标识
	mu       sync.RWMutex      // 保护环的并发读写
}

// New 创建哈希环；虚拟节点用于缓解物理节点过少导致的数据倾斜，fn 为 nil 时用 crc32。
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[uint32]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty 判断哈希环中是否还有节点。
func (m *Map) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ring) == 0
}

// Add 注册真实节点，每个节点派生 replicas 个虚拟节点。
func (m *Map) Add(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点名 = 序号 + 节点名，使哈希值均匀散列在环上
			vNodeName := strconv.Itoa(i) + key
			hash := m.hash([]byte(vNodeName))

			m.ring = append(m.ring, hash)
			m.hashMap[hash] = key
		}
	}
	// 升序排序后即可二分查找
	sort.Slice(m.ring, func(i, j int) bool {
		return m.ring[i] < m.ring[j]
	})
}

// Remove 注销真实节点及其全部虚拟节点。
func (m *Map) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if key == "" {
		return
	}

	delHashes := make(map[uint32]bool)
	for i := 0; i < m.replicas; i++ {
		vNodeName := strconv.Itoa(i) + key
		hash := m.hash([]byte(vNodeName))
		delete(m.hashMap, hash)
		delHashes[hash] = true
	}

	newRing := make([]uint32, 0, len(m.ring)-len(delHashes))
	for _, h := range m.ring {
		if !delHashes[h] {
			newRing = append(newRing, h)
		}
	}
	m.ring = newRing
}

// Get 顺时针找到第一个哈希值不小于 key 的虚拟节点，再映射回真实节点。
func (m *Map) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.ring) == 0 {
		return ""
	}

	hash := m.hash([]byte(key))

	// 二分查找第一个 ring[i] >= hash 的节点
	idx := sort.Search(len(m.ring), func(i int) bool {
		return m.ring[i] >= hash
	})

	// 超出环上最大值时回绕到起点
	if idx == len(m.ring) {
		idx = 0
	}

	return m.hashMap[m.ring[idx]]
}
