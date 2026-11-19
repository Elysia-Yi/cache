package gocache

import (
	"errors"
	"log"
	"sync"

	"gocache/cachepb"
	"gocache/singleflight"
)

// Getter 定义缓存未命中时的回源逻辑（查 DB、读磁盘或调用外部服务）。
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc 让普通函数或闭包直接满足 Getter 接口。
type GetterFunc func(key string) ([]byte, error)

// Get 回调实现
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// Group 是一个独立的缓存命名空间，拥有自己的内存配额、回源 Getter、节点路由与防击穿组件。
type Group struct {
	name      string              // 命名空间标识
	getter    Getter              // 未命中时的回源回调
	mainCache cache               // 本地 LRU 缓存
	peers     PeerPicker          // 节点选择器（基于一致性哈希）
	loader    *singleflight.Group // 合并同一 key 的并发请求
}

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// NewGroup 创建并注册全局 Group。
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// NewIsolatedGroup 创建不注册到全局表的 Group，便于单进程内模拟多节点。
func NewIsolatedGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	return &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
}

// GetGroup 按名称获取已注册的 Group。
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// RegisterPeers 注入节点选择器（如 GRPCPool 或 HTTPPool）。
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// Name 返回 Group 的名称。
func (g *Group) Name() string {
	return g.name
}

// Get 先查本地缓存，未命中再交给 load 走分布式加载流程。
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, errors.New("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Printf("[GoCache] [Local Hit] Group: %s, Key: %s", g.name, key)
		return v, nil
	}

	return g.load(key)
}

// load 用 singleflight 合并同一 key 的并发请求，优先取远程节点，失败则本地回源。
func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err := g.getFromPeer(peer, key); err == nil {
					log.Printf("[GoCache] [Remote Peer Hit] Group: %s, Key: %s", g.name, key)
					return value, nil
				}
				log.Printf("[GoCache] [Remote Peer Miss/Err] Failed to get from peer, falling back locally: %v", err)
			}
		}

		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return ByteView{}, err
}

// getFromPeer 通过 Protobuf 序列化从远程节点拉取数据。
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &cachepb.GetRequest{
		Group: g.name,
		Key:   key,
	}
	res := &cachepb.GetResponse{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

// getLocally 本地回源加载数据并回填缓存。
func (g *Group) getLocally(key string) (ByteView, error) {
	log.Printf("[GoCache] [DB Fallback Loading] Group: %s, Key: %s", g.name, key)
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// populateCache 把数据写回本地 LRU 缓存。
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}
