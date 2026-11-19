package gocache

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"gocache/cachepb"
	"gocache/consistenthash"

	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath = "/_gocache/"
	defaultReplicas = 50
)

// HTTPPool 既是本节点的 HTTP 服务端，也维护其他节点的哈希路由与请求客户端。
type HTTPPool struct {
	self        string                 // 本节点地址，如 "http://127.0.0.1:8001"
	basePath    string                 // HTTP 路径前缀，默认 "/_gocache/"
	mu          sync.Mutex             // 保护 peers 与 httpGetters
	peers       *consistenthash.Map    // 一致性哈希环
	httpGetters map[string]*httpGetter // 节点地址 -> 客户端
	groups      map[string]*Group      // 节点私有缓存组（单进程多节点仿真用）
}

// NewHTTPPool 初始化当前节点的 HTTP 通信池。
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
		groups:   make(map[string]*Group),
	}
}

// RegisterGroup 绑定缓存组到当前实例。
func (p *HTTPPool) RegisterGroup(g *Group) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groups[g.Name()] = g
}

// GetGroup 优先查节点私有表，其次回退全局注册表。
func (p *HTTPPool) GetGroup(name string) *Group {
	p.mu.Lock()
	defer p.mu.Unlock()
	if g, ok := p.groups[name]; ok {
		return g
	}
	return GetGroup(name)
}

// Log 打印带当前节点标识的日志。
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServeHTTP 处理对等节点的查询请求，路径格式为 /_gocache/<group>/<key>，响应是 Protobuf 二进制。
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		http.Error(w, "HTTPPool serving unexpected path: "+r.URL.Path, http.StatusBadRequest)
		return
	}

	p.Log("%s %s", r.Method, r.URL.Path)

	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request path: expected /<group>/<key>", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := p.GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Protobuf 二进制比 JSON 更紧凑，省带宽与编解码开销
	body, err := proto.Marshal(&cachepb.GetResponse{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

// Set 构建哈希环并为每个对等节点创建客户端。
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// PickPeer 在哈希环上选出负责该 key 的远程节点。
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers == nil || p.peers.IsEmpty() {
		return nil, false
	}

	peer := p.peers.Get(key)
	if peer != "" && peer != p.self {
		p.Log("Pick remote peer: %s for key: %s", peer, key)
		return p.httpGetters[peer], true
	}
	return nil, false
}

// httpGetter 是访问远程 HTTP 节点的客户端，实现 PeerGetter 接口。
type httpGetter struct {
	baseURL string // 目标节点的请求基地址，如 "http://127.0.0.1:8002/_gocache/"
}

// Get 向远程节点发起 HTTP GET，并把响应反序列化到 out。
func (h *httpGetter) Get(in *cachepb.GetRequest, out *cachepb.GetResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)

	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("remote server returned status: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body error: %v", err)
	}

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body error: %v", err)
	}

	return nil
}
