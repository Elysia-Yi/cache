package gocache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"gocache/cachepb"
	"gocache/consistenthash"

	"google.golang.org/grpc"
)

// GRPCPool 实现 PeerPicker 与 GroupCacheServer：哈希选节点，并作为 gRPC 服务端响应远程拉取。
type GRPCPool struct {
	cachepb.UnimplementedGroupCacheServer

	self       string              // 本节点 gRPC 监听地址，如 "127.0.0.1:9001"
	mu         sync.RWMutex        // 保护 peers、server 与 groups
	peers      *consistenthash.Map // 一致性哈希环
	clientPool *ClientPool         // 复用的 gRPC 长连接池
	server     *grpc.Server        // gRPC 服务端
	lis        net.Listener        // 网络监听器
	groups     map[string]*Group   // 节点私有缓存组
}

// NewGRPCPool 创建 gRPC 节点通信池。
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{
		self:       self,
		clientPool: NewClientPool(),
		groups:     make(map[string]*Group),
	}
}

// RegisterGroup 绑定缓存组到当前实例。
func (p *GRPCPool) RegisterGroup(g *Group) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groups[g.Name()] = g
}

// GetGroup 优先查节点私有表，其次回退全局注册表。
func (p *GRPCPool) GetGroup(name string) *Group {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if g, ok := p.groups[name]; ok {
		return g
	}
	return GetGroup(name)
}

// Set 注册集群节点地址并按虚拟节点构建哈希环。
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
}

// PickPeer 在哈希环上选出负责该 key 的远程节点。
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.peers == nil || p.peers.IsEmpty() {
		return nil, false
	}

	peer := p.peers.Get(key)
	if peer != "" && peer != p.self {
		log.Printf("[GRPCPool %s] Pick remote peer: %s for key: %s", p.self, peer, key)
		client, err := p.clientPool.GetClient(peer)
		if err != nil {
			log.Printf("[GRPCPool %s] Failed to get client for %s: %v", p.self, peer, err)
			return nil, false
		}
		return client, true
	}
	return nil, false
}

// Get 处理其他节点发来的 gRPC 拉取请求。
func (p *GRPCPool) Get(ctx context.Context, in *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	groupName := in.GetGroup()
	key := in.GetKey()

	log.Printf("[GRPCPool %s] Receive RPC Get: Group=%s, Key=%s", p.self, groupName, key)

	if groupName == "" || key == "" {
		return nil, errors.New("group and key are required")
	}

	group := p.GetGroup(groupName)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", groupName)
	}

	view, err := group.Get(key)
	if err != nil {
		return nil, err
	}

	return &cachepb.GetResponse{
		Value: view.ByteSlice(),
	}, nil
}

// Start 启动 gRPC 服务并阻塞运行（通常配合 go pool.Start() 使用）。
func (p *GRPCPool) Start() error {
	lis, err := net.Listen("tcp", p.self)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.self, err)
	}

	p.mu.Lock()
	p.lis = lis
	p.server = grpc.NewServer()
	cachepb.RegisterGroupCacheServer(p.server, p)
	p.mu.Unlock()

	log.Printf("[GRPCPool %s] gRPC Server listening at %s", p.self, p.self)
	return p.server.Serve(lis)
}

// Stop 优雅关闭服务并释放连接池。
func (p *GRPCPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.server != nil {
		p.server.GracefulStop()
	}
	if p.clientPool != nil {
		_ = p.clientPool.Close()
	}
	log.Printf("[GRPCPool %s] Stopped successfully", p.self)
}

// grpcClient 连接单个远程节点，实现 PeerGetter 接口。
type grpcClient struct {
	addr   string
	conn   *grpc.ClientConn
	client cachepb.GroupCacheClient
}

// Get 发起带 5 秒超时的远程调用。
func (c *grpcClient) Get(in *cachepb.GetRequest, out *cachepb.GetResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, in)
	if err != nil {
		return fmt.Errorf("gRPC call to %s failed: %w", c.addr, err)
	}

	out.Value = resp.GetValue()
	return nil
}
