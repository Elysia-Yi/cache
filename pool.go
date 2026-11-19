package gocache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gocache/cachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientPool 复用跨节点的 gRPC 长连接，避免每次未命中都重新建连。
type ClientPool struct {
	mu      sync.RWMutex
	clients map[string]*grpcClient
}

// NewClientPool 初始化 gRPC 客户端连接池。
func NewClientPool() *ClientPool {
	return &ClientPool{
		clients: make(map[string]*grpcClient),
	}
}

// GetClient 取目标节点的客户端，连接不存在或状态不健康时重建并缓存。
func (p *ClientPool) GetClient(addr string) (*grpcClient, error) {
	p.mu.RLock()
	client, exists := p.clients[addr]
	p.mu.RUnlock()

	// 连接健康（READY 或 IDLE）则直接复用
	if exists && client != nil && client.conn != nil {
		state := client.conn.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return client, nil
		}
	}

	// 需要重建，改用写锁
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-Check，避免并发重复建连
	if client, exists = p.clients[addr]; exists && client != nil && client.conn != nil {
		state := client.conn.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return client, nil
		}
		_ = client.conn.Close()
	}

	// KeepAlive 心跳参数：定期 ping 以维持长连接
	kacp := keepalive.ClientParameters{
		Time:                10 * time.Second, // 10 秒无活动即发探测 ping
		Timeout:             3 * time.Second,  // ping 响应超时
		PermitWithoutStream: true,             // 无活跃 RPC 时也允许 ping，保持长连接
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithBlock(), // 阻塞直到连接建立或超时
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial grpc peer %s: %w", addr, err)
	}

	client = &grpcClient{
		addr:   addr,
		conn:   conn,
		client: cachepb.NewGroupCacheClient(conn),
	}
	p.clients[addr] = client
	return client, nil
}

// Close 关闭连接池中的所有连接。
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, client := range p.clients {
		if client != nil && client.conn != nil {
			_ = client.conn.Close()
		}
		delete(p.clients, addr)
	}
	return nil
}
