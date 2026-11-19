# GoCache

基于 groupcache 架构的 Go 分布式缓存框架，支持 HTTP 和 gRPC 两种集群通信协议。

## 特性

- **LRU 内存淘汰** — 带并发读写锁的 LRU 缓存
- **一致性哈希** — 基于虚拟节点的分布式路由
- **Singleflight** — 合并同 key 并发请求，防止缓存击穿
- **双协议支持** — HTTPPool / GRPCPool 两种集群通信
- **Protobuf 序列化** — 跨节点数据传输紧凑高效

## 目录结构

```
├── cache.go            # 并发安全的缓存层（LRU + RWMutex）
├── group.go            # Group 缓存命名空间与分布式加载逻辑
├── http.go             # HTTPPool：HTTP 集群通信
├── grpc.go             # GRPCPool：gRPC 集群通信
├── peers.go            # PeerPicker / PeerGetter 接口
├── byteview.go         # 只读字节视图，减少拷贝
├── lru/                # LRU 淘汰算法
├── singleflight/       # 防击穿合并器
├── consistenthash/     # 一致性哈希环
├── cachepb/            # Protobuf 定义与生成代码
├── cmd/demo/           # HTTP 集群演示
└── cmd/grpc_demo/      # gRPC 集群演示
```

## 快速开始

### HTTP 集群演示

```bash
go run cmd/demo/main.go -demo=true
```

启动后自动拉起 3 个节点 (8001/8002/8003)，演示跨节点路由 + Singleflight 防击穿。

### 作为库使用

```go
// 1. 创建缓存组（带回源回调）
g := gocache.NewGroup("scores", 2<<20, gocache.GetterFunc(
    func(key string) ([]byte, error) {
        // 查 DB / 调外部服务
        return []byte("value"), nil
    }))

// 2. 配置集群节点
peers := gocache.NewHTTPPool("http://127.0.0.1:8001")
peers.Set("http://127.0.0.1:8001", "http://127.0.0.1:8002")
g.RegisterPeers(peers)

// 3. 启动 HTTP 服务
http.ListenAndServe(":8001", peers)

// 4. 查询缓存，自动处理远程拉取与本地回源
v, err := g.Get("Tom")
```

## 架构流程

```
Get(key) → 本地命中? → 直接返回
              ↓ 未命中
         PickPeer (一致性哈希)
              ↓
         远程节点有? → Protobuf 拉取 → 返回并回填本地
              ↓ 没有
         Singleflight 合并并发
              ↓
         Getter 回源 → 写入缓存 → 返回
```
