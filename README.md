# GoCache：Go 分布式缓存

一个基于 groupcache 架构的对等（P2P）分布式缓存：集群里每个节点地位相同，客户端连任意节点都能取到数据。
单机层手写 LRU 淘汰引擎，集群层用一致性哈希分片、用 singleflight 合并并发回源，
节点间通信支持 HTTP 与 gRPC 两种协议，传输统一走 Protobuf。

## 特性

- **手写并发安全 LRU**：哈希表 + 自定义双向链表（不用 `container/list`，避免 `interface{}` 装箱与类型断言），
  哨兵头尾节点消除边界判断，取值与淘汰均为 O(1)。
- **一致性哈希分片**：`crc32` + 虚拟节点（默认每节点 50 个），节点增减时只影响相邻一段数据。
- **Singleflight 防击穿**：同一个 key 的并发回源只放行一个请求，其余等待并共享结果。
- **双协议节点通信**：`HTTPPool`（HTTP/1.1 + Protobuf）与 `GRPCPool`（gRPC + 长连接池）可互换。
- **连接池复用**：gRPC 节点间维持长连接，配合 Keepalive 心跳，避免每次拉取都重新握手。
- **零业务侵入**：注册一个 Getter 回调（查 DB / 调下游服务），其余缓存与路由逻辑由框架接管。

## 核心组件

| 文件                                 | 职责                                                        |
|:---------------------------------- |:--------------------------------------------------------- |
| `group.go`                         | 对外门面：`NewGroup` / `Get` / `RegisterPeers`，串起缓存、路由与回源三段    |
| `cache.go`                         | 在 LRU 之上加 `sync.RWMutex` 的并发缓存层，并统计已用内存                   |
| `lru/lru.go`                       | LRU 淘汰引擎：双向链表 + 哈希表，支持 `onEvicted` 回调                     |
| `singleflight/singleflight.go`     | 并发请求合并：`Do(key, fn)` 让同 key 的调用共享一次执行结果                   |
| `consistenthash/consistenthash.go` | 哈希环：`Add` 建虚拟节点，`Get` 二分查找找到负责节点                          |
| `byteview.go`                      | 只读字节视图：读取走引用，导出时才做防御性拷贝                                   |
| `peers.go`                         | `PeerPicker` / `PeerGetter` 接口，屏蔽底层是 HTTP 还是 gRPC         |
| `http.go`                          | `HTTPPool`：`/_gocache/<group>/<key>` 路由，响应体是 Protobuf 二进制 |
| `grpc.go`                          | `GRPCPool`：gRPC 服务端与客户端实现，复用 `ClientPool` 取连接             |
| `pool.go`                          | `ClientPool`：gRPC 连接池，Keepalive 保活 + 双检锁懒加载               |
| `gocache.go`                       | 全局 `Group` 注册表与 `DestroyGroup`                            |
| `cachepb/`                         | Protobuf 定义（`GetRequest` / `GetResponse`）与生成代码            |

## 目录结构

```
.
├── lru/                  # LRU 淘汰引擎
├── singleflight/         # 并发请求合并
├── consistenthash/       # 一致性哈希环
├── cachepb/              # Protobuf 定义与生成代码
├── cmd/demo/             # 3 节点 HTTP 集群演示
├── cmd/grpc_demo/        # 3 节点 gRPC 集群演示
├── group.go  cache.go  byteview.go  peers.go
├── http.go   grpc.go   pool.go     gocache.go
└── go.mod
```

## 快速开始

### 1. 运行测试

```bash
go test ./...
```

覆盖 LRU（淘汰、回调、重复写）、一致性哈希（分布均匀性）、singleflight（合并、不同 key、错误传播）
与 Group 端到端读写。

### 2. HTTP 三节点集群演示

```bash
go run ./cmd/demo
```

会在 8001 / 8002 / 8003 拉起三个节点（同一进程内，用 `NewIsolatedGroup` 模拟独立节点），
然后依次演示：

| 步骤  | 演示内容                                   | 关键日志                                                |
|:--- |:-------------------------------------- |:--------------------------------------------------- |
| 1   | 三节点就绪                                  | `[分布式节点就绪] ... http://127.0.0.1:800X`               |
| 2   | 首次查询 `Tom` 走回源（模拟 20ms 的 DB 延迟），再查命中本地 | `[DB Fallback Loading]` → `[Local Hit]`             |
| 2   | 向 8003 查同一个 key，验证跨节点路由                | `[Pick remote peer: ...8001]` → `[Remote Peer Hit]` |
| 3   | 20 个并发客户端同时请求未缓存的 `Alice`，对比 DB 命中次数   | `[DB Fallback Loading]` 只出现一次                       |

### 3. gRPC 三节点集群演示

```bash
go run ./cmd/grpc_demo
```

在 9001 / 9002 / 9003 上启动 gRPC 节点，演示 Protobuf 编解码、连接池复用与并发防击穿。

### 4. 作为库使用

```go
// 1) 创建缓存组：名字、容量上限、回源回调
g := gocache.NewGroup("scores", 2<<20, gocache.GetterFunc(
    func(key string) ([]byte, error) {
        return queryDB(key) // 查 DB / 调下游服务
    }))

// 2) 配置集群：本节点地址 + 全部节点地址
peers := gocache.NewHTTPPool("http://127.0.0.1:8001")
peers.Set("http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003")
peers.RegisterGroup(g)   // 让节点能响应其他节点对该 group 的查询
g.RegisterPeers(peers)   // 让 group 能按 key 路由到其他节点

// 3) 启动服务
http.ListenAndServe(":8001", peers)

// 4) 之后所有读取都走缓存
v, err := g.Get("Tom")   // 本地命中 / 远程拉取 / 本地回源 + singleflight
```

改用 gRPC 只需把 `NewHTTPPool` 换成 `NewGRPCPool`，并用
`cachepb.RegisterGroupCacheServer(grpcServer, pool)` 把节点注册成 gRPC 服务，业务代码不变。

## 基准测试

```bash
go test -bench=. -benchmem -run=none .
```

本机实测（24 核 / `-benchtime=100ms`，数据供参考，可用上面的命令复现）：

| 压测项                                 | ns/op   | 内存/次 | 堆分配/次    |
|:----------------------------------- | -------:| ----:| --------:|
| `BenchmarkLRUGet`（命中读取）             | 41.55   | 3 B  | 0 allocs |
| `BenchmarkLRUAdd`（写入/淘汰）            | 34.37   | 2 B  | 0 allocs |
| `BenchmarkCacheParallelGet`（并发读）    | 163.4   | 4 B  | 0 allocs |
| `BenchmarkConsistentHashGet`（哈希环路由） | 78.97   | 16 B | 1 allocs |
| `BenchmarkSingleflight`（热点 key 合并）  | 437 859 | 34 B | 0 allocs |

三个 LRU 相关基准都是 **0 堆分配**：`ByteView` 让值以引用方式在缓存与调用方之间传递。

`BenchmarkSingleflight` 的看点是合并比，而不是单次耗时——该基准里每次回源都 `sleep(10ms)`，
260 次并发调用最终只触发了 11 次真实回源：

```
Total Singleflight invocations: 260, Actual DB Hits: 11
```
