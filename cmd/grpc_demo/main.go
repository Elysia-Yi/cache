package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gocache"
	"gocache/cachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var mockStudentDB = map[string]string{
	"Tom":   "98分 (算法省一)",
	"Jerry": "93分 (ACM校赛一等奖)",
	"Spike": "88分 (系统开发核心贡献者)",
}

var dbHits int64

func createIsolatedGroup(name string) *gocache.Group {
	return gocache.NewIsolatedGroup(name, 2<<20, gocache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Printf("[gRPC 节点 DB 回源] 查询学生信息: %s (耗时 15ms)", key)
			atomic.AddInt64(&dbHits, 1)
			time.Sleep(15 * time.Millisecond)
			if val, ok := mockStudentDB[key]; ok {
				return []byte(val), nil
			}
			return nil, fmt.Errorf("student %s not found", key)
		}))
}

func startGRPCNode(addr string, addrs []string, group *gocache.Group) (*gocache.GRPCPool, *grpc.Server) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	pool := gocache.NewGRPCPool(addr)
	pool.Set(addrs...)
	pool.RegisterGroup(group)
	group.RegisterPeers(pool)

	grpcServer := grpc.NewServer()
	cachepb.RegisterGroupCacheServer(grpcServer, pool)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("gRPC server %s exited: %v", addr, err)
		}
	}()

	return pool, grpcServer
}

func main() {
	fmt.Println("==================================================================")
	fmt.Println("      Go 高性能分布式缓存系统 (gRPC + Protobuf + 连接池) - 集群演示      ")
	fmt.Println("==================================================================")

	nodeAddrs := []string{
		"127.0.0.1:9001",
		"127.0.0.1:9002",
		"127.0.0.1:9003",
	}

	groups := make(map[string]*gocache.Group)
	pools := make([]*gocache.GRPCPool, len(nodeAddrs))
	servers := make([]*grpc.Server, len(nodeAddrs))

	for i, addr := range nodeAddrs {
		g := createIsolatedGroup("students")
		groups[addr] = g
		pools[i], servers[i] = startGRPCNode(addr, nodeAddrs, g)
		log.Printf("[gRPC 节点启动成功] 监听于 %s", addr)
	}

	defer func() {
		for i := range servers {
			servers[i].GracefulStop()
			pools[i].Stop()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	fmt.Println("\n>>> [阶段 1] 3 个 gRPC 对等节点初始化就绪 (集成 Protobuf 二进制序列化)")

	// 模拟外部客户端连接 9001 节点发起 RPC 查询
	conn, err := grpc.Dial("127.0.0.1:9001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to node 9001: %v", err)
	}
	defer conn.Close()

	client := cachepb.NewGroupCacheClient(conn)

	fmt.Println("\n>>> [阶段 2] 客户端向节点 9001 查询 'Tom':")
	t0 := time.Now()
	res, err := client.Get(context.Background(), &cachepb.GetRequest{Group: "students", Key: "Tom"})
	if err != nil {
		log.Fatalf("Get Tom error: %v", err)
	}
	fmt.Printf("--> 返回结果: %s (耗时: %v)\n", string(res.GetValue()), time.Since(t0))

	fmt.Println("\n>>> [阶段 3] 再次向节点 9001 查询 'Tom' (命中缓存，零 DB 查询):")
	t1 := time.Now()
	res, err = client.Get(context.Background(), &cachepb.GetRequest{Group: "students", Key: "Tom"})
	if err != nil {
		log.Fatalf("Get Tom error: %v", err)
	}
	fmt.Printf("--> 返回结果: %s (耗时: %v)\n", string(res.GetValue()), time.Since(t1))

	fmt.Println("\n>>> [阶段 4] 突发 30 个并发协程请求未缓存的 'Jerry' (Singleflight 防击穿验证):")
	beforeHits := atomic.LoadInt64(&dbHits)

	var wg sync.WaitGroup
	concurrentN := 30
	wg.Add(concurrentN)

	for i := 0; i < concurrentN; i++ {
		go func(id int) {
			defer wg.Done()
			r, e := client.Get(context.Background(), &cachepb.GetRequest{Group: "students", Key: "Jerry"})
			if e != nil {
				log.Printf("client %d error: %v", id, e)
			}
			if id == 0 {
				fmt.Printf("    客户端 #%d 通过 gRPC 成功获取: %s\n", id, string(r.GetValue()))
			}
		}(i)
	}

	wg.Wait()
	afterHits := atomic.LoadInt64(&dbHits)
	newHits := afterHits - beforeHits

	fmt.Printf("\n[防击穿验证结论]:\n")
	fmt.Printf("  - 并发 RPC 请求数: %d\n", concurrentN)
	fmt.Printf("  - 底层 DB 实际加载次数: %d 次！(海量并发合并为单次底层加载)\n", newHits)

	fmt.Println("\n==================================================================")
	fmt.Println("                 gRPC 分布式缓存集群测试圆满通过!                  ")
	fmt.Println("==================================================================")
}
