package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"gocache"
)

// 模拟后端慢速数据库
var mockDB = map[string]string{
	"Tom":     "98分 (北京联合大学)",
	"Jack":    "85分 (清华大学)",
	"Sam":     "92分 (北京大学)",
	"Alice":   "95分 (浙江大学)",
	"Bob":     "89分 (复旦大学)",
	"Charlie": "91分 (上海交通大学)",
}

var dbQueryCount int64

// createGroup 创建具备真实回源回调的缓存命名空间
func createGroup() *gocache.Group {
	return gocache.NewIsolatedGroup("scores", 2<<20, gocache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Printf("[MySQL 模拟数据库] 正在执行 SQL 查询键: %s ... (模拟 20ms I/O 延迟)", key)
			atomic.AddInt64(&dbQueryCount, 1)
			time.Sleep(20 * time.Millisecond) // 模拟 DB 延迟

			if v, ok := mockDB[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("student %s not found in DB", key)
		}))
}

// startCacheServer 启动指定端口的分布式缓存 HTTP 服务端
func startCacheServer(addr string, addrs []string, group *gocache.Group) {
	peers := gocache.NewHTTPPool(addr)
	peers.Set(addrs...)
	peers.RegisterGroup(group)
	group.RegisterPeers(peers)
	log.Printf("[分布式节点就绪] GoCache HTTP 节点运行在: %s", addr)
	server := &http.Server{Addr: addr[7:], Handler: peers} // 截掉 "http://" 前缀
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server %s failed: %v", addr, err)
	}
}

// runClusterDemo 运行 3 节点集群演示
func runClusterDemo() {
	fmt.Println("==================================================================")
	fmt.Println("        Go 高性能分布式缓存系统 (GroupCache 架构) - 集群演示        ")
	fmt.Println("==================================================================")

	nodeAddrs := []string{
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8002",
		"http://127.0.0.1:8003",
	}

	for _, addr := range nodeAddrs {
		g := createGroup()
		go startCacheServer(addr, nodeAddrs, g)
	}

	// 等待 3 个节点全部启动监听
	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n>>> [步骤 1] 3 节点集群启动完毕，节点列表:", nodeAddrs)

	client := &http.Client{Timeout: 3 * time.Second}
	query := func(clientPort, key string) string {
		url := fmt.Sprintf("http://127.0.0.1:%s/_gocache/scores/%s", clientPort, key)
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Sprintf("Error: %v", err)
		}
		defer resp.Body.Close()
		buf := make([]byte, 256)
		n, _ := resp.Body.Read(buf)
		return string(buf[:n])
	}

	fmt.Println("\n>>> [步骤 2] 测试一致性哈希跨节点协同路由与回源：")
	fmt.Println("向 8001 节点查询 'Tom':")
	t0 := time.Now()
	val1 := query("8001", "Tom")
	fmt.Printf("--> 结果: %s (耗时: %v)\n", val1, time.Since(t0))

	fmt.Println("\n再次向 8001 节点查询 'Tom' (此时应命中缓存，耗时接近 0ms):")
	t1 := time.Now()
	val2 := query("8001", "Tom")
	fmt.Printf("--> 结果: %s (耗时: %v)\n", val2, time.Since(t1))

	fmt.Println("\n向 8003 节点查询 'Tom' (验证跨节点数据路由):")
	t2 := time.Now()
	val3 := query("8003", "Tom")
	fmt.Printf("--> 结果: %s (耗时: %v)\n", val3, time.Since(t2))

	fmt.Println("\n>>> [步骤 3] 测试 Singleflight 防缓存击穿 (并发热点突发测试)：")
	fmt.Println("模拟 20 个并发客户端同一瞬间向 8002 节点请求尚未被缓存的 'Alice'...")
	beforeDBCount := atomic.LoadInt64(&dbQueryCount)

	var wg sync.WaitGroup
	concurrentReqs := 20
	wg.Add(concurrentReqs)

	for i := 0; i < concurrentReqs; i++ {
		go func(id int) {
			defer wg.Done()
			res := query("8002", "Alice")
			if id == 0 {
				fmt.Printf("    客户端 #%d 查询成功: %s\n", id, res)
			}
		}(i)
	}

	wg.Wait()
	afterDBCount := atomic.LoadInt64(&dbQueryCount)
	dbHitsForAlice := afterDBCount - beforeDBCount

	fmt.Printf("\n[防击穿验证结论]:\n")
	fmt.Printf("  - 并发请求数: %d\n", concurrentReqs)
	fmt.Printf("  - 底层 DB 实际回源次数: %d 次！(彻底杜绝击穿，其余全部被 Singleflight 拦截并复用结果)\n", dbHitsForAlice)

	fmt.Println("\n==================================================================")
	fmt.Println("                   集群演示成功执行完毕!                           ")
	fmt.Println("==================================================================")
}

func main() {
	var port int
	var runDemo bool
	flag.IntVar(&port, "port", 8001, "GoCache node port")
	flag.BoolVar(&runDemo, "demo", true, "Run multi-node cluster demo")
	flag.Parse()

	if runDemo {
		runClusterDemo()
		return
	}

	// 独立单节点启动模式
	nodeAddr := fmt.Sprintf("http://127.0.0.1:%d", port)
	addrs := []string{
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8002",
		"http://127.0.0.1:8003",
	}
	g := createGroup()
	startCacheServer(nodeAddr, addrs, g)
}
