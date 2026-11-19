package singleflight

import "sync"

// call 是一次进行中的调用，跟随者通过 WaitGroup 等待 leader 结束。
type call struct {
	wg  sync.WaitGroup // 等待 leader 完成的跟随者协程
	val interface{}    // 底层函数的返回值
	err error          // 底层函数的错误
}

// Group 把同一 key 的并发请求合并为一次底层加载，避免缓存击穿。
type Group struct {
	mu sync.Mutex       // 保护 m
	m  map[string]*call // 进行中的 key -> call
}

// Do 对同一 key 只执行一次 fn，其余并发调用者阻塞并复用同一结果。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	// 已有调用在途，等待其完成；等待期间不持锁
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	// 本协程作为 leader 负责实际加载
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	// 执行加载，期间不持锁
	c.val, c.err = fn()

	c.wg.Done()

	// 结束后立即移除，允许后续请求重新加载
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
