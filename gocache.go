package gocache

// Version 当前分布式缓存框架版本
const Version = "1.0.0"

// DestroyGroup 注销指定名称的缓存组
func DestroyGroup(name string) {
	groupsMu.Lock()
	defer groupsMu.Unlock()
	delete(groups, name)
}
