package gocache

// ByteView 是缓存值的只读视图；b 未导出，外部只能拿到拷贝，避免写入污染内部数据。
type ByteView struct {
	b []byte
}

// Len 实现 lru.Value 接口，返回值占用的字节数。
func (v ByteView) Len() int {
	return len(v.b)
}

// ByteSlice 返回独立拷贝副本，防止外部修改影响缓存内部数据。
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

// String 以字符串形式返回值。
func (v ByteView) String() string {
	return string(v.b)
}

// cloneBytes 深拷贝字节切片。
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
