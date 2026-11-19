package gocache

import "gocache/cachepb"

// PeerPicker 按 key 在集群中定位负责节点。
type PeerPicker interface {
	// PickPeer 返回负责该 key 的远程节点；若为本机或哈希环为空则返回 nil, false。
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 是远程取值接口，屏蔽底层 HTTP 或 gRPC 与序列化细节。
type PeerGetter interface {
	// Get 请求远程节点，并把结果反序列化填入 out。
	Get(in *cachepb.GetRequest, out *cachepb.GetResponse) error
}
