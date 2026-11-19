package consistenthash

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

// TestHashing 采用确定性自定义哈希函数验证顺时针寻址与回绕逻辑
func TestHashing(t *testing.T) {
	// 自定义哈希函数：直接将数字字符串转为整数
	// 虚拟节点名格式为 "02", "12", "22", "04", "14", "24", "06", "16", "26"
	hash := New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	// 真实节点 "2"、"4"、"6" 各对应 3 个虚拟节点
	// 环升序排列: 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2", // hash=2  -> 匹配到 2 (node 2)
		"11": "2", // hash=11 -> 第一个>=11的是 12 (node 2)
		"23": "4", // hash=23 -> 第一个>=23的是 24 (node 4)
		"27": "2", // hash=27 -> 超过最大值 26，环形回绕到 2 (node 2)
	}

	for k, v := range testCases {
		if node := hash.Get(k); node != v {
			t.Errorf("Asking for %s, should have yielded %s, got %s", k, v, node)
		}
	}

	// 扩容：加入节点 "8"（虚拟节点 08/18/28）
	hash.Add("8")

	// 此时 27 的下一个节点不再是回绕的 2，而是 28 (node 8)
	testCases["27"] = "8"

	for k, v := range testCases {
		if node := hash.Get(k); node != v {
			t.Errorf("Asking for %s after Add('8'), should have yielded %s, got %s", k, v, node)
		}
	}

	// 动态缩容：删除节点 "8"
	hash.Remove("8")
	if node := hash.Get("27"); node != "2" {
		t.Errorf("Asking for 27 after Remove('8'), should revert to 2, got %s", node)
	}
}

// TestDataSkewControl 测试虚拟节点倍数对“数据倾斜”的平抑效果
func TestDataSkewControl(t *testing.T) {
	nodes := []string{"nodeA:8001", "nodeB:8002", "nodeC:8003"}
	const totalKeys = 3000

	// 低虚拟节点（replicas=1）时数据明显倾斜
	lowHash := New(1, nil)
	lowHash.Add(nodes...)
	lowCounts := make(map[string]int)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		n := lowHash.Get(key)
		lowCounts[n]++
	}

	// 高虚拟节点（replicas=100）时分布更均匀
	highHash := New(100, nil)
	highHash.Add(nodes...)
	highCounts := make(map[string]int)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		n := highHash.Get(key)
		highCounts[n]++
	}

	// 计算标准差
	calcStdDev := func(counts map[string]int, total, n int) float64 {
		mean := float64(total) / float64(n)
		var sumSquares float64
		for _, c := range counts {
			diff := float64(c) - mean
			sumSquares += diff * diff
		}
		return math.Sqrt(sumSquares / float64(n))
	}

	lowStdDev := calcStdDev(lowCounts, totalKeys, len(nodes))
	highStdDev := calcStdDev(highCounts, totalKeys, len(nodes))

	t.Logf("低虚拟节点 (replicas=1) 分布: %v, 标准差: %.2f", lowCounts, lowStdDev)
	t.Logf("高虚拟节点 (replicas=100) 分布: %v, 标准差: %.2f", highCounts, highStdDev)

	// 高虚拟节点的标准差应显著低于低虚拟节点
	if highStdDev >= lowStdDev {
		t.Logf("提示: 在少量采样下 highStdDev(%.2f) 应优于 lowStdDev(%.2f)", highStdDev, lowStdDev)
	}
}
