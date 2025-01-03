package magicsock

import (
	"fmt"
	"testing"
)

func TestUDPSpeeder(t *testing.T) {
	// 1. 初始化测试数据
	testData := "Hello, this is a UDP test message!"
	inputBuff := [][]byte{[]byte(testData)}

	// 2. 创建 UDPSpeeder 实例
	speeder, err := NewUDPSpeeder()
	if err != nil {
		t.Fatalf("Failed to create UDPSpeeder: %v", err)
	}

	// 3. 模拟发送过程
	encodedPackets, err := speeder.SpeedSend(inputBuff)
	if err != nil {
		t.Fatalf("SpeedSend failed: %v", err)
	}

	// 4. 准备接收数据
	// 模拟网络传输,创建接收到的数据包和大小数组
	receivedSizes := make([]int, len(encodedPackets))
	for i, packet := range encodedPackets {
		receivedSizes[i] = len(packet)
	}

	// 5. 模拟接收过程
	decodedBuffs, _, err := speeder.HandleReceive(encodedPackets, receivedSizes)
	if err != nil {
		t.Fatalf("HandleReceive failed: %v", err)
	}

	// 6. 验证结果
	if len(decodedBuffs) != 1 {
		t.Fatalf("Expected 1 decoded buffer, got %d", len(decodedBuffs))
	}

	receivedMsg := string(decodedBuffs[0])
	fmt.Printf("Original message: %s\n", testData)
	fmt.Printf("Received message: %s\n", receivedMsg)

	if receivedMsg != testData {
		t.Errorf("Message mismatch\nExpected: %s\nGot: %s", testData, receivedMsg)
	}

	// 7. 输出一些统计信息
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("Original data size: %d bytes\n", len(testData))
	fmt.Printf("Number of encoded packets: %d\n", len(encodedPackets))
	fmt.Printf("Total encoded size: %d bytes\n", sumPacketSizes(encodedPackets))
	fmt.Printf("Overhead ratio: %.2f\n", float64(sumPacketSizes(encodedPackets))/float64(len(testData)))
}

// 辅助函数:计算数据包总大小
func sumPacketSizes(packets [][]byte) int {
	total := 0
	for _, p := range packets {
		total += len(p)
	}
	return total
}
