package magicsock

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

// UDPSpeeder 处理 UDP 加速相关功能
type UDPSpeeder struct {
	mu        sync.Mutex
	encoderMu sync.Mutex
	decoderMu sync.Mutex
	seqNum    uint32
	// 数据包缓存，key为序列号，value为接收到的数据包
	packetCache map[uint32]*packetGroup
	// 最后一次清理缓存的时间
	lastCleanup time.Time
	// 添加编码器作为成员
	encoder reedsolomon.Encoder
	decoder reedsolomon.Encoder
}

type packetGroup struct {
	packets    [][]byte
	sizes      []int
	createTime time.Time
	checksum   uint32
}

// 包头结构
const (
	headerSize    = 16   // 序列号(4字节) + 分片索引(2字节) + 总分片数(2字节) + 校验和(4字节) + 魔术数字(4字节)
	dataShards    = 20   // 原始数据分片数
	parityShards  = 10   // 冗余数据分片数
	maxPacketSize = 1400 // UDP包最大大小(预留头部空间)
	// 添加魔术数字常量
	magicNumber = 0x54534653 // "TSFS" in hex
	// 添加超时相关常量
	packetTimeout   = 30 * time.Second
	cleanupInterval = 1 * time.Minute
	minShardSize    = 64 // 最小分片大小
)

// 定义错误类型
type UDPSpeederError struct {
	Code    int
	Message string
}

const (
	ErrInvalidPacket = iota
	ErrChecksumMismatch
	ErrInsufficientShards
	ErrReconstructFailed
)

func (e *UDPSpeederError) Error() string {
	return e.Message
}

// SpeedSend 处理发送数据的加速
// buffs: 原始数据包切片
// addr: 目标地址
// 返回:
// [][]byte: 处理后要发送的数据包(包含原始数据和冗余数据)
// error: 处理过程中的错误
func (s *UDPSpeeder) SpeedSend(buffs [][]byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentSeq := s.seqNum
	s.seqNum++

	var result [][]byte

	// 处理每个输入包
	for _, buff := range buffs {
		// 计算校验和
		checksum := calculateChecksum(buff)

		// 计算每个分片的大小，确保所有分片大小相同
		shardSize := (len(buff) + dataShards - 1) / dataShards
		if shardSize < minShardSize {
			shardSize = minShardSize
		}
		if shardSize+headerSize > maxPacketSize {
			return nil, &UDPSpeederError{
				Code:    ErrInvalidPacket,
				Message: "packet size exceeds maximum allowed size",
			}
		}

		// 创建固定大小的分片数组
		shards := make([][]byte, dataShards+parityShards)
		for i := range shards {
			shards[i] = make([]byte, shardSize)
		}

		// 将原始数据复制到分片中
		for i := 0; i < dataShards; i++ {
			start := i * shardSize
			end := start + shardSize
			if start < len(buff) {
				if end > len(buff) {
					end = len(buff)
				}
				copy(shards[i], buff[start:end])
			}
		}

		// 生成冗余分片
		s.encoderMu.Lock()
		err := s.encoder.Encode(shards)
		s.encoderMu.Unlock()
		if err != nil {
			return nil, &UDPSpeederError{
				Code:    ErrReconstructFailed,
				Message: "failed to encode shards: " + err.Error(),
			}
		}

		// 为所有分片添加头部
		for i := 0; i < dataShards+parityShards; i++ {
			// 创建带头部的完整分片
			packetWithHeader := make([]byte, headerSize+shardSize)

			// 添加头部信息
			binary.BigEndian.PutUint32(packetWithHeader[0:4], currentSeq)
			binary.BigEndian.PutUint16(packetWithHeader[4:6], uint16(i))
			binary.BigEndian.PutUint16(packetWithHeader[6:8], uint16(dataShards+parityShards))
			binary.BigEndian.PutUint32(packetWithHeader[8:12], checksum)
			// 添加魔术数字
			binary.BigEndian.PutUint32(packetWithHeader[12:16], magicNumber)

			// 复制分片数据
			copy(packetWithHeader[headerSize:], shards[i])

			result = append(result, packetWithHeader)
		}

		// 清理分片数据
		for i := range shards {
			shards[i] = nil
		}
	}

	return result, nil
}

// HandleReceive 处理接收数据的恢复和重组
// packets: 接收到的数据包
// sizes: 每个数据包的大小
// 返回:
// [][]byte: 恢复后的原始数据包
// []int: 恢复后数据包的大小
// error: 处理过程中的错误
func (s *UDPSpeeder) HandleReceive(packets [][]byte, sizes []int) ([][]byte, []int, error) {
	if len(packets) == 0 || len(sizes) == 0 {
		return nil, nil, nil
	}

	// 检查第一个包是否包含魔术数字
	if sizes[0] < headerSize {
		// 包太小,不是 UDPSpeeder 的包,返回原始数据
		return packets, sizes, nil
	}

	magic := binary.BigEndian.Uint32(packets[0][12:16])
	if magic != magicNumber {
		// 不是 UDPSpeeder 的包,返回原始数据
		return packets, sizes, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanExpiredPackets()

	// 处理新接收的数据包
	for i, packet := range packets {
		if sizes[i] < headerSize {
			continue
		}

		seqNum := binary.BigEndian.Uint32(packet[0:4])
		shardIndex := binary.BigEndian.Uint16(packet[4:6])
		totalShards := binary.BigEndian.Uint16(packet[6:8])
		storedChecksum := binary.BigEndian.Uint32(packet[8:12])

		if totalShards != dataShards+parityShards {
			return nil, nil, &UDPSpeederError{
				Code:    ErrInvalidPacket,
				Message: "invalid total shards count",
			}
		}

		// 创建或获取数据包组
		group, exists := s.packetCache[seqNum]
		if !exists {
			group = &packetGroup{
				packets:    make([][]byte, totalShards),
				sizes:      make([]int, totalShards),
				createTime: time.Now(),
			}
			s.packetCache[seqNum] = group
		}

		// 保存分片数据和校验和
		shardData := make([]byte, sizes[i]-headerSize)
		copy(shardData, packet[headerSize:])
		group.packets[shardIndex] = shardData
		if shardIndex == 0 {
			group.checksum = storedChecksum
		}
		group.sizes[shardIndex] = sizes[i] - headerSize
	}

	var resultBuffs [][]byte
	var resultSizes []int

	// 按序处理缓存中的数据包
	seqNums := make([]uint32, 0, len(s.packetCache))
	for seq := range s.packetCache {
		seqNums = append(seqNums, seq)
	}
	sort.Slice(seqNums, func(i, j int) bool { return seqNums[i] < seqNums[j] })

	for _, seq := range seqNums {
		group := s.packetCache[seq]

		// 检查是否有足够的分片进行恢复
		validShards := 0
		for _, p := range group.packets {
			if p != nil {
				validShards++
			}
		}

		// 如果收到的分片数量不足以恢复，且未超时，则继续等待
		if validShards < dataShards {
			if time.Since(group.createTime) >= packetTimeout {
				delete(s.packetCache, seq)
			}
			continue
		}

		// 尝试恢复数据
		s.decoderMu.Lock()
		err := s.decoder.Reconstruct(group.packets)
		s.decoderMu.Unlock()
		if err != nil {
			delete(s.packetCache, seq)
			continue
		}

		// 重组原始数据
		var originalData []byte
		for i := 0; i < dataShards; i++ {
			if group.packets[i] != nil {
				originalData = append(originalData, group.packets[i]...)
			}
		}

		// 移除填充的零值
		originalData = bytes.TrimRight(originalData, "\x00")

		// 验证校验和
		calculatedChecksum := calculateChecksum(originalData)
		if calculatedChecksum == group.checksum {
			resultBuffs = append(resultBuffs, originalData)
			resultSizes = append(resultSizes, len(originalData))
		}

		// 清理已处理的数据
		delete(s.packetCache, seq)
	}

	return resultBuffs, resultSizes, nil
}

// 清理过期的数据包缓存
func (s *UDPSpeeder) cleanExpiredPackets() {
	now := time.Now()
	if now.Sub(s.lastCleanup) < cleanupInterval {
		return
	}

	for seq, group := range s.packetCache {
		if now.Sub(group.createTime) > packetTimeout {
			delete(s.packetCache, seq)
		}
	}
	s.lastCleanup = now
}

// NewUDPSpeeder 创建一个新的 UDPSpeeder 实例
func NewUDPSpeeder() (*UDPSpeeder, error) {
	enc, err := reedsolomon.New(dataShards, parityShards, reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, err
	}

	dec, err := reedsolomon.New(dataShards, parityShards, reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, err
	}

	return &UDPSpeeder{
		seqNum:      0,
		packetCache: make(map[uint32]*packetGroup),
		lastCleanup: time.Now(),
		encoder:     enc,
		decoder:     dec,
	}, nil
}

// 添加数据校验和计算函数
func calculateChecksum(data []byte) uint32 {
	var checksum uint32
	for i := 0; i < len(data); i += 4 {
		if i+4 <= len(data) {
			checksum ^= binary.BigEndian.Uint32(data[i : i+4])
		} else {
			var lastBytes [4]byte
			copy(lastBytes[:], data[i:])
			checksum ^= binary.BigEndian.Uint32(lastBytes[:])
		}
	}
	return checksum
}

// 添加数据校验和验证函数
func verifyChecksum(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	storedChecksum := binary.BigEndian.Uint32(data[:4])
	calculatedChecksum := calculateChecksum(data[4:])
	return storedChecksum == calculatedChecksum
}
