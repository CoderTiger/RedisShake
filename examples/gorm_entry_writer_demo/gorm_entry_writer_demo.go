package main

import (
	"RedisShake/common"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

type GormEntryWriterDemo struct {
	db     *gorm.DB
	debugF func(msg string, args ...interface{})
	infoF  func(msg string, args ...interface{})
	warnF  func(msg string, args ...interface{})
	panicF func(msg string, args ...interface{})
}

// 数据库表结构体定义
type RedisString struct {
	KeyHash   string    `gorm:"column:key_hash;primaryKey"`
	Key       string    `gorm:"column:key"`
	Value     []byte    `gorm:"column:value;type:varbinary(10240)"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

type RedisHash struct {
	KeyHash   string    `gorm:"column:key_hash;primaryKey"`
	Key       string    `gorm:"column:key"`
	FieldHash string    `gorm:"column:field_hash;primaryKey"`
	Field     string    `gorm:"column:field"`
	Value     []byte    `gorm:"column:value;type:varbinary(10240)"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

type RedisList struct {
	KeyHash   string    `gorm:"column:key_hash;primaryKey"`
	Key       string    `gorm:"column:key"`
	Position  uint      `gorm:"column:position;primaryKey"`
	Value     []byte    `gorm:"column:value;type:varbinary(10240)"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

type RedisSet struct {
	KeyHash    string    `gorm:"column:key_hash;primaryKey"`
	Key        string    `gorm:"column:key"`
	MemberHash string    `gorm:"column:member_hash;primaryKey"`
	Member     string    `gorm:"column:member"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

type RedisZSet struct {
	KeyHash    string    `gorm:"column:key_hash;primaryKey"`
	Key        string    `gorm:"column:key"`
	MemberHash string    `gorm:"column:member_hash;primaryKey"`
	Member     string    `gorm:"column:member"`
	Score      float64   `gorm:"column:score"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

func (RedisString) TableName() string { return "redis_string" }
func (RedisHash) TableName() string   { return "redis_hash" }
func (RedisList) TableName() string   { return "redis_list" }
func (RedisSet) TableName() string    { return "redis_set" }
func (RedisZSet) TableName() string   { return "redis_zset" }

func NewWriter(config string, db *gorm.DB, debugF, infoF, warnF, panicF func(msg string, args ...interface{})) common.GormEntryWriter {
	infoF("Creating new GormEntryWriterDemo with config: %s", config)
	demo := &GormEntryWriterDemo{
		db:     db,
		debugF: debugF,
		infoF:  infoF,
		warnF:  warnF,
		panicF: panicF,
	}
	return demo
}

//go:embed assets/sql/*.sql
var sqlFS embed.FS

var sqlFileNames = []string{
	"redis_string.sql",
	"redis_hash.sql",
	"redis_list.sql",
	"redis_set.sql",
	"redis_zset.sql"}

func (w *GormEntryWriterDemo) Init() error {
	for _, fileName := range sqlFileNames {
		data, err := sqlFS.ReadFile("assets/sql/" + fileName)
		if err != nil {
			w.panicF("failed to read SQL file %s: %w", fileName, err)
		}
		if err := w.db.Exec(string(data)).Error; err != nil {
			w.panicF("failed to execute SQL from %s: %w", fileName, err)
		}
	}
	return nil
}

func (w *GormEntryWriterDemo) Write(e *common.Entry) error {
	w.debugF("Writing entry: DbId=%d, CmdName=%s, Keys=%v, Group=%s", e.DbId, e.CmdName, e.Keys, e.Group)
	cmd := strings.ToLower(e.CmdName)
	switch cmd {
	case "restore": // sync with scan reader
		if err := w.HandleRestoreCommand(e); err != nil {
			return err
		}
	case "ping":
		// todo: implement the logic to handle the ping command
		w.infoF("Handling ping command for entry with DbId=%d", e.DbId)
	default:
		w.infoF("Skipping unsupported command %s for entry with DbId=%d, Keys=%v", cmd, e.DbId, e.Keys)
	}

	return nil
}

func (w *GormEntryWriterDemo) Close() error {
	w.db = nil
	return nil
}

func (w *GormEntryWriterDemo) decodeRDBString(data string) []byte {
	if len(data) == 0 {
		return []byte{}
	}

	// RDB 字符串编码格式:
	// - 如果第一个字节的前两位是 00，后6位表示长度
	// - 如果第一个字节的前两位是 01，该字节的后6位和下一个字节组成14位长度
	// - 如果第一个字节的前两位是 10，后面4个字节是32位长度
	// - 如果第一个字节的前两位是 11，表示特殊编码

	firstByte := data[0]
	encoding := (firstByte & 0xC0) >> 6 // 获取前两位

	switch encoding {
	case 0: // 00xxxxxx - 6位长度
		length := int(firstByte & 0x3F)
		if len(data) < 1+length {
			w.panicF("data too short for 6-bit length encoding")
		}
		return []byte(data[1 : 1+length])

	case 1: // 01xxxxxx - 14位长度
		if len(data) < 2 {
			w.panicF("data too short for 14-bit length encoding")
		}
		length := int(firstByte&0x3F)<<8 | int(data[1])
		if len(data) < 2+length {
			w.panicF("data too short for 14-bit length encoding")
		}
		return []byte(data[2 : 2+length])

	case 2: // 10xxxxxx - 32位长度
		if len(data) < 5 {
			w.panicF("data too short for 32-bit length encoding")
		}
		length := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		if len(data) < 5+length {
			w.panicF("data too short for 32-bit length encoding")
		}
		return []byte(data[5 : 5+length])

	case 3: // 11xxxxxx - 特殊编码
		format := firstByte & 0x3F
		switch format {
		case 0: // 8位整数
			if len(data) < 2 {
				w.panicF("data too short for 8-bit integer")
			}
			return []byte(strconv.Itoa(int(int8(data[1]))))
		case 1: // 16位整数
			if len(data) < 3 {
				w.panicF("data too short for 16-bit integer")
			}
			val := int16(data[1]) | int16(data[2])<<8
			return []byte(strconv.Itoa(int(val)))
		case 2: // 32位整数
			if len(data) < 5 {
				w.panicF("data too short for 32-bit integer")
			}
			val := int32(data[1]) | int32(data[2])<<8 | int32(data[3])<<16 | int32(data[4])<<24
			return []byte(strconv.Itoa(int(val)))
		case 3: // LZF压缩字符串
			w.panicF("LZF compressed strings not supported")
		default:
			w.panicF("unknown special encoding format: %d", format)
		}

	default:
		w.panicF("unknown encoding type: %d", encoding)
	}
	return nil // This line will never be reached due to panic
}

func (w *GormEntryWriterDemo) HandleRestoreCommand(e *common.Entry) error {
	w.debugF("Handling RESTORE command for entry: DbId=%d, CmdName=%s, Keys=%v, Group=%s", e.DbId, e.CmdName, e.Keys, e.Group)
	// RESTORE 命令格式: RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
	if len(e.Argv) < 4 {
		w.panicF("invalid RESTORE command, expected at least 4 arguments, got %d", len(e.Argv))
	}

	key := e.Argv[1]
	ttlStr := e.Argv[2]
	serializedValue := e.Argv[3]

	// 解析 TTL (暂时不使用，但保留用于将来扩展)
	_, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		w.panicF("invalid TTL value: %s", ttlStr)
	}
	w.debugF("Handling RESTORE command for key %s with TTL %s", key, ttlStr)

	// 计算 key 的 SHA256 哈希
	keyHash := w.calculateHash(key)

	// 解析序列化的 Redis 数据
	if len(serializedValue) < 11 { // 至少需要 type(1) + data + version(2) + crc(8)
		w.panicF("invalid serialized value, too short")
	}

	// 获取数据类型 (第一个字节)
	dataType := serializedValue[0]

	// 获取实际数据 (去掉第一个字节和最后10个字节的版本和CRC)
	actualData := serializedValue[1 : len(serializedValue)-10]

	// 根据数据类型处理数据
	switch dataType {
	case 0: // RDB_TYPE_STRING
		return w.handleStringType(key, keyHash, actualData)
	// case 1: // RDB_TYPE_LIST
	// 	return w.handleListType(key, keyHash, actualData)
	// case 2: // RDB_TYPE_SET
	// 	return w.handleSetType(key, keyHash, actualData)
	case 3, 5: // RDB_TYPE_ZSET, RDB_TYPE_ZSET_2
		return w.handleZSetType(key, keyHash, actualData)
	case 4: // RDB_TYPE_HASH
		return w.handleHashType(key, keyHash, actualData)
	// case 10: // RDB_TYPE_LIST_ZIPLIST
	// 	return w.handleListZiplistType(key, keyHash, actualData)
	// case 11: // RDB_TYPE_SET_INTSET
	// 	return w.handleSetIntsetType(key, keyHash, actualData)
	// case 12: // RDB_TYPE_ZSET_ZIPLIST
	// 	return w.handleZSetZiplistType(key, keyHash, actualData)
	// case 13: // RDB_TYPE_HASH_ZIPLIST
	// 	return w.handleHashZiplistType(key, keyHash, actualData)
	// case 14, 18: // RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2
	// 	return w.handleListQuicklistType(key, keyHash, actualData)
	case 16: // RDB_TYPE_HASH_LISTPACK
		return w.handleHashListpackType(key, keyHash, actualData)
	// case 17: // RDB_TYPE_ZSET_LISTPACK
	// 	return w.handleZSetListpackType(key, keyHash, actualData)
	// case 20: // RDB_TYPE_SET_LISTPACK
	// 	return w.handleSetListpackType(key, keyHash, actualData)
	default:
		w.warnF("Unsupported RDB data type %d for key %s", dataType, key)
		return nil // 跳过不支持的类型，不报错
	}
}

// 计算字符串的 SHA256 哈希
func (w *GormEntryWriterDemo) calculateHash(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// decodeRDBLength 解码 RDB 长度编码
func (w *GormEntryWriterDemo) decodeRDBLength(data string, pos int) (int64, int) {
	if pos >= len(data) {
		w.panicF("position %d exceeds data length %d", pos, len(data))
	}

	firstByte := data[pos]
	encoding := (firstByte & 0xC0) >> 6 // 获取前两位

	switch encoding {
	case 0: // 00xxxxxx - 6位长度
		length := int64(firstByte & 0x3F)
		return length, 1

	case 1: // 01xxxxxx - 14位长度
		if pos+1 >= len(data) {
			w.panicF("data too short for 14-bit length encoding")
		}
		length := int64(firstByte&0x3F)<<8 | int64(data[pos+1])
		return length, 2

	case 2: // 10xxxxxx - 32位长度
		if pos+4 >= len(data) {
			w.panicF("data too short for 32-bit length encoding")
		}
		length := int64(data[pos+1])<<24 | int64(data[pos+2])<<16 | int64(data[pos+3])<<8 | int64(data[pos+4])
		return length, 5

	case 3: // 11xxxxxx - 特殊编码（这里应该是长度，不是特殊格式）
		format := firstByte & 0x3F
		switch format {
		case 0: // 后面跟8位整数表示长度
			if pos+1 >= len(data) {
				w.panicF("data too short for 8-bit length")
			}
			return int64(data[pos+1]), 2
		case 1: // 后面跟16位整数表示长度
			if pos+2 >= len(data) {
				w.panicF("data too short for 16-bit length")
			}
			length := int64(data[pos+1]) | int64(data[pos+2])<<8
			return length, 3
		case 2: // 后面跟32位整数表示长度
			if pos+4 >= len(data) {
				w.panicF("data too short for 32-bit length")
			}
			length := int64(data[pos+1]) | int64(data[pos+2])<<8 | int64(data[pos+3])<<16 | int64(data[pos+4])<<24
			return length, 5
		default:
			w.panicF("unknown length encoding format: %d", format)
		}

	default:
		w.panicF("invalid length encoding: %d", encoding)
	}
	return 0, 0 // This line will never be reached due to panic
}

// calculateRDBStringLength 计算 RDB 字符串编码占用的字节数
func (w *GormEntryWriterDemo) calculateRDBStringLength(data string) int {
	if len(data) == 0 {
		w.panicF("empty data")
	}

	firstByte := data[0]
	encoding := (firstByte & 0xC0) >> 6 // 获取前两位

	switch encoding {
	case 0: // 00xxxxxx - 6位长度
		length := int(firstByte & 0x3F)
		return 1 + length

	case 1: // 01xxxxxx - 14位长度
		if len(data) < 2 {
			w.panicF("data too short for 14-bit length encoding")
		}
		length := int(firstByte&0x3F)<<8 | int(data[1])
		return 2 + length

	case 2: // 10xxxxxx - 32位长度
		if len(data) < 5 {
			w.panicF("data too short for 32-bit length encoding")
		}
		length := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		return 5 + length

	case 3: // 11xxxxxx - 特殊编码
		format := firstByte & 0x3F
		switch format {
		case 0: // 8位整数
			return 2
		case 1: // 16位整数
			return 3
		case 2: // 32位整数
			return 5
		case 3: // LZF压缩字符串
			w.panicF("LZF compressed strings not supported")
		default:
			w.panicF("unknown special encoding format: %d", format)
		}

	default:
		w.panicF("invalid encoding: %d", encoding)
	}
	return 0 // This line will never be reached due to panic
}

// 处理 String 类型
func (w *GormEntryWriterDemo) handleStringType(key, keyHash, data string) error {
	// 解码 RDB 字符串数据，移除长度编码前缀
	decodedValue := w.decodeRDBString(data)

	record := RedisString{
		KeyHash:   keyHash,
		Key:       key,
		Value:     decodedValue,
		CreatedAt: time.Now(),
	}

	if err := w.db.Save(&record).Error; err != nil {
		w.panicF("failed to save string record for key %s: %v", key, err)
	}
	return nil
}

// 处理 Hash 类型 (RDB_TYPE_HASH)
func (w *GormEntryWriterDemo) handleHashType(key, keyHash, data string) error {
	w.debugF("Handling Hash for key %s with data length: %d, data-hex: %x", key, len(data), data)

	if len(data) == 0 {
		w.warnF("Empty hash data for key %s", key)
		return nil
	}

	pos := 0

	// 首先读取哈希表的大小
	hashSize, bytesRead := w.decodeRDBLength(data, pos)
	pos += bytesRead

	w.debugF("Hash size: %d entries for key %s", hashSize, key)

	// 解析每个 field-value 对
	for i := 0; i < int(hashSize); i++ {
		// 解码 field
		fieldValue := w.decodeRDBString(data[pos:])
		field := string(fieldValue)

		// 计算field消耗的字节数以移动位置
		fieldBytesUsed := w.calculateRDBStringLength(data[pos:])
		pos += fieldBytesUsed

		// 解码 value
		valueData := w.decodeRDBString(data[pos:])
		value := valueData

		// 计算value消耗的字节数以移动位置
		valueBytesUsed := w.calculateRDBStringLength(data[pos:])
		pos += valueBytesUsed

		// 保存到数据库
		fieldHash := w.calculateHash(field)
		record := RedisHash{
			KeyHash:   keyHash,
			Key:       key,
			FieldHash: fieldHash,
			Field:     field,
			Value:     value,
			CreatedAt: time.Now(),
		}

		if err := w.db.Save(&record).Error; err != nil {
			w.panicF("failed to save hash field %s for key %s: %v", field, key, err)
		}

		w.debugF("Saved hash field %d: key=%s, field=%s, value=%s", i, key, field, string(value))
	}

	w.debugF("Successfully processed %d hash entries for key %s", hashSize, key)
	return nil
}

func (w *GormEntryWriterDemo) handleHashListpackType(key, keyHash, data string) error {
	w.debugF("Handling Hash Listpack for key %s with data length: %d, data-hex: %x", key, len(data), data)

	// 解析 Listpack 格式的 Hash 数据
	entries := w.parseListpack(data)

	// Hash Listpack 格式：field1, value1, field2, value2, ...
	if len(entries)%2 != 0 {
		w.warnF("Odd number of entries (%d) for key %s, truncating last entry", len(entries), key)
	}

	w.debugF("Successfully parsed %d entries (%d field-value pairs) for key %s", len(entries), len(entries)/2, key)

	// 逐对处理 field-value
	for i := 0; i < len(entries); i += 2 {
		field := entries[i]
		value := entries[i+1]

		fieldHash := w.calculateHash(field)
		record := RedisHash{
			KeyHash:   keyHash,
			Key:       key,
			FieldHash: fieldHash,
			Field:     field,
			Value:     []byte(value),
			CreatedAt: time.Now(),
		}

		if err := w.db.Save(&record).Error; err != nil {
			w.panicF("failed to save hash field %s for key %s: %v", field, key, err)
		}

		w.debugF("Saved hash field: key=%s, field=%s, value=%s", key, field, value)
	}

	return nil
}

// 处理 ZSet 类型 (RDB_TYPE_ZSET, RDB_TYPE_ZSET_2)
func (w *GormEntryWriterDemo) handleZSetType(key, keyHash, data string) error {
	w.debugF("Handling ZSet for key %s with data length: %d, data-hex: %x", key, len(data), data)

	if len(data) == 0 {
		w.warnF("Empty zset data for key %s", key)
		return nil
	}

	pos := 0

	// 首先读取有序集合的大小
	zsetSize, bytesRead := w.decodeRDBLength(data, pos)
	pos += bytesRead

	w.debugF("ZSet size: %d entries for key %s", zsetSize, key)

	// 解析每个 member-score 对
	for i := 0; i < int(zsetSize); i++ {
		// 解码 member
		memberValue := w.decodeRDBString(data[pos:])
		member := string(memberValue)

		// 计算member消耗的字节数以移动位置
		memberBytesUsed := w.calculateRDBStringLength(data[pos:])
		pos += memberBytesUsed

		// 解码 score (8字节双精度浮点数, little-endian)
		if pos+8 > len(data) {
			w.panicF("insufficient data for zset score %d for key %s", i, key)
		}

		// 从8个字节构造double（IEEE 754格式，little-endian）
		scoreBytes := make([]byte, 8)
		for j := 0; j < 8; j++ {
			scoreBytes[j] = byte(data[pos+j])
		}
		pos += 8

		// 将字节转换为float64
		// Go的math.Float64frombits期望big-endian，所以需要转换
		var scoreBits uint64
		for j := 0; j < 8; j++ {
			scoreBits |= uint64(scoreBytes[j]) << (uint64(j) * 8)
		}

		score := math.Float64frombits(scoreBits)

		// 保存到数据库
		memberHash := w.calculateHash(member)
		record := RedisZSet{
			KeyHash:    keyHash,
			Key:        key,
			MemberHash: memberHash,
			Member:     member,
			Score:      score,
			CreatedAt:  time.Now(),
		}

		if err := w.db.Save(&record).Error; err != nil {
			w.panicF("failed to save zset member %s for key %s: %v", member, key, err)
		}

		w.debugF("Saved zset member %d: key=%s, member=%s, score=%f", i, key, member, score)
	}

	w.debugF("Successfully processed %d zset entries for key %s", zsetSize, key)
	return nil
}

// parseListpack 解析 Listpack 格式的数据
func (w *GormEntryWriterDemo) parseListpack(data string) []string {
	if len(data) < 7 { // 至少需要 header(6字节) + 结束符(1字节)
		w.panicF("listpack data too short: %d bytes", len(data))
	}

	// 尝试从不同位置开始解析，因为可能有前缀字节
	for offset := 0; offset <= 2 && offset < len(data)-6; offset++ {
		pos := offset

		// 读取 Listpack 头部
		totalBytes := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16 | int(data[pos+3])<<24
		pos += 4
		size := int(data[pos]) | int(data[pos+1])<<8
		pos += 2

		// 检查这个解析是否合理
		if totalBytes > 1000 || size > 100 || totalBytes < 6 {
			continue // 尝试下一个offset
		}

		w.debugF("Parsing Listpack at offset %d: totalBytes=%d, size=%d", offset, totalBytes, size)

		var elements []string

		// 读取每个元素
		for i := 0; i < size && pos < len(data)-1; i++ {
			if pos >= len(data) {
				w.warnF("Reached end of data while parsing listpack entry %d at position %d", i, pos)
				break
			}

			element, nextPos := w.parseListpackEntry(data, pos)

			if nextPos <= pos {
				w.warnF("Next position %d is not greater than current position %d for entry %d", nextPos, pos, i)
				break
			}

			elements = append(elements, element)
			pos = nextPos

			w.debugF("Parsed listpack entry %d: %s at position %d", i, element, pos)
		}

		// 如果解析到了预期数量的元素，就认为成功
		if len(elements) == size {
			// 验证结束标记（可选，因为可能没有）
			if pos < len(data) && data[pos] != 0xFF {
				w.warnF("Listpack did not end with expected 0xFF byte at position %d", pos)
			} else {
				w.debugF("Successfully parsed Listpack with %d elements at offset %d", len(elements), offset)
			}
			return elements
		}
	}

	w.panicF("failed to parse listpack with any offset")
	return nil // This line will never be reached due to panic
}

// parseListpackEntry 解析单个 Listpack 条目
func (w *GormEntryWriterDemo) parseListpackEntry(data string, pos int) (string, int) {
	if pos >= len(data) {
		w.panicF("position %d exceeds data length %d", pos, len(data))
	}

	firstByte := data[pos]
	pos++

	w.debugF("Parsing Listpack entry at position %d with first byte: 0x%02X", pos-1, firstByte)

	// 按照 Redis Listpack 编码规范的优先级顺序解析
	if (firstByte & 0x80) == 0x00 { // 7位无符号整数: 0xxxxxxx
		value := int64(firstByte & 0x7F)
		entryLen := 1
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 7-bit uint: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else if (firstByte & 0xC0) == 0x80 { // 6位字符串长度: 10xxxxxx
		length := int(firstByte & 0x3F)
		if pos+length > len(data) {
			w.panicF("6-bit string length %d exceeds remaining data at position %d (data length: %d)", length, pos, len(data))
		}
		value := data[pos : pos+length]
		pos += length

		entryLen := 1 + length
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 6-bit string (len=%d): %s at position %d", length, value, pos)
		return value, pos

	} else if (firstByte & 0xE0) == 0xC0 { // 13位有符号整数: 110xxxxx
		if pos >= len(data) {
			w.panicF("13-bit int missing second byte")
		}
		secondByte := data[pos]
		pos++

		value := int64(firstByte&0x1F)<<8 | int64(secondByte)
		// 处理负数 (13位补码)
		if value >= (1 << 12) {
			value = value - (1 << 13)
		}

		entryLen := 2
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 13-bit int: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else if (firstByte & 0xF0) == 0xE0 { // 12位字符串长度: 1110xxxx
		if pos >= len(data) {
			w.panicF("12-bit string missing second byte")
		}
		secondByte := data[pos]
		pos++

		length := (int(firstByte&0x0F) << 8) | int(secondByte)
		if pos+length > len(data) {
			w.panicF("12-bit string length %d exceeds remaining data", length)
		}
		value := data[pos : pos+length]
		pos += length

		entryLen := 2 + length
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 12-bit string (len=%d): %s at position %d", length, value, pos)
		return value, pos

	} else if firstByte == 0xF0 { // 32位字符串长度
		if pos+4 > len(data) {
			w.panicF("32-bit string missing length bytes")
		}
		length := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16 | int(data[pos+3])<<24
		pos += 4
		if pos+length > len(data) {
			w.panicF("32-bit string length %d exceeds remaining data", length)
		}
		value := data[pos : pos+length]
		pos += length

		entryLen := 5 + length
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 32-bit string (len=%d): %s at position %d", length, value, pos)
		return value, pos

	} else if firstByte == 0xF1 { // 16位有符号整数
		if pos+2 > len(data) {
			w.panicF("16-bit int missing bytes")
		}
		value := int64(data[pos]) | int64(data[pos+1])<<8
		pos += 2
		// 处理负数 (16位补码)
		if value >= (1 << 15) {
			value = value - (1 << 16)
		}

		entryLen := 3
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 16-bit int: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else if firstByte == 0xF2 { // 24位有符号整数
		if pos+3 > len(data) {
			w.panicF("24-bit int missing bytes")
		}
		value := int64(data[pos]) | int64(data[pos+1])<<8 | int64(data[pos+2])<<16
		pos += 3
		// 处理负数 (24位补码)
		if value >= (1 << 23) {
			value = value - (1 << 24)
		}

		entryLen := 4
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 24-bit int: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else if firstByte == 0xF3 { // 32位有符号整数
		if pos+4 > len(data) {
			w.panicF("32-bit int missing bytes")
		}
		value := int64(data[pos]) | int64(data[pos+1])<<8 | int64(data[pos+2])<<16 | int64(data[pos+3])<<24
		pos += 4
		// 处理负数 (32位补码)
		if value >= (1 << 31) {
			value = value - (1 << 32)
		}

		entryLen := 5
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 32-bit int: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else if firstByte == 0xF4 { // 64位有符号整数
		if pos+8 > len(data) {
			w.panicF("64-bit int missing bytes")
		}
		value := int64(data[pos]) | int64(data[pos+1])<<8 | int64(data[pos+2])<<16 | int64(data[pos+3])<<24 |
			int64(data[pos+4])<<32 | int64(data[pos+5])<<40 | int64(data[pos+6])<<48 | int64(data[pos+7])<<56
		pos += 8

		entryLen := 9
		backLengthBytes := w.skipBackLength(data, pos, entryLen)
		if pos+backLengthBytes <= len(data) {
			pos += backLengthBytes
		}
		w.debugF("Parsed 64-bit int: %d at position %d", value, pos)
		return strconv.FormatInt(value, 10), pos

	} else {
		w.panicF("unknown listpack encoding: 0x%x at position %d", firstByte, pos-1)
		return "", pos // This line will never be reached due to panic
	}
}

// skipBackLength 跳过 Listpack 条目的后向长度字段
func (w *GormEntryWriterDemo) skipBackLength(data string, pos int, entryLen int) int {
	// Listpack 后向长度编码规则：
	// 1-127 字节：1字节编码
	// 128-16383 字节：2字节编码
	// 16384-2097151 字节：3字节编码
	// 2097152-268435455 字节：4字节编码
	// >268435455 字节：5字节编码

	// 但是需要检查实际的后向长度字节内容来确定
	if pos >= len(data) {
		return 0
	}

	backLengthByte := data[pos]

	// 第一字节的最高位决定了后向长度字段的字节数
	if (backLengthByte & 0x80) == 0 { // 0xxxxxxx - 1字节
		return 1
	} else if (backLengthByte & 0xC0) == 0x80 { // 10xxxxxx - 2字节
		return 2
	} else if (backLengthByte & 0xE0) == 0xC0 { // 110xxxxx - 3字节
		return 3
	} else if (backLengthByte & 0xF0) == 0xE0 { // 1110xxxx - 4字节
		return 4
	} else { // 11110xxx - 5字节
		return 5
	}
}
