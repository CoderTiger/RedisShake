package main

import (
	"RedisShake/common"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

type GormEntryWriterDemo struct {
	ctx context.Context
	db  *gorm.DB
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

func NewGormEntryWriter() common.GormEntryWriter {
	return &GormEntryWriterDemo{
		db: nil, // This will be initialized later
	}
}

//go:embed assets/sql/*.sql
var sqlFS embed.FS

var sqlFileNames = []string{
	"redis_string.sql",
	"redis_hash.sql",
	"redis_list.sql",
	"redis_set.sql",
	"redis_zset.sql"}

func (w *GormEntryWriterDemo) Init(ctx context.Context, db *gorm.DB) error {
	w.ctx = ctx
	w.db = db

	for _, fileName := range sqlFileNames {
		data, err := sqlFS.ReadFile("assets/sql/" + fileName)
		if err != nil {
			return fmt.Errorf("failed to read SQL file %s: %w", fileName, err)
		}
		if err := db.Exec(string(data)).Error; err != nil {
			return fmt.Errorf("failed to execute SQL from %s: %w", fileName, err)
		}
	}

	return nil
}

func (w *GormEntryWriterDemo) Write(e *common.Entry) error {
	fmt.Printf("Writing entry: DbId=%d, CmdName=%s, Keys=%v, Group=%s\n", e.DbId, e.CmdName, e.Keys, e.Group)
	cmd := strings.ToLower(e.CmdName)
	switch cmd {
	case "restore": // sync with scan reader
		if err := w.HandleRestoreCommand(e); err != nil {
			return err
		}
	case "ping":
		// todo: implement the logic to handle the ping command
		w.db.Logger.Info(w.ctx, "Handling ping command for entry with DbId=%d", e.DbId)
	default:
		w.db.Logger.Info(w.ctx, "Skipping unsupported command %s for entry with DbId=%d, Keys=%v", cmd, e.DbId, e.Keys)
	}

	return nil
}

func (w *GormEntryWriterDemo) Close() error {
	w.db = nil
	return nil
}

func (w *GormEntryWriterDemo) decodeRDBString(data string) (any, any) {
	if len(data) == 0 {
		return []byte{}, nil
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
			return nil, fmt.Errorf("data too short for 6-bit length encoding")
		}
		return []byte(data[1 : 1+length]), nil

	case 1: // 01xxxxxx - 14位长度
		if len(data) < 2 {
			return nil, fmt.Errorf("data too short for 14-bit length encoding")
		}
		length := int(firstByte&0x3F)<<8 | int(data[1])
		if len(data) < 2+length {
			return nil, fmt.Errorf("data too short for 14-bit length encoding")
		}
		return []byte(data[2 : 2+length]), nil

	case 2: // 10xxxxxx - 32位长度
		if len(data) < 5 {
			return nil, fmt.Errorf("data too short for 32-bit length encoding")
		}
		length := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
		if len(data) < 5+length {
			return nil, fmt.Errorf("data too short for 32-bit length encoding")
		}
		return []byte(data[5 : 5+length]), nil

	case 3: // 11xxxxxx - 特殊编码
		format := firstByte & 0x3F
		switch format {
		case 0: // 8位整数
			if len(data) < 2 {
				return nil, fmt.Errorf("data too short for 8-bit integer")
			}
			return []byte(strconv.Itoa(int(int8(data[1])))), nil
		case 1: // 16位整数
			if len(data) < 3 {
				return nil, fmt.Errorf("data too short for 16-bit integer")
			}
			val := int16(data[1]) | int16(data[2])<<8
			return []byte(strconv.Itoa(int(val))), nil
		case 2: // 32位整数
			if len(data) < 5 {
				return nil, fmt.Errorf("data too short for 32-bit integer")
			}
			val := int32(data[1]) | int32(data[2])<<8 | int32(data[3])<<16 | int32(data[4])<<24
			return []byte(strconv.Itoa(int(val))), nil
		case 3: // LZF压缩字符串
			return nil, fmt.Errorf("LZF compressed strings not supported")
		default:
			return nil, fmt.Errorf("unknown special encoding format: %d", format)
		}

	default:
		return nil, fmt.Errorf("invalid encoding: %d", encoding)
	}
}

func (w *GormEntryWriterDemo) HandleRestoreCommand(e *common.Entry) error {
	// RESTORE 命令格式: RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
	if len(e.Argv) < 4 {
		return fmt.Errorf("invalid RESTORE command, expected at least 4 arguments, got %d", len(e.Argv))
	}

	key := e.Argv[1]
	ttlStr := e.Argv[2]
	serializedValue := e.Argv[3]

	// 解析 TTL (暂时不使用，但保留用于将来扩展)
	_, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid TTL value: %s", ttlStr)
	}
	w.db.Logger.Info(w.ctx, "Handling RESTORE command for key %s with TTL %s", key, ttlStr)

	// 计算 key 的 SHA256 哈希
	keyHash := w.calculateHash(key)

	// 解析序列化的 Redis 数据
	if len(serializedValue) < 11 { // 至少需要 type(1) + data + version(2) + crc(8)
		return fmt.Errorf("invalid serialized value, too short")
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
	// case 3, 5: // RDB_TYPE_ZSET, RDB_TYPE_ZSET_2
	// 	return w.handleZSetType(key, keyHash, actualData)
	// case 4: // RDB_TYPE_HASH
	// 	return w.handleHashType(key, keyHash, actualData)
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
	// case 16: // RDB_TYPE_HASH_LISTPACK
	// 	return w.handleHashListpackType(key, keyHash, actualData)
	// case 17: // RDB_TYPE_ZSET_LISTPACK
	// 	return w.handleZSetListpackType(key, keyHash, actualData)
	// case 20: // RDB_TYPE_SET_LISTPACK
	// 	return w.handleSetListpackType(key, keyHash, actualData)
	default:
		w.db.Logger.Warn(w.ctx, "Unsupported RDB data type %d for key %s", dataType, key)
		return nil // 跳过不支持的类型，不报错
	}
}

// 计算字符串的 SHA256 哈希
func (w *GormEntryWriterDemo) calculateHash(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// 处理 String 类型
func (w *GormEntryWriterDemo) handleStringType(key, keyHash, data string) error {
	// 解码 RDB 字符串数据，移除长度编码前缀
	decodedValue, err := w.decodeRDBString(data)
	if err != nil {
		return fmt.Errorf("failed to decode RDB string for key %s: %v", key, err)
	}

	record := RedisString{
		KeyHash:   keyHash,
		Key:       key,
		Value:     decodedValue.([]byte),
		CreatedAt: time.Now(),
	}

	return w.db.Save(&record).Error
}

// 处理 Hash 类型 (简化实现，假设是原始格式)
func (w *GormEntryWriterDemo) handleHashType(key, keyHash, data string) error {
	// 这里需要根据 RDB 格式解析哈希数据
	// 简化实现：假设数据格式为 "field1\x00value1\x00field2\x00value2..."
	parts := strings.Split(data, "\x00")

	if len(parts) < 2 || len(parts)%2 != 0 {
		return fmt.Errorf("invalid hash data format for key %s", key)
	}

	for i := 0; i < len(parts)-1; i += 2 {
		field := parts[i]
		value := parts[i+1]
		decodedValue, err := w.decodeRDBString(value)
		if err != nil {
			return fmt.Errorf("failed to decode RDB string for field %s in key %s: %v", field, key, err)
		}

		fieldHash := w.calculateHash(field)
		record := RedisHash{
			KeyHash:   keyHash,
			Key:       key,
			FieldHash: fieldHash,
			// Field:     base64.StdEncoding.EncodeToString([]byte(field)),
			// Value:     base64.StdEncoding.EncodeToString([]byte(value)),
			Field:     field,
			Value:     decodedValue.([]byte),
			CreatedAt: time.Now(),
		}

		if err := w.db.Create(&record).Error; err != nil {
			return err
		}
	}
	return nil
}

// 处理 List 类型 (简化实现)
func (w *GormEntryWriterDemo) handleListType(key, keyHash, data string) error {
	// 简化实现：假设数据格式为 "value1\x00value2\x00value3..."
	values := strings.Split(data, "\x00")

	for i, value := range values {
		if value == "" {
			continue
		}

		decodedValue, err := w.decodeRDBString(value)
		if err != nil {
			return fmt.Errorf("failed to decode RDB string for value %s in key %s: %v", value, key, err)
		}

		record := RedisList{
			KeyHash:   keyHash,
			Key:       key,
			Position:  uint(i),
			Value:     decodedValue.([]byte),
			CreatedAt: time.Now(),
		}

		if err := w.db.Create(&record).Error; err != nil {
			return err
		}
	}

	return nil
}

// 处理 Set 类型 (简化实现)
func (w *GormEntryWriterDemo) handleSetType(key, keyHash, data string) error {
	// 简化实现：假设数据格式为 "member1\x00member2\x00member3..."
	members := strings.Split(data, "\x00")

	for _, member := range members {
		if member == "" {
			continue
		}

		memberHash := w.calculateHash(member)
		record := RedisSet{
			KeyHash:    keyHash,
			Key:        key,
			MemberHash: memberHash,
			Member:     member,
			CreatedAt:  time.Now(),
		}

		if err := w.db.Create(&record).Error; err != nil {
			return err
		}
	}

	return nil
}

// 处理 ZSet 类型 (简化实现)
func (w *GormEntryWriterDemo) handleZSetType(key, keyHash, data string) error {
	// 简化实现：假设数据格式为 "member1\x00score1\x00member2\x00score2..."
	parts := strings.Split(data, "\x00")

	for i := 0; i < len(parts)-1; i += 2 {
		member := parts[i]
		scoreStr := parts[i+1]

		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			w.db.Logger.Warn(w.ctx, "Invalid score for ZSet member %s: %s", member, scoreStr)
			continue
		}
		memberHash := w.calculateHash(member)
		record := RedisZSet{
			KeyHash:    keyHash,
			Key:        key,
			MemberHash: memberHash,
			Member:     member,
			Score:      score,
			CreatedAt:  time.Now(),
		}

		if err := w.db.Create(&record).Error; err != nil {
			return err
		}
	}

	return nil
}

// 以下是各种编码格式的处理方法，这里提供简化实现
// 实际项目中需要根据 Redis RDB 格式规范来正确解析

func (w *GormEntryWriterDemo) handleListZiplistType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 List 处理
	return w.handleListType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleSetIntsetType(key, keyHash, data string) error {
	// 简化实现，解析整数集合
	// 这里需要根据 intset 格式解析，暂时用简化方式
	return w.handleSetType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleZSetZiplistType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 ZSet 处理
	return w.handleZSetType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleHashZiplistType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 Hash 处理
	return w.handleHashType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleListQuicklistType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 List 处理
	return w.handleListType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleHashListpackType(key, keyHash, data string) error {
	fmt.Printf("Handling Hash Listpack for key %s with data: %s, data-hex: %x\n", key, data, data)
	// 简化实现，直接当作普通 Hash 处理
	return w.handleHashType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleZSetListpackType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 ZSet 处理
	return w.handleZSetType(key, keyHash, data)
}

func (w *GormEntryWriterDemo) handleSetListpackType(key, keyHash, data string) error {
	// 简化实现，直接当作普通 Set 处理
	return w.handleSetType(key, keyHash, data)
}
