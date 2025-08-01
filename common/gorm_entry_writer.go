package common

import (
	"RedisShake/internal/entry"

	"gorm.io/gorm"
)

type GormEntryWriter interface {
	Init(db *gorm.DB) error
	Write(e *entry.Entry) error
	Close() error
}
