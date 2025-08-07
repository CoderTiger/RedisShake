package common

import (
	"context"

	"gorm.io/gorm"
)

// Entry represents a Redis entry for external use
type Entry struct {
	DbId           int
	Argv           []string
	CmdName        string
	Group          string
	Keys           []string
	KeyIndexes     []int
	Slots          []int
	SerializedSize int64
}

type GormEntryWriter interface {
	Init(ctx context.Context, db *gorm.DB) error
	Write(e *Entry) error
	Close() error
}
