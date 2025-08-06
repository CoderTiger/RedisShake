package main

import (
	"RedisShake/common"
	"embed"
	"fmt"

	"gorm.io/gorm"
)

type GormEntryWriterDemo struct {
	db *gorm.DB
}

func NewGormEntryWriter() common.GormEntryWriter {
	return &GormEntryWriterDemo{
		db: nil, // This will be initialized later
	}
}

//go:embed assets/sql/*.sql
var sqlFS embed.FS

var sqlFileNames = []string{"redis_string.sql",
	"redis_hash.sql",
	"redis_list.sql",
	"redis_set.sql",
	"redis_zset.sql"}

func (w *GormEntryWriterDemo) Init(db *gorm.DB) error {
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
	// todo: Write the entry to the database using GORM
	fmt.Printf("Writing entry to database: DbId=%d, CmdName=%s, Keys=%v, Group=%s\n", e.DbId, e.CmdName, e.Keys, e.Group)
	return nil
}

func (w *GormEntryWriterDemo) Close() error {
	w.db = nil
	return nil
}
