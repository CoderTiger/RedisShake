package main

import (
	"RedisShake/common"
	"context"
	"embed"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type GormEntryWriterDemo struct {
	ctx context.Context
	db  *gorm.DB
}

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
	// fmt.Printf("Writing entry: DbId=%d, CmdName=%s, Keys=%v, Group=%s\n", e.DbId, e.CmdName, e.Keys, e.Group)
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

func (w *GormEntryWriterDemo) HandleRestoreCommand(e *common.Entry) error {
	// todo: implement the logic to handle the restore command
	w.db.Logger.Info(w.ctx, "Handling restore command for entry with DbId=%d, Keys=%v", e.DbId, e.Keys)
	return nil
}
