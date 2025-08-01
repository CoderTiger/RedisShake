package main

import (
	"RedisShake/common"
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

func (w *GormEntryWriterDemo) Init(db *gorm.DB) error {
	w.db = db
	return nil
}

func (w *GormEntryWriterDemo) Write(e *common.Entry) error {
	// Write the entry to the database using GORM
	// Example: Save to database table
	// return w.db.Create(e).Error
	fmt.Println("Writing entry to database:", e)
	return nil
}

func (w *GormEntryWriterDemo) Close() error {
	w.db = nil
	return nil
}
