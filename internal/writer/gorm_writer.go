package writer

import (
	"RedisShake/common"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"context"
	"fmt"
	"plugin"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	// "gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	_ "github.com/joho/godotenv/autoload"
)

type GormWriterOptions struct {
	Host   string `mapstructure:"host" default:"localhost"`
	Port   int    `mapstructure:"port" default:"4000"`
	User   string `mapstructure:"user" default:"root"`
	Pass   string `mapstructure:"pass" default:""`
	Db     string `mapstructure:"db" default:"redis_shake"`
	UseSSL bool   `mapstructure:"use_ssl" default:"false"`
	// Connection pool settings
	MaxIdleConns    int `mapstructure:"max_idle_conns" default:"10"`
	MaxOpenConns    int `mapstructure:"max_open_conns" default:"100"`
	ConnMaxLifetime int `mapstructure:"conn_max_lifetime" default:"-1"` // no limit
	// Entry writer plugins: array of plugin configurations
	Plugins []map[string]string `mapstructure:"plugins" default:"[]"`

	LogLevel string `mapstructure:"log_level" default:"info"` // all options: silent, error, warn, info
}

// implements Writer interface
type gormWriter struct {
	db   *gorm.DB
	DbId int
	ch   chan *entry.Entry
	chWg sync.WaitGroup
	stat struct {
		EntryCount int `json:"entry_count"`
	}

	plugins []*common.GormEntryWriter
}

func getDSN(opts *GormWriterOptions) string {
	var tlsValue string
	if opts.UseSSL {
		tlsValue = "true"
	} else {
		tlsValue = "false"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&tls=%s",
		opts.User, opts.Pass, opts.Host, opts.Port, opts.Db, tlsValue)
	return dsn
}

func createDB(opts *GormWriterOptions) *gorm.DB {
	// Create GORM DB instance
	db, err := gorm.Open(mysql.Open(getDSN(opts)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic(err)
	}

	logLevel := strings.ToLower(opts.LogLevel)
	switch logLevel {
	case "silent":
		db.Logger = logger.Default.LogMode(logger.Silent)
	case "error":
		db.Logger = logger.Default.LogMode(logger.Error)
	case "warn":
		db.Logger = logger.Default.LogMode(logger.Warn)
	case "info":
		db.Logger = logger.Default.LogMode(logger.Info)
	default:
		log.Panicf("Invalid log level: %s. Supported values are: silent, error, warn, info", opts.LogLevel)
	}

	// Set connection pool options
	sqlDB, err := db.DB()
	if err != nil {
		log.Panicf("failed to get sql.DB: %v", err)
	}
	sqlDB.SetMaxIdleConns(opts.MaxIdleConns)
	sqlDB.SetMaxOpenConns(opts.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnMaxLifetime) * time.Second)

	return db
}

func loadPlugins(opts *GormWriterOptions, db *gorm.DB) ([]*common.GormEntryWriter, error) {
	var plugins []*common.GormEntryWriter
	for _, pluginMap := range opts.Plugins {
		// Each pluginMap should have exactly one key-value pair: plugin_path -> config_json
		if len(pluginMap) != 1 {
			log.Panicf("each plugin configuration must have exactly one key-value pair, got %d pairs", len(pluginMap))
		}

		var pluginPath, pluginConfig string
		for path, config := range pluginMap {
			pluginPath = strings.TrimSpace(path)
			pluginConfig = strings.TrimSpace(config)
			break // Only one iteration since we expect exactly one pair
		}

		if pluginPath == "" {
			continue // skip empty plugin paths
		}

		log.Infof("Loading plugin: %s with config: %s", pluginPath, pluginConfig)

		pluginObj, err := plugin.Open(pluginPath)
		if err != nil {
			log.Panicf("failed to open plugin %s: %v", pluginPath, err)
		}

		newWriterFuncSym, err := pluginObj.Lookup("NewWriter")
		if err != nil {
			log.Panicf("plugin %s does not export 'NewWriter' symbol: %v", pluginPath, err)
		}

		newWriterFunc, ok := newWriterFuncSym.(func(config string, db *gorm.DB, infoF, warnF, errorF, panicF func(msg string, args ...interface{})) common.GormEntryWriter)
		if !ok {
			log.Panicf("plugin %s does not export 'NewWriter' function with correct signature", pluginPath)
		}

		p := newWriterFunc(pluginConfig, db, log.Debugf, log.Infof, log.Warnf, log.Panicf)
		if p == nil {
			log.Panicf("plugin %s returned nil writer", pluginPath)
		}

		if err := p.Init(); err != nil {
			log.Panicf("plugin %s failed to initialize: %v", pluginPath, err)
		}

		log.Infof("Loaded plugin: %s", pluginPath)
		plugins = append(plugins, &p)
	}
	return plugins, nil
}

func NewGormWriter(ctx context.Context, opts *GormWriterOptions) Writer {
	if opts.Host == "" || opts.Port <= 0 || opts.User == "" || opts.Db == "" {
		log.Panicf("Invalid GormWriter options: %+v", opts)
	}
	if opts.MaxIdleConns <= 0 || opts.MaxOpenConns <= 0 {
		log.Panicf("Invalid connection pool settings: max_idle_conns=%d, max_open_conns=%d",
			opts.MaxIdleConns, opts.MaxOpenConns)
	}
	w := &gormWriter{}
	w.db = createDB(opts)
	plugins, err := loadPlugins(opts, w.db)
	if err != nil {
		log.Panicf("%v", err)
	}
	w.plugins = plugins
	w.DbId = 0
	w.ch = make(chan *entry.Entry, 1000)
	w.chWg.Add(1)
	w.stat.EntryCount = 0
	return w
}

func (w *gormWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *gormWriter) StartWrite(ctx context.Context) (ch chan *entry.Entry) {
	w.chWg = sync.WaitGroup{}
	w.chWg.Add(1)
	go w.processWrite(ctx)
	return w.ch
}

func (w *gormWriter) Close() {
	close(w.ch)
	w.chWg.Wait()
	log.Infof("GormWriter closed, total entries written: %d", w.stat.EntryCount)
}

func (w *gormWriter) Status() interface{} {
	return w.stat
}

func (w *gormWriter) StatusString() string {
	return "[gorm_writer] writing to database, entry count=" + fmt.Sprint(w.stat.EntryCount)
}

func (w *gormWriter) StatusConsistent() bool {
	return true
}

func (w *gormWriter) writeEntry(_ *gorm.DB, e *entry.Entry) {
	commonEntry := &common.Entry{
		DbId:           e.DbId,
		Argv:           e.Argv,
		CmdName:        e.CmdName,
		Group:          e.Group,
		Keys:           e.Keys,
		KeyIndexes:     e.KeyIndexes,
		Slots:          e.Slots,
		SerializedSize: e.SerializedSize,
	}

	for _, plugin := range w.plugins {
		if err := (*plugin).Write(commonEntry); err != nil {
			log.Warnf("failed to write entry using plugin: %v", err)
			continue
		}
	}
}

func (w *gormWriter) Flush() error {
	// do nothing for now, TODO: investigate how to flush with GORM
	return nil
}

func (w *gormWriter) processWrite(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// do nothing until w.ch is closed
		case <-ticker.C:
			w.Flush()
		case e, ok := <-w.ch:
			if !ok {
				// clean up and exit
				w.chWg.Done()
				w.Flush()
				return
			}
			w.stat.EntryCount++
			w.writeEntry(w.db, e)
		}
	}
}
