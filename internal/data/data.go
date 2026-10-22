package data

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"order/internal/conf"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	NewOrderRepo,
	NewDTMFulfiller,
	NewExpirationWorker,
	NewProductServiceClient,
	NewInventoryServiceClient,
	NewWalletServiceClient,
)

// Data .
type Data struct {
	db    *gorm.DB
	redis redis.UniversalClient
	log   *log.Helper
}

// Step 0.1: 初始化 Order 持久层 Data 容器与连接池
func NewData(c *conf.Data, logger log.Logger) (*Data, func(), error) {
	helper := log.NewHelper(logger)

	db, err := gorm.Open(mysql.Open(c.Database.Source), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		helper.Errorf("failed opening connection to mysql: %v", err)
		return nil, nil, fmt.Errorf("connect mysql: %w", err)
	}

	var rdb redis.UniversalClient
	if c.Redis != nil && c.Redis.Addr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:     c.Redis.Addr,
			Password: c.Redis.Password,
			DB:       int(c.Redis.Db),
		})
	}

	d := &Data{
		db:    db,
		redis: rdb,
		log:   helper,
	}

	cleanup := func() {
		helper.Info("closing order data resources")
		if sqlDB, err := d.db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		if d.redis != nil {
			_ = d.redis.Close()
		}
	}

	return d, cleanup, nil
}
