package redis

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"gopkg.in/redis.v5"
)

var (
	_ Configurator = (*Config)(nil)
)

type Configurator interface {
	GetAddr() string
	GetPassword() string
	GetPoolSize() int
	GetDialTimeout() time.Duration
	GetReadTimeout() time.Duration
	GetWriteTimeout() time.Duration
	GetPoolTimeout() time.Duration
}

type Config struct {
	Addr         string        `yaml:"addr" json:"addr"`
	Password     string        `yaml:"password" json:"password"`
	PoolSize     int           `yaml:"pool_size" json:"pool_size"`
	DialTimeout  time.Duration `yaml:"dial_timeout" json:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`
	PoolTimeout  time.Duration `yaml:"pool_timeout" json:"pool_timeout"`
}

func (c *Config) GetAddr() string {
	return c.Addr
}

func (c *Config) GetPassword() string {
	return c.Password
}

func (c *Config) GetPoolSize() int {
	return c.PoolSize
}

func (c *Config) GetDialTimeout() time.Duration {
	return c.DialTimeout
}

func (c *Config) GetReadTimeout() time.Duration {
	return c.ReadTimeout
}

func (c *Config) GetWriteTimeout() time.Duration {
	return c.WriteTimeout
}

func (c *Config) GetPoolTimeout() time.Duration {
	return c.PoolTimeout
}

//------------------------------------------------------------------------------

func initRedisNormal(cfg Configurator) (redis.Cmdable, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.GetAddr(),
		Password:     cfg.GetPassword(),
		PoolSize:     cfg.GetPoolSize(),
		DialTimeout:  cfg.GetDialTimeout(),
		ReadTimeout:  cfg.GetReadTimeout(),
		WriteTimeout: cfg.GetWriteTimeout(),
		PoolTimeout:  cfg.GetPoolTimeout(),
	})
	_, err := client.Ping().Result()
	return client, err
}

func initRedisCluster(cfg Configurator) (redis.Cmdable, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        strings.Split(cfg.GetAddr(), ","),
		Password:     cfg.GetPassword(),
		PoolSize:     cfg.GetPoolSize(),
		DialTimeout:  cfg.GetDialTimeout(),
		ReadTimeout:  cfg.GetReadTimeout(),
		WriteTimeout: cfg.GetWriteTimeout(),
		PoolTimeout:  cfg.GetPoolTimeout(),
	})
	_, err := client.Ping().Result()
	return client, err
}

// 创建redis实例，集群模式地址使用英文逗号分隔，例:"{IP:PORT},{IP:PORT}"
func NewRedis(cfg Configurator) (redis.Cmdable, error) {
	if cfg == nil || reflect.ValueOf(cfg).IsNil() {
		return nil, errors.New("nil config")
	}
	if len(strings.TrimSpace(cfg.GetAddr())) == 0 {
		return nil, errors.New("empty addr")
	}
	if strings.Contains(cfg.GetAddr(), ",") {
		return initRedisCluster(cfg)
	}
	return initRedisNormal(cfg)
}

func Nil(err error) bool {
	return err == redis.Nil
}
