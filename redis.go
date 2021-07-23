package redis

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/redis.v5"
)

const (
	defaultAlias = "default"
)

type Config interface {
	GetAddr() string
	GetPassword() string
	GetPoolSize() int
	GetDialTimeout() time.Duration
	GetReadTimeout() time.Duration
	GetWriteTimeout() time.Duration
	GetPoolTimeout() time.Duration
}

var clients = make(map[string]redis.Cmdable)

func initRedisNormal(cfg Config) (redis.Cmdable, error) {
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

func initRedisCluster(cfg Config) (redis.Cmdable, error) {
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

func initRedis(cfg Config) (redis.Cmdable, error) {
	if cfg == nil {
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

func resolveAlias(aliases ...string) string {
	if len(aliases) > 0 {
		return aliases[0]
	}
	return defaultAlias
}

func Register(cfg Config, aliases ...string) (redis.Cmdable, error) {
	client, err := initRedis(cfg)
	if err != nil {
		return nil, err
	}
	clients[resolveAlias(aliases...)] = client
	return client, nil
}

func Client(aliases ...string) redis.Cmdable {
	alias := resolveAlias(aliases...)
	if client, ok := clients[alias]; ok {
		return client
	}
	panic(fmt.Sprintf("redis client [%s] unregistered", alias))
}

func Nil(err error) bool {
	return err == redis.Nil
}
