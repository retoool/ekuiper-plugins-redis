package main

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	redis "github.com/redis/go-redis/v9"
)

type redisSubConfig struct {
	Address  string   `json:"address"`
	Db       int      `json:"db"`
	Password string   `json:"password"`
	Channels []string `json:"channels"`
}

type redisSub struct {
	conf *redisSubConfig
	conn *redis.Client
}

func (s *redisSub) Configure(datasource string, props map[string]interface{}) error {
	cfg := &redisSubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	s.conf = cfg
	s.conn = redis.NewClient(&redis.Options{
		Addr:     s.conf.Address,
		Password: s.conf.Password,
		DB:       s.conf.Db,
	})
	// Create a context
	ctx := context.Background()

	// Ping Redis to check if the connection is alive
	pong, err := s.conn.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	fmt.Printf("Redis Ping response: %s\n", pong)

	return nil
}
func (s *redisSub) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	defer func() {
		if r := recover(); r != nil {
			errCh <- fmt.Errorf("recovered from panic: %v", r)
		}
		close(consumer)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			sub := s.conn.PSubscribe(ctx, s.conf.Channels...)
			defer s.conn.Close()
			defer sub.Close()
			for {
				// Subscribe Data
				msg, err := sub.ReceiveMessage(ctx)
				if err != nil {
					logger.Errorf("Error receiving message from Redis: %v", err)
					return
				}
				// Decompress Data
				data, err := DecompressData(cast.StringToBytes(msg.Payload))
				if err != nil {
					logger.Errorf("Error decompressing data: %v", err)
					continue
				}
				// Decode Data
				rm := RedisSourceFormat{}
				decodeDatas, err := rm.Decode(data)
				if err != nil {
					logger.Errorf("Error decoding data: %v", err)
					continue
				}
				// Send Data
				for _, decodeData := range decodeDatas {
					consumer <- api.NewDefaultSourceTuple(decodeData, nil)
				}
			}
		}
	}
}

func (s *redisSub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redis sink")
	return nil
}

func RedisSub() api.Source {
	return &redisSub{}
}
