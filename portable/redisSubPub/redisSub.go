package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	redis "github.com/redis/go-redis/v9"
	"time"
)

type redisSubConfig struct {
	Address  string   `json:"address"`
	Db       int      `json:"db"`
	Pass     string   `json:"pass"`
	Channels []string `json:"channels"`
}

type redisSub struct {
	conf *redisSubConfig
	conn *redis.Client
}

func (s *redisSub) Configure(topic string, props map[string]interface{}) error {
	cfg := &redisSubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	s.conf = cfg
	s.conn = redis.NewClient(&redis.Options{
		Addr:     s.conf.Address,
		Password: s.conf.Pass,
		DB:       s.conf.Db,
	})
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
		// Create a new Redis connection in each iteration
		s.conn = redis.NewClient(&redis.Options{
			Addr:     s.conf.Address,
			Password: s.conf.Pass,
			DB:       s.conf.Db,
		})

		pubsub := s.conn.PSubscribe(ctx, s.conf.Channels...)
		defer pubsub.Close()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				// Handle the error, and attempt to reconnect
				logger.Errorf("Error receiving message from Redis: %v", err)
				break // Break this inner loop to reconnect
			}
			data, err := DecompressData(cast.StringToBytes(msg.Payload))
			if err != nil {
				logger.Errorf("Error decompressing data: %v", err)
				continue
			}
			rm := RedisSourceFormat{}
			decodeDatas, _ := rm.Decode(ctx, data)
			if err != nil {
				logger.Errorf("Error decoding data: %v", err)
				continue
			}
			for _, decodeData := range decodeDatas {
				consumer <- api.NewDefaultSourceTuple(decodeData, nil)
			}
			select {
			case <-ctx.Done():
				logger.Info("redisSub source done")
				return
			default:
				// do nothing
			}
		}

		// Sleep before attempting to reconnect
		time.Sleep(5 * time.Second)
		logger.Info("Reconnecting to Redis...")
	}
}

func (s *redisSub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redis sink")
	err := s.conn.Close()
	return err
}

func RedisSub() api.Source {
	return &redisSub{}
}
