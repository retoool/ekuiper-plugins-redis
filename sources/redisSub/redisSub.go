package main

import (
	"context"
	"ekuiper-plugins-redis/pkg/compressor"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	redis "github.com/redis/go-redis/v9"
)

type redisSub struct {
	conf         *redisSubConfig
	conn         *redis.Client
	decompressor message.Decompressor
}

type redisSubConfig struct {
	Address       string   `json:"address"`
	Db            int      `json:"db"`
	Password      string   `json:"password"`
	Channels      []string `json:"channels"`
	Decompression string   `json:"decompression"`
}

func (r *redisSub) Configure(topic string, props map[string]interface{}) error {
	cfg := &redisSubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	r.conf = cfg
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})

	if cfg.Decompression != "" {
		dc, err := compressor.GetDecompressor(cfg.Decompression)
		if err != nil {
			return fmt.Errorf("get decompressor %r fail with error: %v", cfg.Decompression, err)
		}
		r.decompressor = dc
	}

	// Create a context
	ctx := context.Background()

	// Ping Redis to check if the connection is alive
	err = r.conn.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}
func (r *redisSub) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
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
			sub := r.conn.PSubscribe(ctx, r.conf.Channels...)
			defer r.conn.Close()
			defer sub.Close()
			for {
				// Subscribe
				msg, err := sub.ReceiveMessage(ctx)
				if err != nil {
					logger.Errorf("Error receiving message from Redis: %v", err)
					return
				}
				payload := cast.StringToBytes(msg.Payload)
				// Decompress
				if r.decompressor != nil {
					payload, err = r.decompressor.Decompress(payload)
					if err != nil {
						logger.Errorf("can not decompress redis message %v.", err)
					}
				}
				// Decode
				decodeDatas, err := ctx.DecodeIntoList(payload)
				if err != nil {
					logger.Errorf("Invalid data format, cannot decode %s with error %s", string(payload), err)
					continue
				}
				// Send
				for _, item := range decodeDatas {
					consumer <- api.NewDefaultSourceTuple(item, nil)
				}
			}
		}
	}
}

func (r *redisSub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisSub source")
	return nil
}

func RedisSub() api.Source {
	return &redisSub{}
}
