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

type redisPub struct {
	conf       *redisPubConfig
	conn       *redis.Client
	compressor message.Compressor
}

type redisPubConfig struct {
	Address       string `json:"address"`
	Db            int    `json:"db"`
	Password      string `json:"password"`
	Channel       string `json:"channel"`
	Compression   string `json:"compression"`
	ResendChannel string `json:"resendDestination"`
}

func (r *redisPub) Configure(props map[string]interface{}) error {
	cfg := &redisPubConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Channel == "" {
		return fmt.Errorf("redisPub sink is missing property channel")
	}
	if cfg.Compression != "" {
		var err error
		r.compressor, err = compressor.GetCompressor(cfg.Compression)
		if err != nil {
			return fmt.Errorf("invalid compression method %r", cfg.Compression)
		}
	}
	if cfg.ResendChannel == "" {
		cfg.ResendChannel = cfg.Channel
	}
	r.conf = cfg

	return nil
}

func (r *redisPub) Open(ctx api.StreamContext) error {
	r.conn = redis.NewClient(&redis.Options{
		Addr:     r.conf.Address,
		Password: r.conf.Password,
		DB:       r.conf.Db,
	})

	// Ping Redis to check if the connection is alive
	err := r.conn.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("Ping Redis failed with error: %v", err)
	}
	return nil
}
func (r *redisPub) Collect(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.Channel)
}

func (r *redisPub) CollectResend(ctx api.StreamContext, item interface{}) error {
	return r.collectWithChannel(ctx, item, r.conf.ResendChannel)
}

func (r *redisPub) collectWithChannel(ctx api.StreamContext, item interface{}, channel string) error {
	logger := ctx.GetLogger()
	// Transform
	jsonBytes, _, err := ctx.TransformOutput(item)
	if err != nil {
		return err
	}
	logger.Debugf("%s publish %s", ctx.GetOpId(), jsonBytes)
	// Compress
	if r.compressor != nil {
		jsonBytes, err = r.compressor.Compress(jsonBytes)
		if err != nil {
			return err
		}
	}
	// Publish
	err = r.conn.Publish(context.TODO(), channel, jsonBytes).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *redisPub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisPub sink")
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func RedisPub() api.Sink {
	return &redisPub{}
}
