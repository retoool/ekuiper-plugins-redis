package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/redis/go-redis/v9"
)

type iotRedisSourceConfig struct {
	Address  string   `json:"address"`
	Db       int      `json:"db"`
	Pass     string   `json:"pass"`
	Channels []string `json:"channels"`
	Metrics  []string `json:"metrics"`
}

type iotRedisSource struct {
	conf *iotRedisSourceConfig
	conn *redis.Client
}

func (s *iotRedisSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &iotRedisSourceConfig{}
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

func (s *iotRedisSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	defer func() {
		if r := recover(); r != nil {
			//errCh <- fmt.Errorf("recovered from panic: %v", r)
		}
		close(consumer)
	}()
	pubsub := s.conn.PSubscribe(ctx, s.conf.Channels...)
	defer pubsub.Close()
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			//errCh <- err
			continue
		}
		data, err := DecompressData(cast.StringToBytes(msg.Payload))
		if err != nil {
			//errCh <- err
			continue
		}
		rm := RedisSourceMsg{}
		decodeDatas, _ := rm.Decode(data, s.conf.Metrics)
		if err != nil {
			continue
		}
		for _, decodeData := range decodeDatas {
			consumer <- api.NewDefaultSourceTuple(decodeData, nil)
		}
		select {
		case <-ctx.Done():
			logger.Info("redis source done")
			return
		default:
			// do nothing
		}
	}
}
func (s *iotRedisSource) Close(_ api.StreamContext) error {
	return nil
}

func IotRedisSource() api.Source {
	return &iotRedisSource{}
}
