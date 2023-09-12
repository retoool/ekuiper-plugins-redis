package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	redis "github.com/redis/go-redis/v9"
	"sync"
	"time"
)

type redisPubConfig struct {
	Address  string `json:"address"`
	Db       int    `json:"db"`
	Password string `json:"password"`
	Channel  string `json:"channel"`
	Interval int    `json:"interval"`
}
type redisPub struct {
	conf    *redisPubConfig
	conn    *redis.Client
	results [][]byte
	mux     sync.Mutex
	cancel  context.CancelFunc
}

func (s *redisPub) Configure(props map[string]interface{}) error {
	cfg := &redisPubConfig{}
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

func (s *redisPub) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	t := time.NewTicker(time.Duration(s.conf.Interval) * time.Millisecond)
	exeCtx, cancel := ctx.WithCancel()
	s.cancel = cancel
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.save(logger)
			case <-exeCtx.Done():
				logger.Info("redisPub done")
				return
			}
		}
	}()
	return nil
}

func (s *redisPub) save(logger api.Logger) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if len(s.results) == 0 {
		return
	}
	msgList := bytes.Join(s.results, []byte(","))
	msgList = append([]byte(","), msgList...)
	compressedData, err := CompressData(msgList)
	if err != nil {
		logger.Error(err)
		return
	}
	s.conn.Publish(context.TODO(), s.conf.Channel, compressedData)
	if err != nil {
		logger.Error(err)
	} else {
		s.results = make([][]byte, 0)
	}
}

func (s *redisPub) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	s.mux.Lock()
	defer s.mux.Unlock()

	switch v := item.(type) {
	case []map[string]interface{}:
		for _, trandata := range v {
			rm := RedisSinkFormat{}
			decodedBytes, err := rm.Encode(trandata)
			if err != nil {
				logger.Error(err)
				continue
			}
			s.results = append(s.results, decodedBytes)
			//decodedBytes, _, err := ctx.TransformOutput(trandata)
			//if err != nil {
			//	return fmt.Errorf("redisPub sink transform data error: %v", err)
			//}
			//s.results = append(s.results, decodedBytes)
		}
	case map[string]interface{}:
		rm := RedisSinkFormat{}
		decodedBytes, err := rm.Encode(v)
		if err != nil {
			logger.Error(err)
		} else {
			s.results = append(s.results, decodedBytes)
		}
		//decodedBytes, _, err := ctx.TransformOutput(v)
		//if err != nil {
		//	return fmt.Errorf("redisPub sink transform data error: %v", err)
		//}
		//s.results = append(s.results, decodedBytes)

	default:
		logger.Debug("redisPub received unsupported data type")
	}

	return nil
}

func (s *redisPub) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing redisPub sink")
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func RedisPub() api.Sink {
	return &redisPub{}
}
