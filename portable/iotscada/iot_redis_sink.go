package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/gogf/gf/contrib/nosql/redis/v2"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	"strings"
	"sync"
	"time"
)

type iotRedisSinkConfig struct {
	Address  string `json:"address"`
	Db       int    `json:"db"`
	Pass     string `json:"pass"`
	Channels string `json:"channels"`
	Interval int    `json:"interval"`
}
type iotRedisSink struct {
	conf *iotRedisSinkConfig
	conn gredis.Conn

	results []string
	mux     sync.Mutex
	cancel  context.CancelFunc
}

func (s *iotRedisSink) Configure(props map[string]interface{}) error {
	cfg := &iotRedisSinkConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	s.conf = cfg
	config := gredis.Config{
		Address: s.conf.Address,
		Db:      s.conf.Db,
		Pass:    s.conf.Pass,
	}
	group := "default"
	ctx := gctx.New()
	gredis.SetConfig(&config, group)
	s.conn, err = g.Redis().Conn(ctx)
	if err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("redis连接失败")
	}
	return nil
}

func (s *iotRedisSink) Open(ctx api.StreamContext) error {
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
				logger.Info("file sink done")
				return
			}
		}
	}()

	return nil
}

func (s *iotRedisSink) save(logger api.Logger) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if len(s.results) == 0 {
		return
	}

	err := s.PublishgRedisWithMsg(context.TODO(), s.results)
	if err != nil {
		logger.Error(err)
	} else {
		s.results = make([]string, 0)
	}
}

func (s *iotRedisSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		s.mux.Lock()
		var trandatas []map[string]interface{}
		if err := json.Unmarshal(v, &trandatas); err != nil {
			return err
		}
		rm := RedisSinkMsg{}
		for _, trandata := range trandatas {
			encode, err := rm.Encode(trandata)
			if err != nil {
				logger.Error(err)
				continue
			}
			s.results = append(s.results, encode)
		}
		s.mux.Unlock()
	} else {
		logger.Debug("file sink receive non byte data")
	}
	return nil
}

func (s *iotRedisSink) Close(ctx api.StreamContext) error {
	return nil
}

func (s *iotRedisSink) PublishgRedisWithMsg(ctx context.Context, message []string) error {
	msgList := strings.Join(message, ",")
	buf := new(bytes.Buffer)
	writer := zlib.NewWriter(buf)
	_, err := writer.Write([]byte("," + msgList))
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	compressedData := buf.Bytes()
	_, err = g.Redis().Publish(ctx, s.conf.Channels, compressedData)
	if err != nil {
		return err
	}
	return nil
}

func IotRedisSink() api.Sink {
	return &iotRedisSink{}
}
