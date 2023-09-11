package main

import (
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/mitchellh/mapstructure"
	"strings"
)

type RedisSourceFormat struct {
	DevCode  string  `json:"devCode"`
	Metric   string  `json:"metric"`
	DataType string  `json:"dataType"`
	Value    float64 `json:"value"`
	Time     int64   `json:"time"`
}

func (x *RedisSourceFormat) Decode(ctx api.StreamContext, redisMsgs string) ([]map[string]interface{}, error) {
	logger := ctx.GetLogger()
	parts := strings.Split(redisMsgs, ",")
	var resultMsgs []map[string]interface{}
	for _, oneMsg := range parts {
		rs := strings.Split(oneMsg, "@")
		if len(rs) != 2 {
			logger.Debugln("unsupported type: %v", oneMsg)
			continue
		}
		point := rs[0]
		pointValue := rs[1]
		lastColonIndex := strings.LastIndex(point, ":")
		devCode := point[:lastColonIndex]
		metric := point[lastColonIndex+1:]
		pvs := strings.Split(pointValue, ":")
		dataType := pvs[0]
		value, err := cast.ToFloat64(pvs[1], cast.CONVERT_ALL)
		if err != nil {
			logger.Debugln(err)
			continue
		}
		timestamp, err := cast.ToInt64(pvs[2], cast.CONVERT_ALL)
		if err != nil {
			logger.Debugln(err)
			continue
		}
		if countDigits64(timestamp) == 10 {
			timestamp *= 1000
		}
		switch countDigits64(timestamp) {
		case 10:
			timestamp *= 1000
		case 13:
		default:
			logger.Debugln("Invalid timestamp format:", timestamp)
			continue
		}
		rm := &RedisSourceFormat{
			DevCode:  devCode,
			Metric:   metric,
			DataType: dataType,
			Value:    value,
			Time:     timestamp,
		}
		var result map[string]interface{}
		err = mapstructure.Decode(rm, &result)
		if err != nil {
			logger.Debugln("Decode failed:", err)
			continue
		}
		resultMsgs = append(resultMsgs, result)
	}
	return resultMsgs, nil
}

func countDigits64(num int64) int {
	count := 0
	for num != 0 {
		num /= 10
		count++
	}
	return count
}
