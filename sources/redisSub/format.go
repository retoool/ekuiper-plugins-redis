package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
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

func (x *RedisSourceFormat) Decode(b []byte) (interface{}, error) {
	redisMsgs := string(b)
	parts := strings.Split(redisMsgs, ",")
	var resultMsgs []map[string]interface{}
	for _, oneMsg := range parts {
		rs := strings.Split(oneMsg, "@")
		if len(rs) != 2 {
			fmt.Println("unsupported type: %v", oneMsg)
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
			fmt.Println(err)
			continue
		}
		timestamp, err := cast.ToInt64(pvs[2], cast.CONVERT_ALL)
		if err != nil {
			fmt.Println(err)
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
			fmt.Println("Invalid timestamp format:", timestamp)
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
			fmt.Println("Decode failed:", err)
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
