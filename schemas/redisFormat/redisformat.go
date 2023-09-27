package main

import (
	"bytes"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
	"strings"
)

type ScadaRedis struct {
	DevCode  string  `json:"devCode"`
	Metric   string  `json:"metric"`
	DataType string  `json:"dataType"`
	Value    float64 `json:"value"`
	Time     int64   `json:"time"`
}

func (x *ScadaRedis) GetSchemaJson() string {
	// return a static schema
	return `{
		"DevCode": {"type": "string"},
		"Metric": {"type": "string"},
		"DataType": {"type": "string"},
		"Value": {"type": "string"},
		"Time": {"type": "string"}
	}`
}

func (x *ScadaRedis) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		fmt.Println("sink rec:", r)
		err := MapToStructStrict(r, x)
		if err != nil {
			return nil, err
		}
		Value_Sink := cast.ToString(x.Value)
		Time_Sink := cast.ToString(x.Time)
		result := []byte(x.DevCode + ":" + x.Metric + "@" + x.DataType + ":" + Value_Sink + ":" + Time_Sink)
		return result, nil
	case []map[string]interface{}:
		var bs [][]byte
		for item := range r {
			fmt.Println("sink rec:", item)
			err := MapToStructStrict(item, x)
			if err != nil {
				return nil, err
			}
			Value_Sink := cast.ToString(x.Value)
			Time_Sink := cast.ToString(x.Time)
			b := []byte(x.DevCode + ":" + x.Metric + "@" + x.DataType + ":" + Value_Sink + ":" + Time_Sink)
			bs = append(bs, b)
		}
		result := bytes.Join(bs, []byte(","))
		result = append([]byte(","), result...)
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *ScadaRedis) Decode(b []byte) (interface{}, error) {
	parts := strings.Split(string(b), ",")
	var resultMsgs []map[string]interface{}
	//fmt.Println("........................................")
	for _, oneMsg := range parts {
		rs := strings.Split(oneMsg, "@")
		if len(rs) != 2 {
			//fmt.Printf("unsupported type: %v", oneMsg)
			continue
		}
		point := rs[0]
		pointValue := rs[1]
		lastColonIndex := strings.LastIndex(point, ":")
		devCode := point[:lastColonIndex]
		metric := point[lastColonIndex+1:]
		pvs := strings.Split(pointValue, ":")
		dataType := pvs[0]
		value := cast.ToFloat64(pvs[1])
		timestamp := cast.ToInt64(pvs[2])
		if countDigits64(timestamp) == 10 {
			timestamp *= 1000
		}
		switch countDigits64(timestamp) {
		case 10:
			timestamp *= 1000
		case 13:
		default:
			//fmt.Println("Invalid timestamp format:", timestamp)
			continue
		}
		rm := &ScadaRedis{
			DevCode:  devCode,
			Metric:   metric,
			DataType: dataType,
			Value:    value,
			Time:     timestamp,
		}
		var result map[string]interface{}
		err := mapstructure.Decode(rm, &result)
		if err != nil {
			//fmt.Println("Decode failed:", err)
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

func MapToStructStrict(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		ErrorUnused: true,
		TagName:     "json",
		Result:      output,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func GetScadaRedis() interface{} {
	return &ScadaRedis{}
}
