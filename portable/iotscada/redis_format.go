package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
	"io/ioutil"
	"strings"
)

type RedisSourceMsg struct {
	DevCode  string  `json:"DevCode"`
	Metric   string  `json:"Metric"`
	DataType string  `json:"DataType"`
	Value    float64 `json:"Value"`
	Time     int64   `json:"Time"`
}

type RedisSinkMsg struct {
	DevCode_Sink  string  `json:"DevCode_Sink"`
	Metric_Sink   string  `json:"Metric_Sink"`
	DataType_Sink string  `json:"DataType_Sink"`
	Value_Sink    float64 `json:"Value_Sink"`
	Time_Sink     int64   `json:"Time_Sink"`
}

func (x *RedisSourceMsg) GetSchemaJson() string {
	// return a static schema
	return `{
		"devCode": {
			"type": "string"
		},
		"metric": {
			"type": "string"
		},
		"dataType": {
			"type": "string"
		},
		"value": {
			"type": "float64"
		},
		"time": {
			"type": "int64"
		},
	}`
}

// DTHYJK:NSFC:Q1:W001:WNAC_WdSpd@s:value:timestamp(unixmilli)
// @f;@s;@b
func (x *RedisSinkMsg) Encode(d interface{}) (string, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		err := MapToStructStrict(r, x)
		if err != nil {
			return "", err
		}
		result := x.DevCode_Sink + ":" + x.Metric_Sink + "@" + x.DataType_Sink + ":" + cast.ToString(x.Value_Sink) + ":" + cast.ToString(x.Time_Sink)
		return result, nil
	default:
		return "", fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *RedisSourceMsg) Decode(redisMsgs string, mainmetrics []string) ([]map[string]interface{}, error) {
	parts := strings.Split(redisMsgs, ",")
	var resultMsgs []map[string]interface{}
	for _, oneMsg := range parts {
		rs := strings.Split(oneMsg, "@")
		if len(rs) != 2 {
			//fmt.Println("unsupported type: %v", oneMsg)
			continue
		}
		point := rs[0]
		pointValue := rs[1]
		lastColonIndex := strings.LastIndex(point, ":")
		devCode := point[:lastColonIndex]
		metric := point[lastColonIndex+1:]
		if len(mainmetrics) > 0 && !isElementInSlice(metric, mainmetrics) {
			continue
		}
		pvs := strings.Split(pointValue, ":")
		dataType := pvs[0]
		value, err := cast.ToFloat64E(pvs[1])
		if err != nil {
			//fmt.Println(err)
			continue
		}
		timestamp, err := cast.ToInt64E(pvs[2])
		if err != nil {
			//fmt.Println(err)
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
			//fmt.Println("时间戳格式错误")
			continue
		}
		rm := &RedisSourceMsg{
			DevCode:  devCode,
			Metric:   metric,
			DataType: dataType,
			Value:    value,
			Time:     timestamp,
		}
		var result map[string]interface{}
		err = mapstructure.Decode(rm, &result)
		if err != nil {
			//fmt.Println("转换失败:", err)
			continue
		}
		resultMsgs = append(resultMsgs, result)
	}
	return resultMsgs, nil
}

func MapToStructStrict(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		TagName: "json",
		Result:  output,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func DecompressData(compressedData []byte) (string, error) {
	// 创建一个 bytes.Buffer 来保存解压后的数据
	decompressedBuf := new(bytes.Buffer)

	// 将压缩数据写入一个 bytes.Buffer 中
	_, err := decompressedBuf.Write(compressedData)
	if err != nil {
		return "", err
	}

	// 创建一个 zlib.Reader 来解压数据
	zlibReader, err := zlib.NewReader(decompressedBuf)
	if err != nil {
		return "", err
	}
	defer zlibReader.Close()

	// 读取解压后的数据并转换为字符串
	decompressedData, err := ioutil.ReadAll(zlibReader)
	if err != nil {
		return "", err
	}

	// 去除开头的逗号并返回解压后的字符串
	return string(decompressedData), nil
}

func countDigits64(num int64) int {
	count := 0
	for num != 0 {
		num /= 10
		count++
	}
	return count
}

func isElementInSlice(element string, slice []string) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}
