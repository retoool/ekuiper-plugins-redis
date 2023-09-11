package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/mitchellh/mapstructure"
)

type RedisSinkFormat struct {
	DevCode_Sink  string  `json:"DevCode_Sink"`
	Metric_Sink   string  `json:"Metric_Sink"`
	DataType_Sink string  `json:"DataType_Sink"`
	Value_Sink    float64 `json:"Value_Sink"`
	Time_Sink     int64   `json:"Time_Sink"`
}

func (x *RedisSinkFormat) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		err := MapToStruct(r, x)
		if err != nil {
			return nil, err
		}
		Value_Sink, err := cast.ToString(x.Value_Sink, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		Time_Sink, err := cast.ToString(x.Time_Sink, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		result := []byte(x.DevCode_Sink + ":" + x.Metric_Sink + "@" + x.DataType_Sink + ":" + Value_Sink + ":" + Time_Sink)
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func MapToStruct(input, output interface{}) error {
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
