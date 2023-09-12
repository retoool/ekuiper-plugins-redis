package main

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

type Hobbies struct {
	Indoor  []string `json:"indoor"`
	Outdoor []string `json:"outdoor"`
}

type Sample struct {
	Id      int64   `json:"id"`
	Name    string  `json:"name"`
	Age     int64   `json:"age"`
	Hobbies Hobbies `json:"hobbies"`
}

func (x *Sample) GetSchemaJson() string {
	// return a static schema
	return `{
		"id": {
			"type": "bigint"
	},
		"name": {
			"type": "string"
	},
		"age": {
			"type": "bigint"
	},
		"hobbies": {
			"type": "struct",
			"properties": {
			"indoor": {
				"type": "array",
					"items": {
						"type": "string"
				}
			},
			"outdoor": {
				"type": "array",
					"items": {
						"type": "string"
				}
			}
		}
	}
	}`
}

func (x *Sample) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		result := &Sample{}
		err := MapToStructStrict(r, result)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *Sample) Decode(b []byte) (interface{}, error) {
	result := &Sample{}
	// check error
	err := json.Unmarshal(b, &result)
	if err != nil {
		return nil, err
	}
	// convert struct to map
	hobbyMap := make(map[string]interface{}, 2)
	hobbyMap["indoor"] = result.Hobbies.Indoor
	hobbyMap["outdoor"] = result.Hobbies.Outdoor
	resultMap := make(map[string]interface{}, 4)
	resultMap["id"] = result.Id
	resultMap["name"] = result.Name
	resultMap["age"] = result.Age
	resultMap["hobbies"] = hobbyMap
	return resultMap, err
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

func GetSample() interface{} {
	return &Sample{}
}
