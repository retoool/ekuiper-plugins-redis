package main

import (
	"github.com/lf-edge/ekuiper/sdk/go/api"
	sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
	"os"
)

func main() {
	sdk.Start(os.Args, &sdk.PluginConfig{
		Name: "iotscada",
		Sources: map[string]sdk.NewSourceFunc{
			"iotRedisSource": func() api.Source {
				return &iotRedisSource{}
			},
		},
		Functions: map[string]sdk.NewFunctionFunc{
			"echo": func() api.Function {
				return &echo{}
			},
		},
		Sinks: map[string]sdk.NewSinkFunc{
			"iotRedisSink": func() api.Sink {
				return &iotRedisSink{}
			},
		},
	})
}
