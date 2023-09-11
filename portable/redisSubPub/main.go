package main

import (
	"github.com/lf-edge/ekuiper/sdk/go/api"
	sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
	"os"
)

func main() {
	sdk.Start(os.Args, &sdk.PluginConfig{
		Name: "redisSubPub",
		Sources: map[string]sdk.NewSourceFunc{
			"redisSub": func() api.Source {
				return &redisSub{}
			},
		},
		Sinks: map[string]sdk.NewSinkFunc{
			"redisPub": func() api.Sink {
				return &redisPub{}
			},
		},
	})
}
