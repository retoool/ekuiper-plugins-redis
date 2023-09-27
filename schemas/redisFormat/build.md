## run docker container
```shell
docke exec -it ekuiper-dev bash
```

## go build
```shell
cd /go/plugins/schemas/redisformat
go build -trimpath --buildmode=plugin
```