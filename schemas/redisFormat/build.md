## run docker container
```shell
docker run --rm -it --name redisformat -v .:/app -w /app golang:1.20
```

## go env set
```shell
export GO111MODULE=on
export GOPROXY=https://goproxy.cn
```

## go build
```shell
go build -trimpath --buildmode=plugin
```