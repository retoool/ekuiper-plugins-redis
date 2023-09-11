# portable plugin


# native plugin

## run docker container
```shell
docker run -it --name ekuiper-dev -v .:/go/plugins -w /go  lfedge/ekuiper:1.11.1-dev bash
```

## go env set
```shell
export GO111MODULE=on
export GOPROXY=https://goproxy.cn
```
## go work
```shell
cd /go
go work init ./kuiper ./plugins
```

## go build
Notice:
Before executing go build, it is necessary to comment out the dependency libraries in the go.mod file.
```shell
cd /go/plugins/sources/redisSub/ && go build -trimpath --buildmode=plugin -o RedisSub.so && zip redisSub.zip RedisSub.so redisSub.json redisSub.yaml
```
```shell
cd /go/plugins/sinks/redisPub/ && go build -trimpath --buildmode=plugin -o RedisPub.so && zip redisPub.zip RedisPub.so redisPub.json
```

