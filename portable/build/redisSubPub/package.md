## run docker container
```shell
docker run --rm -it -v .:/app --name ziptmp -w /app alpine:latest
```
## install zip
```shell
# apk update
apk add zip
```
## zip redisSubPub.zip
```shell
zip -r redisSubPub.zip sinks sources redisSubPub redisSubPub.json
exit
```

