// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build compression || !core

package compressor

import (
	"ekuiper-plugins-redis/pkg/compressor/flate"
	"ekuiper-plugins-redis/pkg/compressor/gzip"
	"ekuiper-plugins-redis/pkg/compressor/zlib"
	"ekuiper-plugins-redis/pkg/compressor/zstd"
	"ekuiper-plugins-redis/pkg/message"
)

const (
	ZLIB  = "zlib"
	GZIP  = "gzip"
	FLATE = "flate"
	ZSTD  = "zstd"
)

func init() {
	compressors[ZLIB] = func(name string) (message.Compressor, error) {
		return zlib.NewZlibCompressor()
	}
	compressors[GZIP] = func(name string) (message.Compressor, error) {
		return gzip.NewGzipCompressor()
	}
	compressors[FLATE] = func(name string) (message.Compressor, error) {
		return flate.NewFlateCompressor()
	}
	compressors[ZSTD] = func(name string) (message.Compressor, error) {
		return zstd.NewZstdCompressor()
	}

	compressWriters[GZIP] = gzip.NewWriter
	compressWriters[ZSTD] = zstd.NewWriter
}
