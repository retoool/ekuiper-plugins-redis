package main

import (
	"bytes"
	"compress/zlib"
)

func CompressData(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := zlib.NewWriter(buf)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
