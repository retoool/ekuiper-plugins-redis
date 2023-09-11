package main

import (
	"bytes"
	"compress/zlib"
	"io/ioutil"
)

func DecompressData(compressedData []byte) (string, error) {
	decompressedBuf := new(bytes.Buffer)
	_, err := decompressedBuf.Write(compressedData)
	if err != nil {
		return "", err
	}
	zlibReader, err := zlib.NewReader(decompressedBuf)
	if err != nil {
		return "", err
	}
	defer zlibReader.Close()
	decompressedData, err := ioutil.ReadAll(zlibReader)
	if err != nil {
		return "", err
	}
	return string(decompressedData), nil
}
