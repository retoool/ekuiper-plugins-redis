package main

import (
	"bytes"
	"compress/zlib"
	"io/ioutil"
)

func DecompressData(compressedData []byte) ([]byte, error) {
	decompressedBuf := new(bytes.Buffer)
	_, err := decompressedBuf.Write(compressedData)
	if err != nil {
		return nil, err
	}
	zlibReader, err := zlib.NewReader(decompressedBuf)
	if err != nil {
		return nil, err
	}
	defer zlibReader.Close()
	decompressedData, err := ioutil.ReadAll(zlibReader)
	if err != nil {
		return nil, err
	}
	return decompressedData, nil
}
