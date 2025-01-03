package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type ApiKeyCode int

const (
	ApiVersions ApiKeyCode = 18
)

type ErrorCode int

const (
	UnsupportedVersion ErrorCode = 35
)

type Request struct {
	Size   uint32
	Header Header
}

type Header struct {
	RequestApiKey     uint16
	RequestApiVersion uint16
	CorrelationId     uint32
}

type ResponseWriter interface {
	encode() ([]byte, error)
}

// message_size: int32
// Header
//
//	correleation_id: int32
//
// Body
//
//	error_code: int16
//	api_keys:
//		api_key: int16
//		min_verison: int16
//		max_version: int16
//		tag_buffer: int8
//	throttle_time_ms: int32
//	tag_buffer: int8
type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}
type ApiVersionRes struct {
	CorrelationId  int32
	ErrorCode      int16
	ApiKey         []ApiKey
	ThrottleTimeMs int32
}

func encodeApiKeys(buffer *bytes.Buffer, res *ApiVersionRes) error {

	// write the length of the api keys array
	if err := binary.Write(buffer, binary.BigEndian, uint8(len(res.ApiKey)+1)); err != nil {
		return err
	}
	// write api keys array
	for _, key := range res.ApiKey {
		if err := binary.Write(buffer, binary.BigEndian, key.ApiKey); err != nil {
			return err
		}
		if err := binary.Write(buffer, binary.BigEndian, key.MinVersion); err != nil {
			return err
		}
		if err := binary.Write(buffer, binary.BigEndian, key.MaxVersion); err != nil {
			return err
		}

	}
	return nil
}

func (r *ApiVersionRes) encode() ([]byte, error) {
	buffer := new(bytes.Buffer)

	if err := binary.Write(buffer, binary.BigEndian, r.CorrelationId); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}

	encodeApiKeys(buffer, r)

	// TODO: hack to write tagged field to 0 : update to tagged fields
	if err := binary.Write(buffer, binary.BigEndian, uint8(0)); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}

	// TODO: hack to write tagged field to 0 : update to tagged fields
	if err := binary.Write(buffer, binary.BigEndian, uint8(0)); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func handleApiVersions(req Request) ResponseWriter {
	res := &ApiVersionRes{
		CorrelationId: int32(req.Header.CorrelationId),
		ErrorCode:     0,
		ApiKey: []ApiKey{
			{
				ApiKey:     18,
				MinVersion: 3,
				MaxVersion: 4,
			},
		},
		ThrottleTimeMs: 0,
	}

	if req.Header.RequestApiVersion < 0 || req.Header.RequestApiVersion > 4 {
		res.ErrorCode = int16(UnsupportedVersion)
	}

	return res
}

func responseHandler(req Request) ResponseWriter {

	api := req.Header.RequestApiKey
	var res ResponseWriter

	switch api {
	case uint16(ApiVersions):
		res = handleApiVersions(req)
	default:
		fmt.Println("Error: unsupported API")
	}
	return res
}

func parseRequest(msg []byte) (Request, error) {
	msgSize := binary.BigEndian.Uint32(msg[:4])
	reqApiKey := binary.BigEndian.Uint16(msg[4:6])
	reqApiVersion := binary.BigEndian.Uint16(msg[6:8])
	correlationId := binary.BigEndian.Uint32(msg[8:12])

	res := Request{
		Size: msgSize,
		Header: Header{
			RequestApiKey:     reqApiKey,
			RequestApiVersion: reqApiVersion,
			CorrelationId:     correlationId,
		},
	}
	return res, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buff := make([]byte, 4096)
		n, err := conn.Read(buff)
		if err != nil {
			os.Exit(1)
		}

		msg := make([]byte, n)
		msg = buff[:n]

		req, err := parseRequest(msg)
		if err != nil {
			fmt.Printf("Error: %e", err)
		}
		resp := responseHandler(req)
		res, err := resp.encode()

		if err != nil {
			fmt.Printf("Error: %e", err)
		}

		fmt.Println("len of slice: ", len(res))
		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size[:4], uint32(len(res)))
		conn.Write(size)
		conn.Write(res)
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(c)
	}
}
