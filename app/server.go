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

type Response struct {
	Size          int32
	CorrelationId int32
	ErrorCode     int16
}

// message_size: int32
// Header
//
//	correleation_id: int32
//
// Body
//
//	error_code: int16
//	api_keys: tag_buffer
//		api_key: int16
//		min_verison: int16
//		max_version: int16
//	throttle_time_ms: int32
type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}
type ApiVersionRes struct {
	Size           int32
	CorrelationId  int32
	ErrorCode      int16
	ApiKey         []ApiKey
	ThrottleTimeMs int32
}

func (r *Response) encode() []byte {
	res := make([]byte, 12)
	binary.BigEndian.PutUint32(res[0:4], 12)
	binary.BigEndian.PutUint32(res[4:8], uint32(r.CorrelationId))
	binary.BigEndian.PutUint16(res[8:10], uint16(r.ErrorCode))

	return res
}

func encodeApiKeys(buffer *bytes.Buffer, res *ApiVersionRes) error {

	// write the length of the api keys array
	if err := binary.Write(buffer, binary.BigEndian, len(res.ApiKey)); err != nil {
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

	if err := binary.Write(buffer, binary.BigEndian, r.Size); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.BigEndian, r.CorrelationId); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}

	encodeApiKeys(buffer, r)

	if err := binary.Write(buffer, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func handleApiVersions(req Request) ResponseWriter {
	res := &ApiVersionRes{
		Size:          24,
		CorrelationId: int32(req.Header.CorrelationId),
		ErrorCode:     0,
		ApiKey: []ApiKey{
			{
				ApiKey:     18,
				MaxVersion: 4,
				MinVersion: 1,
			},
		},
		ThrottleTimeMs: 30,
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
