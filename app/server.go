package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
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

type Response struct {
	Size          uint32
	CorrelationId uint32
	ErrorCode     uint16
}

func (r *Response) encode() []byte {
	res := make([]byte, 12)
	binary.BigEndian.PutUint32(res[0:4], r.Size)
	binary.BigEndian.PutUint32(res[4:8], r.CorrelationId)
	binary.BigEndian.PutUint16(res[8:10], r.ErrorCode)

	return res
}

func responseResolver(req Request) Response {

	res := Response{
		Size:          req.Size,
		CorrelationId: req.Header.CorrelationId,
		ErrorCode:     uint16(35)}

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
		resp := responseResolver(req)
		conn.Write(resp.encode())
	}
}

func main() {
	// You can use print statements ks follows for debugging, they'll be visible when running tests.
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
