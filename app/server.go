package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func responeResolver() []byte {
	msgSize := int32(4)
	correlationId := int32(7)

	response := make([]byte, 8)

	binary.BigEndian.PutUint32(response[0:4], uint32(msgSize))
	binary.BigEndian.PutUint32(response[4:8], uint32(correlationId))

	return response
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buff := make([]byte, 4096)
		_, err := conn.Read(buff)
		if err != nil {
			os.Exit(1)
		}

		resp := responeResolver()
		conn.Write(resp)
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
