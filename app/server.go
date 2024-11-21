package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	kvStore := store.New()
	var port string = "6379"
	kvStore.ParseCommandLine()

	if kvStore.Info.Role == "slave" {
		fmt.Println("connecting to master by slave node")
		go kvStore.HandleReplication()
	}

	addr := fmt.Sprintf("0.0.0.0:%s", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	connections := make(chan net.Conn)
	go kvStore.HandleConections(connections)
	go func() {

		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
			}
			connections <- conn
		}
	}()

}
