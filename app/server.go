package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	kvStore := store.New()
	kvStore.ParseCommandLine()

	// go kvStore.HandleConections()

	fmt.Println(kvStore)

	if kvStore.Info.Role == "slave" {
		fmt.Println("connecting to master by slave node")
		kvStore.HandleReplication()
	}

	addr := fmt.Sprintf("0.0.0.0:%s", kvStore.Info.Port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", kvStore.Info.Port)
		os.Exit(1)
	}
	defer l.Close()
	fmt.Printf("Listening on port %s\n", kvStore.Info.Port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		// kvStore.Connections <- conn
		rdr := resp.NewParser(conn)
		go kvStore.HandleConnection(conn, rdr)
	}

}
