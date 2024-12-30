package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {

	kvStore := store.New()
	kvStore.ParseCommandLine()

	// go kvStore.HandleConections()

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
		connection := store.Connection{
			Conn:       conn,
			TxnStarted: false,
			TxnQueue:   [][]string{},
		}
		rdr := resp.NewParser(conn)
		go kvStore.HandleConnection(connection, rdr)
	}

}
