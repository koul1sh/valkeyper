package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	kvStore := store.New()
	var port string = "6379"
	parseFlags(&port, kvStore)

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
	switch kvStore.Info.Role {
	case "slave":
		master, err := net.Dial("tcp", kvStore.Info.MasterIP+":"+kvStore.Info.MasterPort)
		if err != nil {
			panic(err)
		}
		sendHandshake(master, port)

		kvStore.HandleConnection(master)

	case "master":
		kvStore.Info.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		kvStore.Info.MasterReplOffSet = 0
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	fmt.Println("\nShutting down gracefully...")

}

func parseFlags(port *string, kvStore *store.KVStore) {
	var dir string
	var dbfile string
	if len(os.Args) > 1 && os.Args[1] == "--port" {
		*port = os.Args[2]
	}
	if len(os.Args) > 3 {
		if os.Args[3] == "--replicaof" {
			master := strings.Split(os.Args[4], " ")
			kvStore.Info.MasterIP = string(master[0])
			kvStore.Info.MasterPort = string(master[1])
			kvStore.Info.Role = "slave"
		}
	}
	if len(os.Args) > 4 && os.Args[1] == "--dir" && os.Args[3] == "--dbfilename" {
		dir = os.Args[2]
		dbfile = os.Args[4]
		file, err := os.OpenFile("redis.conf", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		_, err = file.WriteString(fmt.Sprintf("dir %s\n", dir))
		if err != nil {
			panic(err)
		}
		file.WriteString(fmt.Sprintf("dbfilename %s\n", dbfile))
		file.Close()

		rdb, err := rdb.NewRDB(path.Join(dir, dbfile))
		if err == nil {
			err = rdb.Parse()
			if err != nil {
				panic(err)
			}
			kvStore.LoadFromRDB(rdb)
			fmt.Println(kvStore)
		} else {
			fmt.Println(err)
		}
	}
}

func sendHandshake(master net.Conn, port string) {

	rdr := bufio.NewReader(master)
	buff := []string{"PING"}
	master.Write([]byte(resp.ToArray(buff)))
	rdr.ReadBytes('\n')

	buff = []string{"REPLCONF", "listening-port", port}
	master.Write(resp.ToArray(buff))
	rdr.ReadBytes('\n')

	buff = []string{"REPLCONF", "capa", "eof", "capa", "psync2"}
	master.Write(resp.ToArray(buff))
	rdr.ReadBytes('\n')

	master.Write(resp.ToArray([]string{"PSYNC", "?", "-1"}))
	tmp, _ := rdr.ReadBytes('\n')
	fmt.Println("end part", string(tmp))

}
