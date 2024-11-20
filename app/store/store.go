package store

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Info struct {
	Role             string
	MasterIP         string
	MasterPort       string
	MasterReplId     string
	MasterReplOffSet int
	slaves           []net.Conn
}

type KVStore struct {
	Info  Info
	store map[string]string
}

func (kv *KVStore) Set(key, value string, expiry int) {

	if expiry != -1 {
		timeout := time.After(time.Duration(expiry) * time.Millisecond)
		go kv.handleExpiry(timeout, key)

	}

	kv.store[key] = value
}

func New() *KVStore {
	return &KVStore{
		Info: Info{
			Role: "master",
		},
		store: make(map[string]string),
	}
}

func (kv *KVStore) LoadFromRDB(rdb *rdb.RDB) {
	if len(rdb.Dbs) < 1 {
		return
	}
	kv.store = rdb.Dbs[0].DbStore

	for _, x := range rdb.Dbs[0].ExpiryStore {
		kv.store[x.Key] = x.Value
		duration := time.Duration(int64(x.Expiry)-time.Now().UnixMilli()) * time.Millisecond
		go kv.handleExpiry(time.After(duration), x.Key)
	}
}

func (kv *KVStore) handleExpiry(timeout <-chan time.Time, key string) {
	<-timeout
	delete(kv.store, key)
}

func (kv *KVStore) HandleConnection(conn net.Conn) {
	parser := resp.NewParser(conn)
	for {
		buff, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("Error parsing : " + err.Error())
		}
		if len(buff) > 0 {
			switch buff[0] {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				msg := buff[1]
				res := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)

				conn.Write([]byte(res))
			case "SET":

				key := buff[1]
				val := buff[2]
				fmt.Println(kv.Info.Role, key, val)
				ex := -1
				if len(buff) > 4 {
					ex, err = strconv.Atoi(buff[4])
					if err != nil {
						panic(err)
					}
				}
				kv.Set(key, val, ex)
				// if conn.RemoteAddr().String() != kv.Info.masterIP+":"+kv.Info.masterPort {
				conn.Write([]byte("+OK\r\n"))
				// }
				for _, slave := range kv.Info.slaves {
					slave.Write(resp.ToArray(buff))
				}
			case "GET":
				key := buff[1]
				val, ok := kv.store[key]
				var res string
				if !ok {
					res = "$-1\r\n"
				} else {
					res = fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				}
				conn.Write([]byte(res))
			case "CONFIG":
				config := resp.NewConfig("redis.conf")
				config.Marshal()
				if len(buff) > 2 && buff[1] == "GET" {
					key := buff[2]
					val, ok := config.Pair[key]
					if ok {
						conn.Write(resp.ToArray([]string{key, val}))
					}
				} else {
					fmt.Println("key not found in config")
				}

			case "KEYS":
				keys := []string{}

				for k := range kv.store {
					keys = append(keys, k)
				}
				conn.Write(resp.ToArray(keys))
			case "INFO":
				res := []string{
					fmt.Sprintf("role:%s", kv.Info.Role),
					fmt.Sprintf("master_replid:%s", kv.Info.MasterReplId),
					fmt.Sprintf("master_repl_offset:%d", kv.Info.MasterReplOffSet),
				}
				conn.Write([]byte(toBulkFromArr(res)))
			case "REPLCONF":
				switch buff[1] {
				case "GETACK":
					conn.Write(resp.ToArray([]string{"REPLCONF", "ACK", "0"}))
				default:
					conn.Write([]byte("+OK\r\n"))
				}
			case "PSYNC":
				fmt.Println(kv.Info.MasterReplId)
				fmt.Println(kv.Info.MasterReplOffSet)
				conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", kv.Info.MasterReplId, kv.Info.MasterReplOffSet)))

				rdbFile, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
				if err != nil {
					panic(err)
				}

				res := append([]byte(fmt.Sprintf("$%d\r\n", len(rdbFile))), rdbFile...)
				kv.Info.slaves = append(kv.Info.slaves, conn)
				fmt.Println(res)
				conn.Write(res)

			}

		}
	}
}

var OP_CODES = []string{"FF", "FE", "FD", "FC", "FB", "FA"}

func toBulkFromArr(arr []string) string {
	total := 0
	var buff string
	for _, x := range arr {
		buff += fmt.Sprintf("%s\r\n", x)
		total += len(x) + 2
	}
	total -= 2
	return fmt.Sprintf("$%d\r\n%s", total, buff)

}

func (kv *KVStore) HandleConections(connections chan net.Conn) {
	for {
		conn := <-connections
		go kv.HandleConnection(conn)
	}
}
