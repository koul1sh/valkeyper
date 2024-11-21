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
	MasterConn       net.Conn
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

func (kv *KVStore) LoadRDB(master net.Conn) {

	fmt.Println("started loading rdb from master...")
	parser := resp.NewParser(master)
	// for {

	byt, err := parser.ReadByte()
	if err != nil {
		fmt.Println("no byte to read", err)
		return

	}
	fmt.Println("first", byt)
	if string(byt) == "$" {
		fmt.Println("not possible")
		rdbContent, _ := parser.ParseBulkString()
		rdb, err := rdb.NewFromBytes(rdbContent)
		if err != nil {
			panic(err)
		}
		kv.LoadFromRDB(rdb)
		// break
	} else {
		fmt.Printf("expected $, got %s\n", string(byt))
		parser.UnreadByte()
	}
	// }
	fmt.Println("finished loading rdb from master...")

}

func (kv *KVStore) HandleConnection(conn net.Conn) {
	if conn == kv.Info.MasterConn {
		fmt.Println("reading from master")
	}
	time.Sleep(time.Millisecond * 100)
	parser := resp.NewParser(conn)
	for {
		buff, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				if conn == kv.Info.MasterConn {
					fmt.Println("nothing to read from master")
				}
				break
			}
			panic("Error parsing : " + err.Error())
		}
		fmt.Println(buff)
		var res []byte = []byte{}
		if len(buff) > 0 {
			switch buff[0] {
			case "PING":
				if conn != kv.Info.MasterConn {
					res = []byte("+PONG\r\n")
				}
			case "ECHO":
				msg := buff[1]
				res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg))

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
				if kv.Info.MasterConn != conn {
					res = []byte("+OK\r\n")
				}
				for _, slave := range kv.Info.slaves {
					slave.Write(resp.ToArray(buff))
				}
			case "GET":
				key := buff[1]
				val, ok := kv.store[key]
				if !ok {
					res = []byte("$-1\r\n")
				} else {
					res = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
				}
			case "CONFIG":
				config := resp.NewConfig("redis.conf")
				config.Marshal()
				if len(buff) > 2 && buff[1] == "GET" {
					key := buff[2]
					val, ok := config.Pair[key]
					if ok {
						res = resp.ToArray([]string{key, val})
					}
				} else {
					fmt.Println("key not found in config")
				}

			case "KEYS":
				keys := []string{}

				for k := range kv.store {
					keys = append(keys, k)
				}
				res = resp.ToArray(keys)
			case "INFO":
				info := []string{
					fmt.Sprintf("role:%s", kv.Info.Role),
					fmt.Sprintf("master_replid:%s", kv.Info.MasterReplId),
					fmt.Sprintf("master_repl_offset:%d", kv.Info.MasterReplOffSet),
				}
				res = []byte(toBulkFromArr(info))
			case "REPLCONF":
				switch buff[1] {
				case "GETACK":
					res = resp.ToArray([]string{"REPLCONF", "ACK", fmt.Sprintf("%d", kv.Info.MasterReplOffSet)})
				default:
					res = []byte("+OK\r\n")
				}
			case "PSYNC":
				fmt.Println(kv.Info.MasterReplId)
				fmt.Println(kv.Info.MasterReplOffSet)
				res = []byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", kv.Info.MasterReplId, kv.Info.MasterReplOffSet))

				rdbFile, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
				if err != nil {
					panic(err)
				}

				tmp := append([]byte(fmt.Sprintf("$%d\r\n", len(rdbFile))), rdbFile...)
				res = append(res, tmp...)
				kv.Info.slaves = append(kv.Info.slaves, conn)
			}

			if kv.Info.Role == "slave" {

				if conn == kv.Info.MasterConn {
					if buff[0] == "REPLCONF" && buff[1] == "GETACK" {
						conn.Write(res)
					}
				} else {
					conn.Write(res)
				}

				kv.Info.MasterReplOffSet += len(resp.ToArray(buff))
			} else {
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
