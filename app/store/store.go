package store

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
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
	Port             string
	flags            map[string]string
}

type StreamEntry struct {
	Id   string
	Pair map[string]string
}

type KVStore struct {
	Info           Info
	store          map[string]string
	AckCh          chan int
	ProcessedWrite bool
	StreamXCh      chan []byte
	Stream         map[string][]StreamEntry
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
			Role:             "master",
			MasterReplId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			MasterReplOffSet: 0,
			Port:             "6379",
		},
		store: make(map[string]string),
		// Connections: make(chan net.Conn),
		AckCh:     make(chan int),
		Stream:    make(map[string][]StreamEntry),
		StreamXCh: make(chan []byte),
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

func (kv *KVStore) HandleConnection(conn net.Conn, parser *resp.Parser) {
	defer conn.Close()
	for {
		buff, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("Error parsing : " + err.Error())
		}
		fmt.Println(buff)
		if len(buff) == 0 {
			continue
		}
		var res []byte = []byte{}
	switchLoop:
		switch strings.ToUpper(buff[0]) {
		case "PING":
			res = []byte("+PONG\r\n")
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
				conn.Write(res)

			}

			if kv.Info.Role == "master" {
				for _, slave := range kv.Info.slaves {
					slave.Write(resp.ToArray(buff))
				}
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
			if len(buff) > 2 && buff[1] == "GET" {
				key := buff[2]
				val, ok := kv.Info.flags[key]
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
			case "ACK":
				kv.AckCh <- 1
				fmt.Println("ack")
				continue
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
			fmt.Println(kv.Info.slaves)
		case "WAIT":
			reqRepl, _ := strconv.Atoi(buff[1])
			timeout, _ := strconv.Atoi(buff[2])

			if len(kv.store) == 0 {
				res = resp.ToInt(len(kv.Info.slaves))
				break
			}
			for _, slave := range kv.Info.slaves {
				go func() {
					slave.Write(resp.ToArray([]string{"REPLCONF", "GETACK", "*"}))
				}()
			}
			acks := 0
			timeoutCh := time.After(time.Duration(timeout) * time.Millisecond)
		loop:
			for acks < reqRepl {
				select {
				case <-kv.AckCh:
					acks++
					fmt.Println("acks", acks)
				case <-timeoutCh:
					break loop
				}
			}
			res = []byte(fmt.Sprintf(":%d\r\n", acks))

		case "TYPE":
			_, ok := kv.store[buff[1]]
			if ok {
				res = []byte("+string\r\n")
			} else {
				_, ok2 := kv.Stream[buff[1]]
				if ok2 {
					res = []byte("+stream\r\n")
				} else {
					res = []byte("+none\r\n")
				}
			}
		case "XADD":
			if buff[2] == "*" {
				buff[2] = fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0)
			}
			if len(kv.Stream[buff[1]]) > 0 {

				fmt.Println("yes", buff[3])
				lastEntry := strings.Split(kv.Stream[buff[1]][len(kv.Stream[buff[1]])-1].Id, "-")
				currEntry := strings.Split(buff[2], "-")

				lastEntryTime, _ := strconv.Atoi(lastEntry[0])
				currEntryTime, _ := strconv.Atoi(currEntry[0])
				lastEntrySeq, _ := strconv.Atoi(lastEntry[1])

				if currEntry[1] == "*" {
					if lastEntryTime == currEntryTime {

						buff[2] = fmt.Sprintf("%d-%d", lastEntryTime, lastEntrySeq+1)
					} else {
						if currEntryTime == 0 {
							buff[2] = fmt.Sprintf("%d-%d", currEntryTime, 1)
						} else {
							buff[2] = fmt.Sprintf("%d-%d", currEntryTime, 0)
						}
					}
					currEntry = strings.Split(buff[2], "-")
					currEntryTime, _ = strconv.Atoi(currEntry[0])
				}

				currEntrySeq, _ := strconv.Atoi(currEntry[1])
				if currEntryTime < 1 && currEntrySeq < 1 {
					res = []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n")
					break
				}
				if lastEntryTime > currEntryTime {
					res = []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
					break
				}
				if lastEntryTime == currEntryTime && lastEntrySeq >= currEntrySeq {
					res = []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
					break
				}
			} else {
				currEntry := strings.Split(buff[2], "-")
				if currEntry[1] == "*" {
					buff[2] = "0-1"
				}
			}
			se := StreamEntry{
				Id:   buff[2],
				Pair: map[string]string{},
			}
			for i := 3; i < len(buff); i += 2 {
				se.Pair[buff[i]] = buff[i+1]
			}
			_, ok := kv.Stream[buff[1]]
			if ok {
				kv.Stream[buff[1]] = append(kv.Stream[buff[1]], se)
			} else {
				kv.Stream[buff[1]] = []StreamEntry{se}
			}
			res = []byte(resp.ToBulkString(buff[2]))
			select {
			case kv.StreamXCh <- res:
				fmt.Println("someones there, sent...")
			default:
				fmt.Println("no one on the other side")
			}
		case "XRANGE":
			key := buff[1]
			start := strings.Split(buff[2], "-")
			if buff[3] == "+" {
				buff[3] = fmt.Sprintf("%d-%d", math.MaxInt64, math.MaxInt64)
			}
			end := strings.Split(buff[3], "-")

			startTime, _ := strconv.Atoi(start[0])
			endTime, _ := strconv.Atoi(end[0])
			startSeq := 0
			endSeq := -1

			if len(start) > 1 {
				startSeq, _ = strconv.Atoi(start[1])
			}
			if len(end) > 1 {
				endSeq, _ = strconv.Atoi(end[1])
			}
			fmt.Println(startTime, " ", endTime, " ", startSeq, " ", endSeq)

			fin := []string{}

			for _, se := range kv.Stream[key] {
				curr := strings.Split(se.Id, "-")
				currTime, _ := strconv.Atoi(curr[0])
				currSeq, _ := strconv.Atoi(curr[1])
				if currTime >= startTime && currSeq >= startSeq && currTime <= endTime && currSeq <= endSeq {
					fmt.Println(se)
					currArr := []string{resp.ToBulkString(se.Id)}
					tmp := []string{}
					for k, v := range se.Pair {
						tmp = append(tmp, k)
						tmp = append(tmp, v)
					}
					currArr = append(currArr, string(resp.ToArray(tmp)))
					fin = append(fin, string(resp.ToArrayAnyType(currArr)))
				}

			}
			res = resp.ToArrayAnyType(fin)
			fmt.Println(string(res))
		case "XREAD":

			var args []string
			var keys []string
			var ids []string
			var waitTime int
			outer := []string{}
			if buff[1] == "block" {
				waitTime, _ = strconv.Atoi(buff[2])
				args = buff[4:]
				keys = args[:len(args)/2]
				ids = args[len(args)/2:]
				timeoutCh := time.After(time.Duration(waitTime) * time.Millisecond)
				if waitTime != 0 {

					select {
					case <-kv.StreamXCh:
						fmt.Println("from xadd")
					case <-timeoutCh:
						res = []byte("$-1\r\n")
						break switchLoop
					}
				} else {

					<-kv.StreamXCh
					fmt.Println("from xadd")
				}
			} else {

				args = buff[2:]
				keys = args[:len(args)/2]
				ids = args[len(args)/2:]
			}

			for i, key := range keys {

				fin := []string{resp.ToBulkString(key)}
				if buff[3] == "+" {
					buff[3] = fmt.Sprintf("%d-%d", math.MaxInt64, math.MaxInt64)
				}

				sub := []string{}
				if ids[i] == "$" {
					se := kv.Stream[key][len(kv.Stream[key])-1]

					currArr := []string{resp.ToBulkString(se.Id)}
					tmp := []string{}
					for k, v := range se.Pair {
						tmp = append(tmp, k)
						tmp = append(tmp, v)
					}
					currArr = append(currArr, string(resp.ToArray(tmp)))
					sub = append(sub, string(resp.ToArrayAnyType(currArr)))
				} else {

					threshold := strings.Split(ids[i], "-")
					thresholdTime, _ := strconv.Atoi(threshold[0])
					thresholdSeq, _ := strconv.Atoi(threshold[1])
					for _, se := range kv.Stream[key] {
						curr := strings.Split(se.Id, "-")
						currTime, _ := strconv.Atoi(curr[0])
						currSeq, _ := strconv.Atoi(curr[1])
						if (currTime == thresholdTime && currSeq > thresholdSeq) || (currTime > thresholdSeq) {
							currArr := []string{resp.ToBulkString(se.Id)}
							tmp := []string{}
							for k, v := range se.Pair {
								tmp = append(tmp, k)
								tmp = append(tmp, v)
							}
							currArr = append(currArr, string(resp.ToArray(tmp)))
							sub = append(sub, string(resp.ToArrayAnyType(currArr)))
						}

					}
				}
				fin = append(fin, string(resp.ToArrayAnyType(sub)))
				outer = append(outer, string(resp.ToArrayAnyType(fin)))
			}
			res = resp.ToArrayAnyType(outer)
			fmt.Println(string(res))
		case "INCR":
			v, ok := kv.store[buff[1]]
			if !ok {
				kv.store[buff[1]] = "1"

			} else {
				val, err := strconv.Atoi(v)
				if err == nil {
					kv.store[buff[1]] = fmt.Sprintf("%d", val+1)
				} else {
					res = []byte("-ERR value is not an integer or out of range\r\n")
					break
				}
			}
			res = []byte(fmt.Sprintf(":%s\r\n", kv.store[buff[1]]))
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
			if buff[0] != "SET" {

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

func (kv *KVStore) SendHandshake(master net.Conn, rdr *resp.Parser) {

	buff := []string{"PING"}
	res := []byte{}
	fmt.Println("sent ping")
	master.Write([]byte(resp.ToArray(buff)))
	res, _ = rdr.ReadBytes('\n')
	fmt.Println(string(res))

	fmt.Println("sent replconf1")
	buff = []string{"REPLCONF", "listening-port", kv.Info.Port}
	master.Write(resp.ToArray(buff))
	res, _ = rdr.ReadBytes('\n')
	fmt.Println(string(res))

	fmt.Println("sent replconf2")
	buff = []string{"REPLCONF", "capa", "psync2"}
	master.Write(resp.ToArray(buff))
	res, _ = rdr.ReadBytes('\n')
	fmt.Println(string(res))

	fmt.Println("sent psync")
	master.Write(resp.ToArray([]string{"PSYNC", "?", "-1"}))
	res, _ = rdr.ReadBytes('\n')
	fmt.Println(string(res))
	// tmp := make([]byte, 93)
	// m, err := rdr.Read(tmp)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("read bytes", m)
	// fmt.Println(string(tmp))
	expectRDBFile(rdr)
}

func expectRDBFile(p *resp.Parser) {
	byt, err := p.ReadByte()
	if err != nil {
		fmt.Println("nothing to read")
		return
	}
	if string(byt) != "$" {
		fmt.Printf("expected rdb to start with $, got %s\n", string(byt))
		p.UnreadByte()
	} else {
		n, err := p.GetLength()
		if err != nil {
			panic(err)
		}
		p.ReadBytes('\n')
		buff := make([]byte, n)
		_, err = io.ReadFull(p, buff)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(buff))
	}
}

func (kv *KVStore) HandleReplication() {
	master, err := net.Dial("tcp", kv.Info.MasterIP+":"+kv.Info.MasterPort)
	if err != nil {
		panic(err)
	}
	rdr := resp.NewParser(master)
	kv.SendHandshake(master, rdr)
	kv.Info.MasterConn = master
	// kv.Connections <- master
	go kv.HandleConnection(master, rdr)
}
func (kv *KVStore) ParseCommandLine() {

	flags := make(map[string]string)

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i][:2] == "--" {
			flags[os.Args[i][2:]] = os.Args[i+1]
			i++
		}
	}
	port, ok := flags["port"]
	if ok {
		kv.Info.Port = port
	}
	replicaof, ok := flags["replicaof"]
	if ok {
		kv.Info.Role = "slave"
		kv.Info.MasterIP = strings.Split(replicaof, " ")[0]
		kv.Info.MasterPort = strings.Split(replicaof, " ")[1]
	}

	dir, ok1 := flags["dir"]
	dbfile, ok2 := flags["dbfilename"]

	if ok1 && ok2 {

		rdb, err := rdb.NewRDB(path.Join(dir, dbfile))
		if err == nil {
			err = rdb.Parse()
			if err != nil {
				panic(err)
			}
			kv.LoadFromRDB(rdb)
		} else {
			fmt.Println(err)
		}
	}
	kv.Info.flags = flags
	fmt.Println(flags)
}
