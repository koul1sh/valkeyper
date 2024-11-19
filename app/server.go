package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Info struct {
	role       string
	masterIP   string
	masterPort string
}

type KVStore struct {
	info  Info
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		info: Info{
			role: "master",
		},
		store: make(map[string]string),
	}
}

func (kv *KVStore) loadFromRDB(rdb *RDB) {
	kv.store = rdb.dbs[0].dbStore

	for _, x := range rdb.dbs[0].expiryStore {
		kv.store[x.key] = x.value
		duration := time.Duration(int64(x.expiry)-time.Now().UnixMilli()) * time.Millisecond
		go kv.handleExpiry(time.After(duration), x.key)
	}
}

func (kv *KVStore) handleExpiry(timeout <-chan time.Time, key string) {
	<-timeout
	delete(kv.store, key)
}

func (kv *KVStore) handleConnection(conn net.Conn) {
	parser := NewParser(conn)
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
				if len(buff) > 4 {
					ex, err := strconv.Atoi(buff[4])
					if err != nil {
						panic(err)
					}
					timeout := time.After(time.Duration(ex) * time.Millisecond)
					go kv.handleExpiry(timeout, key)

				}

				kv.store[key] = val
				conn.Write([]byte("+OK\r\n"))
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
				config := NewConfig("redis.conf")
				config.marshal()
				if len(buff) > 2 && buff[1] == "GET" {
					key := buff[2]
					val, ok := config.pair[key]
					if ok {
						conn.Write(toArray([]string{key, val}))
					} else {
						fmt.Println("key not found in config")
					}

				}
			case "KEYS":
				keys := []string{}

				for k := range kv.store {
					keys = append(keys, k)
				}
				conn.Write(toArray(keys))
			case "INFO":
				res := []string{
					fmt.Sprintf("role:%s", kv.info.role),
					"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					"master_repl_offset:0",
				}
				conn.Write([]byte(toBulkFromArr(res)))
			case "REPLCONF":
				conn.Write([]byte("+OK\r\n"))
			}

		}
	}
}

var OP_CODES = []string{"FF", "FE", "FD", "FC", "FB", "FA"}

type Database struct {
	index       int
	size        int
	expiry      int
	dbStore     map[string]string
	expiryStore []expiryEntry
}

type expiryEntry struct {
	key    string
	value  string
	expiry uint64
}

type RDB struct {
	reader  *bufio.Reader
	version int
	aux     map[string]string
	dbs     []Database
}

func NewRDB(file string) (*RDB, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return &RDB{
		reader: bufio.NewReader(fd),
		aux:    make(map[string]string),
	}, nil
}
func (rdb *RDB) ParseLength() (int, bool, error) {

	var length int
	var isInt bool = false
	firstByte, err := rdb.reader.ReadByte()
	if err != nil {
		return -1, isInt, err
	}
	bitRep := fmt.Sprintf("%08b", firstByte)
	switch bitRep[:2] {
	case "00":
		length = int(firstByte)
	case "01":
		nextByte, err := rdb.reader.ReadByte()
		if err != nil {
			return -1, isInt, err
		}
		lastN := firstByte & 0b00111111

		merged := uint16(lastN)<<8 | uint16(nextByte)
		fmt.Println("merged", merged)
		length = int(merged)

	case "11":
		mask := byte(1<<6 - 1)
		flag := int(firstByte & mask)
		if flag == 0 {
			length = 1
		} else if flag == 1 {
			length = 2
		} else if flag == 2 {
			length = 4
		}
		isInt = true
	default:
		return -1, isInt, fmt.Errorf("not 00")
	}
	return length, isInt, nil
}

func (rdb *RDB) ParseString() (string, error) {
	length, isInt, err := rdb.ParseLength()
	if err != nil {
		return "", nil
	}
	res := make([]byte, length)
	_, err = io.ReadFull(rdb.reader, res)
	if err != nil {
		return "", err
	}
	if isInt {
		conv, err := binary.Varint(res)
		if err == 0 {
			return "", fmt.Errorf("something went wrong")
		}
		return strconv.Itoa(int(conv)), nil
	}
	return string(res), nil
}

func (rdb *RDB) ParseAux() error {

	byt, _ := rdb.reader.ReadByte()
	flg := fmt.Sprintf("%X", byt)
	if flg != "FA" {
		rdb.reader.UnreadByte()
		return fmt.Errorf("not aux")
	}
	key, err := rdb.ParseString()
	if err != nil {
		return err
	}
	val, err := rdb.ParseString()
	if err != nil {
		return err
	}
	rdb.aux[key] = val
	return nil
}

func (rdb *RDB) ParseSelectDB() error {
	byt, err := rdb.reader.ReadByte()
	if err != nil {
		return err
	}
	flg := fmt.Sprintf("%X", byt)
	if flg != "FE" {
		rdb.reader.UnreadByte()
		return fmt.Errorf("not FE")
	}
	dbIdx, _, err := rdb.ParseLength()
	if err != nil {
		return err
	}
	db := Database{
		index:       dbIdx,
		dbStore:     make(map[string]string),
		expiryStore: []expiryEntry{},
	}
	rdb.dbs = append(rdb.dbs, db)

	rdb.reader.ReadByte() // read FB

	dbLen, _, err := rdb.ParseLength()
	if err != nil {
		return err
	}
	dbExp, _, err := rdb.ParseLength()
	if err != nil {
		return err
	}

	rdb.dbs[dbIdx].size = dbLen
	rdb.dbs[dbIdx].expiry = dbExp
	//load key value
	for {
		var expiry uint64
		var hasExp bool = false
		byt, _ = rdb.reader.ReadByte()
		if fmt.Sprintf("%X", byt) == "FC" {
			hasExp = true
			buff := make([]byte, 8)
			rdb.reader.Read(buff)

			expiry = binary.LittleEndian.Uint64(buff)

		} else {
			rdb.reader.UnreadByte()
		}
		keyval, err := rdb.ParseKeyValue(dbIdx)

		if err != nil {
			if err.Error() == "not kv string" {
				break
			} else {
				return err
			}
		}
		if hasExp {
			rdb.dbs[dbIdx].expiryStore = append(rdb.dbs[dbIdx].expiryStore, expiryEntry{
				key:    keyval[0],
				value:  keyval[1],
				expiry: expiry,
			})
		} else {
			rdb.dbs[dbIdx].dbStore[keyval[0]] = keyval[1]
		}
	}
	return nil
}

func (rdb *RDB) ParseKeyValue(dbIdx int) ([]string, error) {
	byt, err := rdb.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if byt == 0 {
		key, err := rdb.ParseString()
		if err != nil {
			return nil, err
		}
		val, err := rdb.ParseString()
		if err != nil {
			return nil, err
		}
		return []string{key, val}, nil
	} else {
		rdb.reader.UnreadByte()
		return nil, fmt.Errorf("not kv string")
	}
}

func (rdb *RDB) Parse() error {

	buff := make([]byte, 5)
	rdb.reader.Read(buff)
	if string(buff) != "REDIS" {
		return fmt.Errorf("not a rdb file")
	}
	rdb.reader.Read(buff[:4])
	num, err := strconv.Atoi(string(buff[:4]))
	if err != nil {
		panic(err)
	}
	rdb.version = num
	//parse Auxilary fields section
	for {
		err = rdb.ParseAux()
		if err != nil && err.Error() == "not aux" {
			break
		}
	}
	//parse db section
	for {
		err = rdb.ParseSelectDB()
		if err != nil {
			if err.Error() == "not FE" {
				break
			} else {
				return err
			}
		}
	}

	byt, err := rdb.reader.ReadByte()
	if err != nil {
		return err
	}

	flg := fmt.Sprintf("%X", byt)
	if flg == "FF" {
		fmt.Println("rdb file parsing complete")
		return nil
	}
	return nil
}
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
func toBulkString(ele string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(ele), ele)
}
func toArray(arr []string) []byte {
	sb := strings.Builder{}
	tmp := fmt.Sprintf("*%d\r\n", len(arr))
	sb.WriteString(tmp)
	for _, ele := range arr {
		sb.WriteString(toBulkString(ele))
	}
	return []byte(sb.String())
}

func (kv *KVStore) handleConections(connections chan net.Conn) {
	for {
		conn := <-connections
		go kv.handleConnection(conn)
	}
}

type Parser struct {
	bufio.Reader
}

type Config struct {
	file string
	pair map[string]string
}

func NewConfig(file string) *Config {
	return &Config{
		file: file,
		pair: make(map[string]string),
	}
}

// func (c *Config) get(key string) string {
// 	return c.pair[key]
// }

func (c *Config) marshal() {

	file, err := os.Open(c.file)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	b := bufio.NewReader(file)
	for {
		line, _, err := b.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		pair := strings.Split(string(line), " ")
		c.pair[pair[0]] = pair[len(pair)-1]
	}
}

func (p *Parser) getLength() (int, error) {
	buff := strings.Builder{}
	for {
		chr, err := p.ReadByte()
		if err != nil {
			return -1, err
		}
		if chr == '\r' {
			p.UnreadByte()
			break
		}
		err = buff.WriteByte(chr)
		if err != nil {
			return -1, err
		}
	}
	res, err := strconv.Atoi(buff.String())
	if err != nil {
		return -1, err
	}
	return res, nil
}

func NewParser(rdr io.Reader) *Parser {
	return &Parser{
		*bufio.NewReader(rdr),
	}
}
func (p *Parser) ParseBulkString() ([]byte, error) {
	length, err := p.getLength()
	if err != nil {
		return nil, err
	}
	crlf := make([]byte, 2)
	_, err = io.ReadFull(p, crlf)
	if err != nil {
		return nil, err
	}

	res := make([]byte, length)
	_, err = io.ReadFull(p, res)
	if err != nil {
		return nil, err
	}

	return res, nil

}

func (p *Parser) ParseArray() ([]string, error) {
	length, err := p.getLength()
	if err != nil {
		return nil, err
	}
	res := make([]string, length)
	for i := 0; i < length; i++ {
		crlf := make([]byte, 2)
		_, err = io.ReadFull(p, crlf)
		if err != nil {
			return nil, err
		}
		iden, err := p.ReadByte()
		if err != nil {
			return nil, err
		}

		if string(iden) == "$" {
			str, err := p.ParseBulkString()
			if err != nil {
				return nil, err
			}
			res[i] = string(str)
		}
	}
	return res, nil
}

func (p *Parser) Parse() ([]string, error) {
	iden, err := p.ReadByte()
	if err != nil {
		return nil, err
	}
	res := []string{}
	if iden == '*' {
		res, err = p.ParseArray()
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	kvStore := NewKVStore()
	var dir string
	var dbfile string
	var port string = "6379"
	if len(os.Args) > 1 && os.Args[1] == "--port" {
		port = os.Args[2]
	}
	if len(os.Args) > 3 {
		if os.Args[3] == "--replicaof" {
			master := strings.Split(os.Args[4], " ")
			kvStore.info.masterIP = string(master[0])
			kvStore.info.masterPort = string(master[1])
			kvStore.info.role = "slave"
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

		rdb, err := NewRDB(path.Join(dir, dbfile))
		if err == nil {
			err = rdb.Parse()
			if err != nil {
				panic(err)
			}
			kvStore.loadFromRDB(rdb)
			fmt.Println(kvStore)
		} else {
			fmt.Println(err)
		}
	}

	addr := fmt.Sprintf("0.0.0.0:%s", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	switch kvStore.info.role {
	case "slave":
		master, err := net.Dial("tcp", kvStore.info.masterIP+":"+kvStore.info.masterPort)
		if err != nil {
			panic(err)
		}
		resp := make([]byte, 1024)
		buff := []string{"PING"}
		master.Write([]byte(toArray(buff)))
		n, err := master.Read(resp)
		if err != nil {
			panic(err)
		}
		fmt.Println(resp[:n])
		buff = []string{"REPLCONF", "listening-port", port}
		master.Write(toArray(buff))
		master.Read(resp)
		buff = []string{"REPLCONF", "capa", "eof", "capa", "psync2"}
		master.Write(toArray(buff))
		master.Read(resp)
		master.Write(toArray([]string{"PSYNC", "?", "-1"}))

	}
	connections := make(chan net.Conn)
	go kvStore.handleConections(connections)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		connections <- conn
	}
}
