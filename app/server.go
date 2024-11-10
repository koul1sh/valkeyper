package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type KVStore struct {
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
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
				if len(buff) > 2 && buff[1] == "GET" {
					key := buff[2]
					file, err := os.Open("redis.conf")
					if err != nil {
						panic(err)
					}
					defer file.Close()
					b := bufio.NewReader(file)
					for {
						line, _, err := b.ReadLine()
						if err != nil {
							panic(err)
						}
						pair := strings.Split(string(line), " ")

						if pair[0] == key {
							res := toArray(pair)
							fmt.Println(string(res))
							conn.Write(res)
							break
						}
					}
				}
			}

		}
	}
}

func toArray(arr []string) []byte {
	sb := strings.Builder{}
	tmp := fmt.Sprintf("*%d\r\n", len(arr))
	sb.WriteString(tmp)
	for _, ele := range arr {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(ele), ele))
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
	if len(os.Args) > 3 && os.Args[1] == "--dir" {
		dir = os.Args[2]
	}
	if len(os.Args) > 4 && os.Args[3] == "--dbfilename" {
		dbfile = os.Args[4]
	}
	file, err := os.OpenFile("redis.conf", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("dir %s\n", dir))
	if err != nil {
		panic(err)
	}
	file.WriteString(fmt.Sprintf("dbfilename %s\n", dbfile))
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
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
