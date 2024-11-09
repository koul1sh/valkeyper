package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
	fmt.Println(conn.RemoteAddr().String())
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
			}

		}
	}
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

func NewParser(rdr io.Reader) *Parser {
	return &Parser{
		*bufio.NewReader(rdr),
	}
}
func (p *Parser) ParseBulkString() ([]byte, error) {
	byt, err := p.ReadByte()
	if err != nil {
		return nil, err
	}
	length, _ := strconv.Atoi(string(byt))
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
	byt, err := p.ReadByte()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(string(byt))
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
