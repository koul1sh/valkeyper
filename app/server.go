package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

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

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
	}
	parser := NewParser(conn)
	for {

		buff, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("Error parsing : " + err.Error())
		}
		if len(buff) > 0 && buff[0] == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}
