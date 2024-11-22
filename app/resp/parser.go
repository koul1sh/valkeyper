package resp

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type Parser struct {
	*bufio.Reader
}

type Config struct {
	File string
	Pair map[string]string
}

func NewConfig(file string) *Config {
	return &Config{
		File: file,
		Pair: make(map[string]string),
	}
}

func (c *Config) Marshal() {

	file, err := os.Open(c.File)
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
		c.Pair[pair[0]] = pair[len(pair)-1]
	}
}

func (p *Parser) GetLength() (int, error) {
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
		bufio.NewReader(rdr),
	}
}

func (p *Parser) ParseBulkString() ([]byte, error) {
	length, err := p.GetLength()
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
	length, err := p.GetLength()
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
		fmt.Println("no first byte")
		return nil, err
	}
	fmt.Println(string(iden))
	res := []string{}
	switch string(iden) {

	case "*":
		res, err = p.ParseArray()
		if err != nil {
			return nil, err
		}
	case "$":
		buff, err := p.ParseBulkString()
		if err != nil {
			return nil, err
		}
		fmt.Println(string(buff))
	default:
		fmt.Printf("got %s\n", string(iden))
	}
	return res, nil
}

func ToBulkString(ele string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(ele), ele)
}
func ToArray(arr []string) []byte {
	sb := strings.Builder{}
	tmp := fmt.Sprintf("*%d\r\n", len(arr))
	sb.WriteString(tmp)
	for _, ele := range arr {
		sb.WriteString(ToBulkString(ele))
	}
	return []byte(sb.String())
}
