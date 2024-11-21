package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Database struct {
	Index       int
	Size        int
	Expiry      int
	DbStore     map[string]string
	ExpiryStore []expiryEntry
}

type expiryEntry struct {
	Key    string
	Value  string
	Expiry uint64
}

type RDB struct {
	reader  *bufio.Reader
	version int
	aux     map[string]string
	Dbs     []Database
}

func NewFromBytes(content []byte) (*RDB, error) {
	fmt.Println(string(content))
	file, _ := os.Create("dump.rdb")
	file.Write(content)
	rdb, err := NewRDB("dump.rdb")
	if err != nil {
		return nil, err
	}
	return rdb, nil
}

func NewRDB(file string) (*RDB, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return &RDB{
		reader: bufio.NewReader(fd),
		aux:    make(map[string]string),
		Dbs:    make([]Database, 0),
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
		Index:       dbIdx,
		DbStore:     make(map[string]string),
		ExpiryStore: []expiryEntry{},
	}
	rdb.Dbs = append(rdb.Dbs, db)

	rdb.reader.ReadByte() // read FB

	dbLen, _, err := rdb.ParseLength()
	if err != nil {
		return err
	}
	dbExp, _, err := rdb.ParseLength()
	if err != nil {
		return err
	}

	rdb.Dbs[dbIdx].Size = dbLen
	rdb.Dbs[dbIdx].Expiry = dbExp
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
			rdb.Dbs[dbIdx].ExpiryStore = append(rdb.Dbs[dbIdx].ExpiryStore, expiryEntry{
				Key:    keyval[0],
				Value:  keyval[1],
				Expiry: expiry,
			})
		} else {
			rdb.Dbs[dbIdx].DbStore[keyval[0]] = keyval[1]
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
