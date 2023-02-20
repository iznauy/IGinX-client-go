package main

import (
	"fmt"
	"github.com/iznauy/IGinX-client-go/client_v2"
	"github.com/iznauy/IGinX-client-go/rpc"
	"log"
)

var (
	session *client_v2.Session
)

const (
	s1 = "test.gs.a"
	s2 = "test.gs.b"
	s3 = "test.gs.c"
	s4 = "test.gs.d"
	s5 = "test.gs.e"
	s6 = "test.gs.f"
)

func init() {
	settings, err := client_v2.NewSessionSettings("127.0.0.1:6887,127.0.0.1:6888")
	if err != nil {
		panic(err)
	}
	session = client_v2.NewSession(settings)
	if err := session.Open(); err != nil {
		panic(err)
	}
}

func main() {
	defer func() {
		if err := session.Close(); err != nil {
			fmt.Println("close session error: " + err.Error())
		}
	}()

	replicaNum, err := session.GetReplicaNum()
	if err != nil {
		panic(err)
	}
	fmt.Printf("replica number: %d\n", replicaNum)

	writeData()
	queryData()
}

func queryData() {
	fmt.Println("query data: start")
	cursor, err := session.ExecuteQuery("select * from test.gs", 1)
	if err != nil {
		log.Fatal(err)
	}
	fields, err := cursor.GetFields()
	if err != nil {
		log.Fatal(err)
	}
	for _, field := range fields {
		fmt.Printf("field: %+v\n", field)
	}
	for {
		hasMore, err := cursor.HasMore()
		if err != nil {
			log.Fatal(err)
		}
		if !hasMore {
			break
		}
		values, err := cursor.NextRow()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("values: %+v\n", values)
	}
	if err := cursor.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("query data: finish")
}

func writeData() {
	fmt.Println("write data: start")
	path := []string{s1, s2, s3, s4, s5, s6}
	timestamps := []int64{10, 11}
	values := [][]interface{}{
		{"ten", "eleven"},
		{int32(10), int32(11)},
		{int64(10), int64(11)},
		{float32(10.1), float32(11.1)},
		{10.1, 11.1},
		{false, true},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	err := session.InsertColumnRecords(path, timestamps, values, types, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("write data: finish")
}
