package main

import (
	"fmt"
	"github.com/THUIGinX/IGinX-client-go/client_v2"
	"github.com/THUIGinX/IGinX-client-go/rpc"
	"log"
)

var (
	session *client_v2.Session
)

const (
	s1 = "test.go.a"
	s2 = "test.go.b"
	s3 = "test.go.c"
	s4 = "test.go.d"
	s5 = "test.go.e"
	s6 = "test.go.f"
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

	insertColumnData()
}

func insertColumnData() {
	fmt.Println("insertColumnData")
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
	fmt.Println()
}
