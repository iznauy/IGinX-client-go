package main

import (
	"fmt"
	"log"
	"math"

	"github.com/iznauy/IGinX-client-go/client"
	"github.com/iznauy/IGinX-client-go/rpc"
)

var (
	session *client.Session

	s1 = "test.go.a"
	s2 = "test.go.b"
	s3 = "test.go.c"
	s4 = "test.go.d"
	s5 = "test.go.e"
	s6 = "test.go.f"

	tagS1 = "test.tag.a"
	tagS2 = "test.tag.b"
	tagS3 = "test.tag.c"
	tagS4 = "test.tag.d"
	tagS5 = "test.tag.e"
	tagS6 = "test.tag.f"

	allPaths    = "test.go.*"
	allTagPaths = "test.tag.*"
)

func main() {
	session = client.NewSession("127.0.0.1", "6888", "root", "root")

	if err := session.Open(); err != nil {
		log.Fatal(err)
	}

	defer session.Close()

	// 清空数据
	clearData()

	// 查询副本数量
	showReplicaNum()
	// 查询集群信息
	showClusterInfo()

	// 四种插入数据的方式
	insertRowData()
	insertNonAlignedRowRecords()
	insertColumnData()
	insertNonAlignedColumnRecords()

	// 插入tagKV数据
	insertTagKVRowData()

	// 查询时间序列
	showTimeSeries()

	// 查询全部数据
	queryAllData()
	// 降采样查询
	downSampleQuery()
	// 聚合查询
	aggregateQuery()
	// last 查询
	lastQuery()
	// tagKV 查询
	simpleQueryWithTagKV()

	// 删除部分数据
	deleteData()
	// 查询全部数据
	queryAllData()
	// 删除时间序列
	deleteTimeSeries()
	// 查询时间序列
	showTimeSeries()

	// 清空数据
	clearData()
	// 查询时间序列
	showTimeSeries()
}

func showReplicaNum() {
	num, err := session.GetReplicaNum()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replica num: %d\n\n", num)
}

func showClusterInfo() {
	fmt.Println("showClusterInfo")
	info, err := session.GetClusterInfo()
	if err != nil {
		log.Fatal(err)
	}
	info.PrintInfo()
	fmt.Println()
}

func insertRowData() {
	fmt.Println("insertRowData")
	path := []string{s1, s2, s3, s4, s5, s6}
	timestamps := []int64{1, 2, 3, 4, 5, 6, 7}
	values := [][]interface{}{
		{"one", int32(1), int64(1), float32(1.1), float64(1.1), true},
		{"two", int32(2), int64(2), float32(2.1), float64(2.1), false},
		{"three", nil, int64(3), float32(3.1), float64(3.1), true},
		{"four", int32(4), nil, float32(4.1), float64(4.1), false},
		{"five", int32(5), int64(5), nil, float64(5.1), true},
		{"six", int32(6), int64(6), float32(6.1), nil, false},
		{"seven", int32(7), int64(7), float32(7.1), float64(7.1), nil},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	err := session.InsertRowRecords(path, timestamps, values, types, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func insertNonAlignedRowRecords() {
	fmt.Println("insertNonAlignedRowRecords")
	path := []string{s1, s2, s3, s4, s5, s6}
	timestamps := []int64{8, 9}
	values := [][]interface{}{
		{"eight", int32(8), int64(8), float32(8.1), float64(8.1), false},
		{"nine", int32(9), int64(9), float32(9.1), float64(9.1), true},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	err := session.InsertNonAlignedRowRecords(path, timestamps, values, types, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
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
		{float64(10.1), float64(11.1)},
		{false, true},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	err := session.InsertColumnRecords(path, timestamps, values, types, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func insertNonAlignedColumnRecords() {
	fmt.Println("insertNonAlignedColumnRecords")
	paths := []string{s1, s2, s3, s4, s5, s6}
	timestamps := []int64{12, 13}
	values := [][]interface{}{
		{"twelve", "thirteen"},
		{int32(12), int32(13)},
		{int64(12), int64(13)},
		{float32(12.1), float32(13.1)},
		{float64(12.1), float64(13.1)},
		{false, true},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	err := session.InsertNonAlignedColumnRecords(paths, timestamps, values, types, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func insertTagKVRowData() {
	fmt.Println("insertTagKVRowData")
	path := []string{tagS1, tagS2, tagS3, tagS4, tagS5, tagS6}
	timestamps := []int64{14, 15, 16}
	values := [][]interface{}{
		{"fourteen", int32(14), int64(14), float32(14.1), float64(14.1), false},
		{"fifteen", int32(15), int64(15), float32(15.1), float64(15.1), true},
		{"sixteen", int32(16), int64(16), float32(16.1), float64(16.1), false},
	}
	types := []rpc.DataType{rpc.DataType_BINARY, rpc.DataType_INTEGER, rpc.DataType_LONG, rpc.DataType_FLOAT, rpc.DataType_DOUBLE, rpc.DataType_BOOLEAN}
	tagsList := []map[string]string{
		{"k1": "v1", "k2": "v1"},
		{"k1": "v2", "k2": "v2"},
		{"k1": "v3", "k2": "v3"},
		{"k1": "v4", "k2": "v4"},
		{"k1": "v5", "k2": "v5"},
		{"k1": "v6", "k2": "v6"},
	}

	err := session.InsertRowRecords(path, timestamps, values, types, tagsList)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func showTimeSeries() {
	fmt.Println("show time series:")
	tsList, err := session.ListTimeSeries()
	if err != nil {
		log.Fatal(err)
	}
	for _, ts := range tsList {
		fmt.Println(ts.ToString())
	}
	fmt.Println()
}

func queryAllData() {
	fmt.Println("query all data:")
	paths := []string{allPaths}
	dataSet, err := session.Query(paths, 0, math.MaxInt64, nil)
	if err != nil {
		log.Fatal(err)
	}
	dataSet.PrintDataSet()
}

func downSampleQuery() {
	fmt.Println("downSample query:")
	paths := []string{s2, s5}
	dataSet, err := session.DownSampleQuery(paths, 0, 10, rpc.AggregateType_MAX, 5, nil)
	if err != nil {
		log.Fatal(err)
	}
	dataSet.PrintDataSet()
}

func aggregateQuery() {
	fmt.Println("aggregate query:")
	paths := []string{s1, s2}
	dataSet, err := session.AggregateQuery(paths, 0, 10, rpc.AggregateType_MAX, nil)
	if err != nil {
		log.Fatal(err)
	}
	dataSet.PrintDataSet()
}

func lastQuery() {
	fmt.Println("last query:")
	paths := []string{s1, s2, s3}
	dataSet, err := session.LastQuery(paths, 5, nil)
	if err != nil {
		log.Fatal(err)
	}
	dataSet.PrintDataSet()
}

func simpleQueryWithTagKV() {
	fmt.Println("query with tagKV:")
	paths := []string{allTagPaths}
	tagList := map[string][]string{
		"k1": {"v1", "v3"},
		"k2": {"v1"},
	}
	dataSet, err := session.Query(paths, 0, math.MaxInt64, tagList)
	if err != nil {
		log.Fatal(err)
	}
	dataSet.PrintDataSet()
}

func deleteData() {
	fmt.Println("deleteData")
	paths := []string{allPaths}
	err := session.BatchDeleteData(paths, 10, 15, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func deleteTimeSeries() {
	fmt.Println("deleteTimeSeries")
	err := session.DeleteTimeSeries(s6)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}

func clearData() {
	fmt.Println("clearData")
	paths := []string{allPaths, allTagPaths}
	err := session.BatchDeleteTimeSeries(paths)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println()
}
