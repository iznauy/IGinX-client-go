package client

import (
	"fmt"
	"github.com/THUIGinX/IGinX-client-go/rpc"
)

type QueryDataSet struct {
	Paths      []string
	Types      []rpc.DataType
	Timestamps []int64
	Values     [][]interface{}
}

func NewQueryDataSet(paths []string, types []rpc.DataType, timeBuffer []byte, valuesList, bitmapList [][]byte) *QueryDataSet {
	var values [][]interface{}
	for i := range valuesList {
		var rowValues []interface{}
		valuesBuffer := valuesList[i]
		bitmapBuffer := bitmapList[i]

		bitmap := NewBitmapWithBuf(len(types), bitmapBuffer)
		for j := range types {
			if notNil, _ := bitmap.Get(j); notNil {
				var value interface{}
				value, valuesBuffer = GetValueFromBytes(valuesBuffer, types[j])
				rowValues = append(rowValues, value)
			} else {
				rowValues = append(rowValues, nil)
			}
		}
		values = append(values, rowValues)
	}

	return &QueryDataSet{
		Paths:      paths,
		Types:      types,
		Timestamps: GetLongArrayFromBytes(timeBuffer),
		Values:     values,
	}
}

func (s *QueryDataSet) PrintDataSet() {
	fmt.Println("Start print data set")
	fmt.Println("-------------------------------------")
	if len(s.Timestamps) != 0 {
		fmt.Print("Time ")
	}
	for i := range s.Paths {
		fmt.Print(s.Paths[i], " ")
	}
	fmt.Println()
	for i := range s.Values {
		if len(s.Timestamps) != 0 && i < len(s.Timestamps) {
			fmt.Print(s.Timestamps[i], " ")
		}
		for j := range s.Values[i] {
			fmt.Print(s.Values[i][j], " ")
		}
		fmt.Println()
	}
	fmt.Println("-------------------------------------")
	fmt.Println()
}

type AggregateQueryDataSet struct {
	Paths         []string
	AggregateType rpc.AggregateType
	Timestamps    []int64
	Values        []interface{}
}

func NewAggregateQueryDataSet(paths []string, timeBuffer, valuesBuffer []byte, types []rpc.DataType, aggregateType rpc.AggregateType) *AggregateQueryDataSet {
	dataSet := AggregateQueryDataSet{
		Paths:         paths,
		AggregateType: aggregateType,
		Values:        GetValueByDataTypeList(valuesBuffer, types),
	}

	if timeBuffer != nil {
		dataSet.Timestamps = GetLongArrayFromBytes(timeBuffer)
	}

	return &dataSet
}

func (s *AggregateQueryDataSet) PrintDataSet() {
	fmt.Println("Start print aggregate data set")
	fmt.Println("-------------------------------------")

	for _, path := range s.Paths {
		fmt.Print(s.AggregateType.String()+"("+path+")", " ")
	}
	fmt.Println()
	for _, value := range s.Values {
		fmt.Print(value, " ")
	}
	fmt.Println()

	fmt.Println("-------------------------------------")
	fmt.Println()
}

type SQLDataSet struct {
	Type          rpc.SqlType
	ParseErrorMsg string

	QueryDataSet *QueryDataSet

	TimeSeries  []*TimeSeries
	ReplicaNum  int32
	PointsNum   int64
	ClusterInfo *ClusterInfo
}

func NewSQLDataSet(resp *rpc.ExecuteSqlResp) *SQLDataSet {
	dataSet := &SQLDataSet{
		Type:          resp.GetType(),
		ParseErrorMsg: resp.GetParseErrorMsg(),
	}

	switch dataSet.Type {
	case rpc.SqlType_GetReplicaNum:
		dataSet.ReplicaNum = resp.GetReplicaNum()
		break
	case rpc.SqlType_CountPoints:
		dataSet.PointsNum = resp.GetPointsNum()
		break
	case rpc.SqlType_ShowTimeSeries:
		var timeSeries []*TimeSeries
		for i := 0; i < len(resp.GetPaths()); i++ {
			ts := NewTimeSeries(resp.GetPaths()[i], resp.GetDataTypeList()[i])
			timeSeries = append(timeSeries, &ts)
		}
		dataSet.TimeSeries = timeSeries
		break
	case rpc.SqlType_ShowClusterInfo:
		dataSet.ClusterInfo = NewClusterInfo(
			resp.GetIginxInfos(),
			resp.GetStorageEngineInfos(),
			resp.GetMetaStorageInfos(),
			resp.GetLocalMetaStorageInfo(),
		)
		break
	case rpc.SqlType_Query:
		dataSet.QueryDataSet = NewQueryDataSet(
			resp.GetPaths(),
			resp.GetDataTypeList(),
			resp.GetQueryDataSet().GetTimestamps(),
			resp.GetQueryDataSet().GetValuesList(),
			resp.GetQueryDataSet().GetBitmapList(),
		)
		break
	}

	return dataSet
}

func (s *SQLDataSet) GetParseErrorMsg() string {
	return s.ParseErrorMsg
}

func (s *SQLDataSet) IsQuery() bool {
	return s.Type == rpc.SqlType_Query
}

func (s *SQLDataSet) GetReplicaNum() int32 {
	return s.ReplicaNum
}

func (s *SQLDataSet) GetPointsNum() int64 {
	return s.PointsNum
}

func (s *SQLDataSet) GetTimeSeries() []*TimeSeries {
	return s.TimeSeries
}

func (s *SQLDataSet) GetClusterInfo() *ClusterInfo {
	return s.ClusterInfo
}

func (s *SQLDataSet) GetQueryDataSet() *QueryDataSet {
	return s.QueryDataSet
}
