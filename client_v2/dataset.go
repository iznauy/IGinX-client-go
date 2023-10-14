package client_v2

import (
	"github.com/iznauy/IGinX-client-go/rpc"
)

type StateType int32

const (
	HasMore StateType = 0
	NoMore  StateType = 1
	Unknown StateType = 2
)

type Field struct {
	name     string
	tags     map[string]string
	dataType rpc.DataType
}

type IGinXStream struct {
	session   *Session
	queryId   int64
	fetchSize int32

	hasInit    bool
	fields     []Field
	valuesList [][]byte
	bitmapList [][]byte
	index      int
	position   int64
	state      StateType
}

func NewIGinXStream(session *Session, queryId int64, fetchSize int32) *IGinXStream {
	return &IGinXStream{
		session:   session,
		queryId:   queryId,
		fetchSize: fetchSize,

		hasInit:  false,
		index:    0,
		position: 0,
		state:    Unknown,
	}
}

func (s *IGinXStream) fetch() error {
	if s.bitmapList != nil && s.index != len(s.bitmapList) {
		return nil
	}
	s.bitmapList = nil
	s.valuesList = nil
	s.index = 0

	resp, err := s.session.fetchResult(s.queryId, s.position, s.fetchSize)
	if err != nil {
		//log.Printf("fail to fetch stream data, err: %s\n", err)
		return err
	}
	s.state = NoMore
	if resp.HasMoreResults {
		s.state = HasMore
	}
	if resp.GetQueryDataSet() == nil {
		return nil
	}

	s.bitmapList = resp.GetQueryDataSet().GetBitmapList()
	s.valuesList = resp.GetQueryDataSet().GetValuesList()
	s.position += int64(len(resp.GetQueryDataSet().GetValuesList()))

	if !s.hasInit {
		s.hasInit = true
		s.fields = make([]Field, 0, len(resp.Columns))
		for i := 0; i < len(resp.Columns); i++ {
			s.fields = append(s.fields, Field{
				name:     resp.Columns[i],
				dataType: resp.DataTypeList[i],
			})
			if len(resp.TagsList) != 0 {
				s.fields[i].tags = resp.TagsList[i]
			}
		}
	}
	return nil
}

func (s *IGinXStream) HasMore() (bool, error) {
	if s.valuesList != nil && s.index < len(s.valuesList) {
		return true, nil
	}
	s.bitmapList = nil
	s.valuesList = nil
	s.index = 0
	if s.state == HasMore || s.state == Unknown {
		if err := s.fetch(); err != nil {
			return false, err
		}
	}
	return s.valuesList != nil, nil
}

func (s *IGinXStream) NextRow() ([]interface{}, error) {
	hasMore, err := s.HasMore()
	if !hasMore {
		return nil, err
	}
	// nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
	bitmapBuffer := s.bitmapList[s.index]
	valuesBuffer := s.valuesList[s.index]
	s.index++

	var rowValues []interface{}
	bitmap := NewBitmapWithBuf(len(s.fields), bitmapBuffer)
	for i := range s.fields {
		if notNil, _ := bitmap.Get(i); notNil {
			var value interface{}
			value, valuesBuffer = GetValueFromBytes(valuesBuffer, s.fields[i].dataType)
			rowValues = append(rowValues, value)
		} else {
			rowValues = append(rowValues, nil)
		}
	}
	return rowValues, nil
}

func (s *IGinXStream) GetFields() ([]Field, error) {
	if !s.hasInit {
		if err := s.fetch(); err != nil {
			return nil, err
		}
	}
	return s.fields, nil
}

func (s *IGinXStream) Close() error {
	err := s.session.closeQuery(s.queryId)
	s.fields = nil
	s.valuesList = nil
	s.bitmapList = nil
	s.session = nil
	return err
}

func (s *IGinXStream) QueryId() int64 {
	return s.queryId
}
