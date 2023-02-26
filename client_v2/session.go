package client_v2

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/iznauy/IGinX-client-go/rpc"
	"github.com/pkg/errors"
	"log"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	SuccessCode = 200
)

var (
	ErrNetwork   = errors.New("Network Error")
	ErrExecution = errors.New("Execution Error")
)

var logger = log.Default()

type Session struct {
	settings     *SessionSettings
	currEndPoint *EndPoint

	isClose   bool
	client    *Client
	sessionId int64
	transport thrift.TTransport

	mu sync.Mutex
}

type Client struct {
	client *rpc.IServiceClient
	mu     sync.Mutex
}

func NewClient(client *rpc.IServiceClient) *Client {
	return &Client{
		client: client,
	}
}

func (c *Client) OpenSession(ctx context.Context, req *rpc.OpenSessionReq) (*rpc.OpenSessionResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client.OpenSession(ctx, req)
}

func (c *Client) CloseSession(ctx context.Context, req *rpc.CloseSessionReq) (*rpc.Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client.CloseSession(ctx, req)
}

func (c *Client) GetReplicaNum(ctx context.Context, req *rpc.GetReplicaNumReq) (*rpc.GetReplicaNumResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp, err := c.client.GetReplicaNum(ctx, req)
	if err != nil {
		return nil, errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ExecuteStatement(ctx context.Context, req *rpc.ExecuteStatementReq) (*rpc.ExecuteStatementResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp, err := c.client.ExecuteStatement(ctx, req)
	if err != nil {
		return nil, errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) FetchResults(ctx context.Context, req *rpc.FetchResultsReq) (*rpc.FetchResultsResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp, err := c.client.FetchResults(ctx, req)
	if err != nil {
		return nil, errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) CloseStatement(ctx context.Context, req *rpc.CloseStatementReq) (*rpc.Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	status, err := c.client.CloseStatement(ctx, req)
	if err != nil {
		return nil, errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(status); err != nil {
		return nil, err
	}
	return status, nil
}

func (c *Client) InsertColumnRecords(ctx context.Context, req *rpc.InsertColumnRecordsReq) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	status, err := c.client.InsertColumnRecords(ctx, req)
	if err != nil {
		return errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(status); err != nil {
		return err
	}
	return nil
}

func (c *Client) InsertNonAlignedColumnRecords(ctx context.Context, req *rpc.InsertNonAlignedColumnRecordsReq) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	status, err := c.client.InsertNonAlignedColumnRecords(ctx, req)
	if err != nil {
		return errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(status); err != nil {
		return err
	}
	return nil
}

func NewSession(settings *SessionSettings) *Session {
	return &Session{
		settings:     settings,
		currEndPoint: nil,
		isClose:      true,
		sessionId:    -1,
		client:       nil,
		transport:    nil,
	}
}

func (s *Session) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isClose {
		return nil
	}

	flag := false
	for i := 0; i < s.settings.MaxSwitchTimes; i++ {
		if !s.settings.HasMoreEndPoint() {
			break
		}
		endPoint := s.settings.GetEndPoint()

		s.currEndPoint = endPoint

		err := s.open(endPoint)
		if errors.Cause(err) == ErrExecution {
			return err
		}
		if errors.Cause(err) == ErrNetwork {
			logger.Println("connect to "+endPoint.Ip+":"+strconv.Itoa(int(endPoint.Port))+" error", err)
			continue
		}
		flag = true
		break
	}
	if !flag {
		log.Println("No IGinX available, please check network")
		return ErrNetwork
	}

	s.settings.ResetEndPoints()
	if s.settings.EnableHighAvailable && s.settings.AutoLoadAvailableList {
		// TODO：定期加载可用的 IGinX 列表
	}
	return nil
}

func (s *Session) open(endPoint *EndPoint) error {
	if !s.isClose {
		return nil
	}
	s.transport = thrift.NewTSocketConf(net.JoinHostPort(endPoint.Ip, strconv.Itoa(int(endPoint.Port))), &thrift.TConfiguration{
		ConnectTimeout: s.settings.ConnectionTimeout,
		SocketTimeout:  s.settings.SocketTimeout,
	})

	if !s.transport.IsOpen() {
		err := s.transport.Open()
		if err != nil {
			return errors.Wrap(ErrNetwork, err.Error())
		}
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(s.transport)
	oprot := protocolFactory.GetProtocol(s.transport)
	client := rpc.NewIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := rpc.OpenSessionReq{
		Username: &s.settings.Username,
		Password: &s.settings.Password,
	}

	resp, err := client.OpenSession(context.Background(), &req)
	if err != nil {
		return errors.Wrap(ErrNetwork, err.Error())
	}
	if err = verifyStatus(resp.Status); err != nil {
		return err
	}

	s.sessionId = resp.GetSessionId()
	s.isClose = false

	s.client = NewClient(client)
	return nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close()
}

func (s *Session) close() error {
	if s.isClose {
		return nil
	}

	req := rpc.CloseSessionReq{
		SessionId: s.sessionId,
	}

	defer func() {
		s.isClose = true
		if s.transport.IsOpen() {
			_ = s.transport.Close()
		}
		if s.settings.AutoLoadAvailableList {
			// TODO: 清理自动加载可用列表使用的资源
		}
	}()

	status, err := s.client.CloseSession(context.Background(), &req)
	if err != nil {
		return err
	}

	err = verifyStatus(status)
	if err != nil {
		return errors.Wrap(ErrNetwork, err.Error())
	}

	return nil
}

func (s *Session) reconnect() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	flag := false
	for i := 0; i < s.settings.MaxRetryTimes; i++ {
		var err error
		if s.transport != nil {
			_ = s.close()
			if err = s.open(s.currEndPoint); err == nil {
				flag = true
				break
			}
		}
		if err != nil {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return flag
}

func (s *Session) GetReplicaNum() (int32, error) {
	req := rpc.GetReplicaNumReq{
		SessionId: s.sessionId,
	}
	resp, err := s.client.GetReplicaNum(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return 0, err
	}
	if errors.Cause(err) == ErrNetwork {
		reconnect := s.reconnect()
		if !reconnect && s.settings.EnableHighAvailable {
			if err := s.Open(); err == nil {
				reconnect = true
			} else {
				log.Println("switch to other IGinX failure: " + err.Error())
			}
		}
		if reconnect {
			req.SessionId = s.sessionId
			resp, err = s.client.GetReplicaNum(context.Background(), &req)
		}
	}
	if err != nil {
		return 0, err
	}
	return resp.GetReplicaNum(), nil
}

func (s *Session) ExecuteQuery(statement string, fetchSize int32) (*IGinXStream, error) {
	req := rpc.ExecuteStatementReq{
		SessionId: s.sessionId,
		Statement: statement,
	}
	resp, err := s.client.ExecuteStatement(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return nil, err
	}
	if errors.Cause(err) == ErrNetwork {
		reconnect := s.reconnect()
		if !reconnect && s.settings.EnableHighAvailable {
			if err := s.Open(); err == nil {
				reconnect = true
			} else {
				log.Println("switch to other IGinX failure: " + err.Error())
			}
		}
		if reconnect {
			req.SessionId = s.sessionId
			resp, err = s.client.ExecuteStatement(context.Background(), &req)
		}
	}
	if err != nil {
		return nil, err
	}
	return NewIGinXStream(s, resp.GetQueryId(), fetchSize), nil
}

func (s *Session) fetchResult(queryId int64, position int64, fetchSize int32) (*rpc.FetchResultsResp, error) {
	req := rpc.FetchResultsReq{
		SessionId: s.sessionId,
		QueryId:   queryId,
		Position:  position,
		FetchSize: &fetchSize,
	}
	resp, err := s.client.FetchResults(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return nil, err
	}
	if errors.Cause(err) == ErrNetwork {
		if reconnect := s.reconnect(); reconnect {
			resp, err = s.client.FetchResults(context.Background(), &req)
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Session) closeQuery(queryId int64) error {
	req := rpc.CloseStatementReq{
		SessionId: s.sessionId,
		QueryId:   queryId,
	}
	_, err := s.client.CloseStatement(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return err
	}
	if errors.Cause(err) == ErrNetwork {
		if reconnect := s.reconnect(); reconnect {
			_, err = s.client.CloseStatement(context.Background(), &req)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) ReExecuteQuery(statement string, fetchSize int32, queryId int64) (*IGinXStream, error) {
	req := rpc.ExecuteStatementReq{
		SessionId: s.sessionId,
		Statement: statement,
		QueryId:   Int64Ptr(queryId),
	}
	resp, err := s.client.ExecuteStatement(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return nil, err
	}
	if errors.Cause(err) == ErrNetwork {
		reconnect := s.reconnect()
		if !reconnect && s.settings.EnableHighAvailable {
			if err := s.Open(); err == nil {
				reconnect = true
			} else {
				log.Println("switch to other IGinX failure: " + err.Error())
			}
		}
		if reconnect {
			req.SessionId = s.sessionId
			resp, err = s.client.ExecuteStatement(context.Background(), &req)
		}
	}
	if err != nil {
		return nil, err
	}
	return NewIGinXStream(s, resp.GetQueryId(), fetchSize), nil
}

func (s *Session) InsertColumnRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType, tagsList []map[string]string) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if tagsList != nil && len(paths) != len(tagsList) {
		return errors.New("the sizes of paths and tagsList should be equal")
	}
	if len(paths) != len(valueList) {
		return errors.New("the sizes of paths and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	for i := range valueList {
		var values []interface{}
		for j := range timestamps {
			values = append(values, valueList[i][timeIndex[j]])
		}
		valueList[i] = values
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据和数据类型
	var sortedValueList [][]interface{}
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedValueList = append(sortedValueList, valueList[pathIndex[i]])
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 重排tagKV
	var sortedTagsList []map[string]string
	if tagsList != nil {
		for i := range pathIndex {
			sortedTagsList = append(sortedTagsList, tagsList[pathIndex[i]])
		}
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := ColumnValuesToBytes(rowValues, sortedDataTypeList[i])
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertColumnRecordsReq{
		SessionId:    s.sessionId,
		Paths:        paths,
		Timestamps:   timeBytes,
		ValuesList:   valueBufferList,
		BitmapList:   bitmapBufferList,
		DataTypeList: sortedDataTypeList,
		TagsList:     sortedTagsList,
	}

	err = s.client.InsertColumnRecords(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return err
	}
	if errors.Cause(err) == ErrNetwork {
		reconnect := s.reconnect()
		if !reconnect && s.settings.EnableHighAvailable {
			if err := s.Open(); err == nil {
				reconnect = true
			} else {
				log.Println("switch to other IGinX failure: " + err.Error())
			}
		}
		if reconnect {
			req.SessionId = s.sessionId
			err = s.client.InsertColumnRecords(context.Background(), &req)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Session) InsertNonAlignedColumnRecords(paths []string, timestamps []int64, valueList [][]interface{}, dataTypeList []rpc.DataType, tagsList []map[string]string) error {
	if paths == nil || timestamps == nil || valueList == nil || dataTypeList == nil ||
		len(paths) == 0 || len(timestamps) == 0 || len(valueList) == 0 || len(dataTypeList) == 0 {
		return errors.New("invalid insert request")
	}
	if len(paths) != len(dataTypeList) {
		return errors.New("the sizes of paths and dataTypeList should be equal")
	}
	if tagsList != nil && len(paths) != len(tagsList) {
		return errors.New("the sizes of paths and tagsList should be equal")
	}
	if len(paths) != len(valueList) {
		return errors.New("the sizes of paths and valuesList should be equal")
	}

	// 保证时间戳递增
	timeIndex := make([]int, len(timestamps))
	for i := range timestamps {
		timeIndex[i] = i
	}
	sort.Slice(timeIndex, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})
	for i := range valueList {
		var values []interface{}
		for j := range timestamps {
			values = append(values, valueList[i][timeIndex[j]])
		}
		valueList[i] = values
	}
	// 保证序列递增
	pathIndex := make([]int, len(paths))
	for i := range paths {
		pathIndex[i] = i
	}
	sort.Slice(pathIndex, func(i, j int) bool {
		return paths[i] < paths[j]
	})
	sort.Strings(paths)
	// 重排数据和数据类型
	var sortedValueList [][]interface{}
	var sortedDataTypeList []rpc.DataType
	for i := range pathIndex {
		sortedValueList = append(sortedValueList, valueList[pathIndex[i]])
		sortedDataTypeList = append(sortedDataTypeList, dataTypeList[pathIndex[i]])
	}
	// 重排tagKV
	var sortedTagsList []map[string]string
	if tagsList != nil {
		for i := range pathIndex {
			sortedTagsList = append(sortedTagsList, tagsList[pathIndex[i]])
		}
	}
	// 压缩数据
	var valueBufferList, bitmapBufferList [][]byte
	for i := range sortedValueList {
		rowValues := sortedValueList[i]
		rowValuesBuffer, err := ColumnValuesToBytes(rowValues, sortedDataTypeList[i])
		if err != nil {
			return err
		}
		valueBufferList = append(valueBufferList, rowValuesBuffer)
		bitmap := NewBitmap(len(rowValues))
		for j := range rowValues {
			if rowValues[j] != nil {
				err = bitmap.Mark(j)
				if err != nil {
					return err
				}
			}
		}
		bitmapBufferList = append(bitmapBufferList, bitmap.GetBitmap())
	}
	timeBytes, err := TimestampsToBytes(timestamps)
	if err != nil {
		return err
	}

	req := rpc.InsertColumnRecordsReq{
		SessionId:    s.sessionId,
		Paths:        paths,
		Timestamps:   timeBytes,
		ValuesList:   valueBufferList,
		BitmapList:   bitmapBufferList,
		DataTypeList: sortedDataTypeList,
		TagsList:     sortedTagsList,
	}

	err = s.client.InsertColumnRecords(context.Background(), &req)
	if errors.Cause(err) == ErrExecution {
		return err
	}
	if errors.Cause(err) == ErrNetwork {
		reconnect := s.reconnect()
		if !reconnect && s.settings.EnableHighAvailable {
			if err := s.Open(); err == nil {
				reconnect = true
			} else {
				log.Println("switch to other IGinX failure: " + err.Error())
			}
		}
		if reconnect {
			req.SessionId = s.sessionId
			err = s.client.InsertNonAlignedColumnRecords(context.Background(), &req)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func verifyStatus(status *rpc.Status) error {
	if status.GetCode() != SuccessCode {
		return errors.Wrap(ErrExecution, status.GetMessage())
	}
	return nil
}
