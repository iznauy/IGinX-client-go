// Code generated by Thrift Compiler (0.16.0). DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	thrift "github.com/apache/thrift/lib/go/thrift"
	"rpc"
)

var _ = rpc.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  OpenSessionResp openSession(OpenSessionReq req)")
  fmt.Fprintln(os.Stderr, "  Status closeSession(CloseSessionReq req)")
  fmt.Fprintln(os.Stderr, "  Status deleteColumns(DeleteColumnsReq req)")
  fmt.Fprintln(os.Stderr, "  Status insertColumnRecords(InsertColumnRecordsReq req)")
  fmt.Fprintln(os.Stderr, "  Status insertNonAlignedColumnRecords(InsertNonAlignedColumnRecordsReq req)")
  fmt.Fprintln(os.Stderr, "  Status insertRowRecords(InsertRowRecordsReq req)")
  fmt.Fprintln(os.Stderr, "  Status insertNonAlignedRowRecords(InsertNonAlignedRowRecordsReq req)")
  fmt.Fprintln(os.Stderr, "  Status deleteDataInColumns(DeleteDataInColumnsReq req)")
  fmt.Fprintln(os.Stderr, "  QueryDataResp queryData(QueryDataReq req)")
  fmt.Fprintln(os.Stderr, "  Status addStorageEngines(AddStorageEnginesReq req)")
  fmt.Fprintln(os.Stderr, "  Status removeHistoryDataSource(RemoveHistoryDataSourceReq req)")
  fmt.Fprintln(os.Stderr, "  Status removeStorageEngine(RemoveStorageEngineReq req)")
  fmt.Fprintln(os.Stderr, "  AggregateQueryResp aggregateQuery(AggregateQueryReq req)")
  fmt.Fprintln(os.Stderr, "  LastQueryResp lastQuery(LastQueryReq req)")
  fmt.Fprintln(os.Stderr, "  DownsampleQueryResp downsampleQuery(DownsampleQueryReq req)")
  fmt.Fprintln(os.Stderr, "  ShowColumnsResp showColumns(ShowColumnsReq req)")
  fmt.Fprintln(os.Stderr, "  GetReplicaNumResp getReplicaNum(GetReplicaNumReq req)")
  fmt.Fprintln(os.Stderr, "  ExecuteSqlResp executeSql(ExecuteSqlReq req)")
  fmt.Fprintln(os.Stderr, "  Status updateUser(UpdateUserReq req)")
  fmt.Fprintln(os.Stderr, "  Status addUser(AddUserReq req)")
  fmt.Fprintln(os.Stderr, "  Status deleteUser(DeleteUserReq req)")
  fmt.Fprintln(os.Stderr, "  GetUserResp getUser(GetUserReq req)")
  fmt.Fprintln(os.Stderr, "  GetClusterInfoResp getClusterInfo(GetClusterInfoReq req)")
  fmt.Fprintln(os.Stderr, "  ExecuteStatementResp executeStatement(ExecuteStatementReq req)")
  fmt.Fprintln(os.Stderr, "  FetchResultsResp fetchResults(FetchResultsReq req)")
  fmt.Fprintln(os.Stderr, "  Status closeStatement(CloseStatementReq req)")
  fmt.Fprintln(os.Stderr, "  Status cancelStatement(CancelStatementReq req)")
  fmt.Fprintln(os.Stderr, "  CommitTransformJobResp commitTransformJob(CommitTransformJobReq req)")
  fmt.Fprintln(os.Stderr, "  QueryTransformJobStatusResp queryTransformJobStatus(QueryTransformJobStatusReq req)")
  fmt.Fprintln(os.Stderr, "  ShowEligibleJobResp showEligibleJob(ShowEligibleJobReq req)")
  fmt.Fprintln(os.Stderr, "  Status cancelTransformJob(CancelTransformJobReq req)")
  fmt.Fprintln(os.Stderr, "  Status registerTask(RegisterTaskReq req)")
  fmt.Fprintln(os.Stderr, "  Status dropTask(DropTaskReq req)")
  fmt.Fprintln(os.Stderr, "  GetRegisterTaskInfoResp getRegisterTaskInfo(GetRegisterTaskInfoReq req)")
  fmt.Fprintln(os.Stderr, "  CurveMatchResp curveMatch(CurveMatchReq req)")
  fmt.Fprintln(os.Stderr, "  DebugInfoResp debugInfo(DebugInfoReq req)")
  fmt.Fprintln(os.Stderr, "  LoadAvailableEndPointsResp loadAvailableEndPoints(LoadAvailableEndPointsReq req)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  var cfg *thrift.TConfiguration = nil
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), cfg)
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransportConf(trans, cfg)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactoryConf(cfg)
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(cfg)
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryConf(cfg)
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := rpc.NewIServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "openSession":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "OpenSession requires 1 args")
      flag.Usage()
    }
    arg369 := flag.Arg(1)
    mbTrans370 := thrift.NewTMemoryBufferLen(len(arg369))
    defer mbTrans370.Close()
    _, err371 := mbTrans370.WriteString(arg369)
    if err371 != nil {
      Usage()
      return
    }
    factory372 := thrift.NewTJSONProtocolFactory()
    jsProt373 := factory372.GetProtocol(mbTrans370)
    argvalue0 := rpc.NewOpenSessionReq()
    err374 := argvalue0.Read(context.Background(), jsProt373)
    if err374 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.OpenSession(context.Background(), value0))
    fmt.Print("\n")
    break
  case "closeSession":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CloseSession requires 1 args")
      flag.Usage()
    }
    arg375 := flag.Arg(1)
    mbTrans376 := thrift.NewTMemoryBufferLen(len(arg375))
    defer mbTrans376.Close()
    _, err377 := mbTrans376.WriteString(arg375)
    if err377 != nil {
      Usage()
      return
    }
    factory378 := thrift.NewTJSONProtocolFactory()
    jsProt379 := factory378.GetProtocol(mbTrans376)
    argvalue0 := rpc.NewCloseSessionReq()
    err380 := argvalue0.Read(context.Background(), jsProt379)
    if err380 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CloseSession(context.Background(), value0))
    fmt.Print("\n")
    break
  case "deleteColumns":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DeleteColumns requires 1 args")
      flag.Usage()
    }
    arg381 := flag.Arg(1)
    mbTrans382 := thrift.NewTMemoryBufferLen(len(arg381))
    defer mbTrans382.Close()
    _, err383 := mbTrans382.WriteString(arg381)
    if err383 != nil {
      Usage()
      return
    }
    factory384 := thrift.NewTJSONProtocolFactory()
    jsProt385 := factory384.GetProtocol(mbTrans382)
    argvalue0 := rpc.NewDeleteColumnsReq()
    err386 := argvalue0.Read(context.Background(), jsProt385)
    if err386 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DeleteColumns(context.Background(), value0))
    fmt.Print("\n")
    break
  case "insertColumnRecords":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "InsertColumnRecords requires 1 args")
      flag.Usage()
    }
    arg387 := flag.Arg(1)
    mbTrans388 := thrift.NewTMemoryBufferLen(len(arg387))
    defer mbTrans388.Close()
    _, err389 := mbTrans388.WriteString(arg387)
    if err389 != nil {
      Usage()
      return
    }
    factory390 := thrift.NewTJSONProtocolFactory()
    jsProt391 := factory390.GetProtocol(mbTrans388)
    argvalue0 := rpc.NewInsertColumnRecordsReq()
    err392 := argvalue0.Read(context.Background(), jsProt391)
    if err392 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.InsertColumnRecords(context.Background(), value0))
    fmt.Print("\n")
    break
  case "insertNonAlignedColumnRecords":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "InsertNonAlignedColumnRecords requires 1 args")
      flag.Usage()
    }
    arg393 := flag.Arg(1)
    mbTrans394 := thrift.NewTMemoryBufferLen(len(arg393))
    defer mbTrans394.Close()
    _, err395 := mbTrans394.WriteString(arg393)
    if err395 != nil {
      Usage()
      return
    }
    factory396 := thrift.NewTJSONProtocolFactory()
    jsProt397 := factory396.GetProtocol(mbTrans394)
    argvalue0 := rpc.NewInsertNonAlignedColumnRecordsReq()
    err398 := argvalue0.Read(context.Background(), jsProt397)
    if err398 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.InsertNonAlignedColumnRecords(context.Background(), value0))
    fmt.Print("\n")
    break
  case "insertRowRecords":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "InsertRowRecords requires 1 args")
      flag.Usage()
    }
    arg399 := flag.Arg(1)
    mbTrans400 := thrift.NewTMemoryBufferLen(len(arg399))
    defer mbTrans400.Close()
    _, err401 := mbTrans400.WriteString(arg399)
    if err401 != nil {
      Usage()
      return
    }
    factory402 := thrift.NewTJSONProtocolFactory()
    jsProt403 := factory402.GetProtocol(mbTrans400)
    argvalue0 := rpc.NewInsertRowRecordsReq()
    err404 := argvalue0.Read(context.Background(), jsProt403)
    if err404 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.InsertRowRecords(context.Background(), value0))
    fmt.Print("\n")
    break
  case "insertNonAlignedRowRecords":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "InsertNonAlignedRowRecords requires 1 args")
      flag.Usage()
    }
    arg405 := flag.Arg(1)
    mbTrans406 := thrift.NewTMemoryBufferLen(len(arg405))
    defer mbTrans406.Close()
    _, err407 := mbTrans406.WriteString(arg405)
    if err407 != nil {
      Usage()
      return
    }
    factory408 := thrift.NewTJSONProtocolFactory()
    jsProt409 := factory408.GetProtocol(mbTrans406)
    argvalue0 := rpc.NewInsertNonAlignedRowRecordsReq()
    err410 := argvalue0.Read(context.Background(), jsProt409)
    if err410 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.InsertNonAlignedRowRecords(context.Background(), value0))
    fmt.Print("\n")
    break
  case "deleteDataInColumns":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DeleteDataInColumns requires 1 args")
      flag.Usage()
    }
    arg411 := flag.Arg(1)
    mbTrans412 := thrift.NewTMemoryBufferLen(len(arg411))
    defer mbTrans412.Close()
    _, err413 := mbTrans412.WriteString(arg411)
    if err413 != nil {
      Usage()
      return
    }
    factory414 := thrift.NewTJSONProtocolFactory()
    jsProt415 := factory414.GetProtocol(mbTrans412)
    argvalue0 := rpc.NewDeleteDataInColumnsReq()
    err416 := argvalue0.Read(context.Background(), jsProt415)
    if err416 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DeleteDataInColumns(context.Background(), value0))
    fmt.Print("\n")
    break
  case "queryData":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "QueryData requires 1 args")
      flag.Usage()
    }
    arg417 := flag.Arg(1)
    mbTrans418 := thrift.NewTMemoryBufferLen(len(arg417))
    defer mbTrans418.Close()
    _, err419 := mbTrans418.WriteString(arg417)
    if err419 != nil {
      Usage()
      return
    }
    factory420 := thrift.NewTJSONProtocolFactory()
    jsProt421 := factory420.GetProtocol(mbTrans418)
    argvalue0 := rpc.NewQueryDataReq()
    err422 := argvalue0.Read(context.Background(), jsProt421)
    if err422 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.QueryData(context.Background(), value0))
    fmt.Print("\n")
    break
  case "addStorageEngines":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AddStorageEngines requires 1 args")
      flag.Usage()
    }
    arg423 := flag.Arg(1)
    mbTrans424 := thrift.NewTMemoryBufferLen(len(arg423))
    defer mbTrans424.Close()
    _, err425 := mbTrans424.WriteString(arg423)
    if err425 != nil {
      Usage()
      return
    }
    factory426 := thrift.NewTJSONProtocolFactory()
    jsProt427 := factory426.GetProtocol(mbTrans424)
    argvalue0 := rpc.NewAddStorageEnginesReq()
    err428 := argvalue0.Read(context.Background(), jsProt427)
    if err428 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AddStorageEngines(context.Background(), value0))
    fmt.Print("\n")
    break
  case "removeHistoryDataSource":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "RemoveHistoryDataSource requires 1 args")
      flag.Usage()
    }
    arg429 := flag.Arg(1)
    mbTrans430 := thrift.NewTMemoryBufferLen(len(arg429))
    defer mbTrans430.Close()
    _, err431 := mbTrans430.WriteString(arg429)
    if err431 != nil {
      Usage()
      return
    }
    factory432 := thrift.NewTJSONProtocolFactory()
    jsProt433 := factory432.GetProtocol(mbTrans430)
    argvalue0 := rpc.NewRemoveHistoryDataSourceReq()
    err434 := argvalue0.Read(context.Background(), jsProt433)
    if err434 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.RemoveHistoryDataSource(context.Background(), value0))
    fmt.Print("\n")
    break
  case "removeStorageEngine":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "RemoveStorageEngine requires 1 args")
      flag.Usage()
    }
    arg435 := flag.Arg(1)
    mbTrans436 := thrift.NewTMemoryBufferLen(len(arg435))
    defer mbTrans436.Close()
    _, err437 := mbTrans436.WriteString(arg435)
    if err437 != nil {
      Usage()
      return
    }
    factory438 := thrift.NewTJSONProtocolFactory()
    jsProt439 := factory438.GetProtocol(mbTrans436)
    argvalue0 := rpc.NewRemoveStorageEngineReq()
    err440 := argvalue0.Read(context.Background(), jsProt439)
    if err440 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.RemoveStorageEngine(context.Background(), value0))
    fmt.Print("\n")
    break
  case "aggregateQuery":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AggregateQuery requires 1 args")
      flag.Usage()
    }
    arg441 := flag.Arg(1)
    mbTrans442 := thrift.NewTMemoryBufferLen(len(arg441))
    defer mbTrans442.Close()
    _, err443 := mbTrans442.WriteString(arg441)
    if err443 != nil {
      Usage()
      return
    }
    factory444 := thrift.NewTJSONProtocolFactory()
    jsProt445 := factory444.GetProtocol(mbTrans442)
    argvalue0 := rpc.NewAggregateQueryReq()
    err446 := argvalue0.Read(context.Background(), jsProt445)
    if err446 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AggregateQuery(context.Background(), value0))
    fmt.Print("\n")
    break
  case "lastQuery":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "LastQuery requires 1 args")
      flag.Usage()
    }
    arg447 := flag.Arg(1)
    mbTrans448 := thrift.NewTMemoryBufferLen(len(arg447))
    defer mbTrans448.Close()
    _, err449 := mbTrans448.WriteString(arg447)
    if err449 != nil {
      Usage()
      return
    }
    factory450 := thrift.NewTJSONProtocolFactory()
    jsProt451 := factory450.GetProtocol(mbTrans448)
    argvalue0 := rpc.NewLastQueryReq()
    err452 := argvalue0.Read(context.Background(), jsProt451)
    if err452 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.LastQuery(context.Background(), value0))
    fmt.Print("\n")
    break
  case "downsampleQuery":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DownsampleQuery requires 1 args")
      flag.Usage()
    }
    arg453 := flag.Arg(1)
    mbTrans454 := thrift.NewTMemoryBufferLen(len(arg453))
    defer mbTrans454.Close()
    _, err455 := mbTrans454.WriteString(arg453)
    if err455 != nil {
      Usage()
      return
    }
    factory456 := thrift.NewTJSONProtocolFactory()
    jsProt457 := factory456.GetProtocol(mbTrans454)
    argvalue0 := rpc.NewDownsampleQueryReq()
    err458 := argvalue0.Read(context.Background(), jsProt457)
    if err458 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DownsampleQuery(context.Background(), value0))
    fmt.Print("\n")
    break
  case "showColumns":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ShowColumns requires 1 args")
      flag.Usage()
    }
    arg459 := flag.Arg(1)
    mbTrans460 := thrift.NewTMemoryBufferLen(len(arg459))
    defer mbTrans460.Close()
    _, err461 := mbTrans460.WriteString(arg459)
    if err461 != nil {
      Usage()
      return
    }
    factory462 := thrift.NewTJSONProtocolFactory()
    jsProt463 := factory462.GetProtocol(mbTrans460)
    argvalue0 := rpc.NewShowColumnsReq()
    err464 := argvalue0.Read(context.Background(), jsProt463)
    if err464 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ShowColumns(context.Background(), value0))
    fmt.Print("\n")
    break
  case "getReplicaNum":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetReplicaNum requires 1 args")
      flag.Usage()
    }
    arg465 := flag.Arg(1)
    mbTrans466 := thrift.NewTMemoryBufferLen(len(arg465))
    defer mbTrans466.Close()
    _, err467 := mbTrans466.WriteString(arg465)
    if err467 != nil {
      Usage()
      return
    }
    factory468 := thrift.NewTJSONProtocolFactory()
    jsProt469 := factory468.GetProtocol(mbTrans466)
    argvalue0 := rpc.NewGetReplicaNumReq()
    err470 := argvalue0.Read(context.Background(), jsProt469)
    if err470 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetReplicaNum(context.Background(), value0))
    fmt.Print("\n")
    break
  case "executeSql":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ExecuteSql requires 1 args")
      flag.Usage()
    }
    arg471 := flag.Arg(1)
    mbTrans472 := thrift.NewTMemoryBufferLen(len(arg471))
    defer mbTrans472.Close()
    _, err473 := mbTrans472.WriteString(arg471)
    if err473 != nil {
      Usage()
      return
    }
    factory474 := thrift.NewTJSONProtocolFactory()
    jsProt475 := factory474.GetProtocol(mbTrans472)
    argvalue0 := rpc.NewExecuteSqlReq()
    err476 := argvalue0.Read(context.Background(), jsProt475)
    if err476 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ExecuteSql(context.Background(), value0))
    fmt.Print("\n")
    break
  case "updateUser":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "UpdateUser requires 1 args")
      flag.Usage()
    }
    arg477 := flag.Arg(1)
    mbTrans478 := thrift.NewTMemoryBufferLen(len(arg477))
    defer mbTrans478.Close()
    _, err479 := mbTrans478.WriteString(arg477)
    if err479 != nil {
      Usage()
      return
    }
    factory480 := thrift.NewTJSONProtocolFactory()
    jsProt481 := factory480.GetProtocol(mbTrans478)
    argvalue0 := rpc.NewUpdateUserReq()
    err482 := argvalue0.Read(context.Background(), jsProt481)
    if err482 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.UpdateUser(context.Background(), value0))
    fmt.Print("\n")
    break
  case "addUser":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AddUser requires 1 args")
      flag.Usage()
    }
    arg483 := flag.Arg(1)
    mbTrans484 := thrift.NewTMemoryBufferLen(len(arg483))
    defer mbTrans484.Close()
    _, err485 := mbTrans484.WriteString(arg483)
    if err485 != nil {
      Usage()
      return
    }
    factory486 := thrift.NewTJSONProtocolFactory()
    jsProt487 := factory486.GetProtocol(mbTrans484)
    argvalue0 := rpc.NewAddUserReq()
    err488 := argvalue0.Read(context.Background(), jsProt487)
    if err488 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AddUser(context.Background(), value0))
    fmt.Print("\n")
    break
  case "deleteUser":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DeleteUser requires 1 args")
      flag.Usage()
    }
    arg489 := flag.Arg(1)
    mbTrans490 := thrift.NewTMemoryBufferLen(len(arg489))
    defer mbTrans490.Close()
    _, err491 := mbTrans490.WriteString(arg489)
    if err491 != nil {
      Usage()
      return
    }
    factory492 := thrift.NewTJSONProtocolFactory()
    jsProt493 := factory492.GetProtocol(mbTrans490)
    argvalue0 := rpc.NewDeleteUserReq()
    err494 := argvalue0.Read(context.Background(), jsProt493)
    if err494 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DeleteUser(context.Background(), value0))
    fmt.Print("\n")
    break
  case "getUser":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetUser requires 1 args")
      flag.Usage()
    }
    arg495 := flag.Arg(1)
    mbTrans496 := thrift.NewTMemoryBufferLen(len(arg495))
    defer mbTrans496.Close()
    _, err497 := mbTrans496.WriteString(arg495)
    if err497 != nil {
      Usage()
      return
    }
    factory498 := thrift.NewTJSONProtocolFactory()
    jsProt499 := factory498.GetProtocol(mbTrans496)
    argvalue0 := rpc.NewGetUserReq()
    err500 := argvalue0.Read(context.Background(), jsProt499)
    if err500 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetUser(context.Background(), value0))
    fmt.Print("\n")
    break
  case "getClusterInfo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetClusterInfo requires 1 args")
      flag.Usage()
    }
    arg501 := flag.Arg(1)
    mbTrans502 := thrift.NewTMemoryBufferLen(len(arg501))
    defer mbTrans502.Close()
    _, err503 := mbTrans502.WriteString(arg501)
    if err503 != nil {
      Usage()
      return
    }
    factory504 := thrift.NewTJSONProtocolFactory()
    jsProt505 := factory504.GetProtocol(mbTrans502)
    argvalue0 := rpc.NewGetClusterInfoReq()
    err506 := argvalue0.Read(context.Background(), jsProt505)
    if err506 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetClusterInfo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "executeStatement":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ExecuteStatement requires 1 args")
      flag.Usage()
    }
    arg507 := flag.Arg(1)
    mbTrans508 := thrift.NewTMemoryBufferLen(len(arg507))
    defer mbTrans508.Close()
    _, err509 := mbTrans508.WriteString(arg507)
    if err509 != nil {
      Usage()
      return
    }
    factory510 := thrift.NewTJSONProtocolFactory()
    jsProt511 := factory510.GetProtocol(mbTrans508)
    argvalue0 := rpc.NewExecuteStatementReq()
    err512 := argvalue0.Read(context.Background(), jsProt511)
    if err512 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ExecuteStatement(context.Background(), value0))
    fmt.Print("\n")
    break
  case "fetchResults":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "FetchResults requires 1 args")
      flag.Usage()
    }
    arg513 := flag.Arg(1)
    mbTrans514 := thrift.NewTMemoryBufferLen(len(arg513))
    defer mbTrans514.Close()
    _, err515 := mbTrans514.WriteString(arg513)
    if err515 != nil {
      Usage()
      return
    }
    factory516 := thrift.NewTJSONProtocolFactory()
    jsProt517 := factory516.GetProtocol(mbTrans514)
    argvalue0 := rpc.NewFetchResultsReq()
    err518 := argvalue0.Read(context.Background(), jsProt517)
    if err518 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.FetchResults(context.Background(), value0))
    fmt.Print("\n")
    break
  case "closeStatement":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CloseStatement requires 1 args")
      flag.Usage()
    }
    arg519 := flag.Arg(1)
    mbTrans520 := thrift.NewTMemoryBufferLen(len(arg519))
    defer mbTrans520.Close()
    _, err521 := mbTrans520.WriteString(arg519)
    if err521 != nil {
      Usage()
      return
    }
    factory522 := thrift.NewTJSONProtocolFactory()
    jsProt523 := factory522.GetProtocol(mbTrans520)
    argvalue0 := rpc.NewCloseStatementReq()
    err524 := argvalue0.Read(context.Background(), jsProt523)
    if err524 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CloseStatement(context.Background(), value0))
    fmt.Print("\n")
    break
  case "cancelStatement":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CancelStatement requires 1 args")
      flag.Usage()
    }
    arg525 := flag.Arg(1)
    mbTrans526 := thrift.NewTMemoryBufferLen(len(arg525))
    defer mbTrans526.Close()
    _, err527 := mbTrans526.WriteString(arg525)
    if err527 != nil {
      Usage()
      return
    }
    factory528 := thrift.NewTJSONProtocolFactory()
    jsProt529 := factory528.GetProtocol(mbTrans526)
    argvalue0 := rpc.NewCancelStatementReq()
    err530 := argvalue0.Read(context.Background(), jsProt529)
    if err530 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CancelStatement(context.Background(), value0))
    fmt.Print("\n")
    break
  case "commitTransformJob":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CommitTransformJob requires 1 args")
      flag.Usage()
    }
    arg531 := flag.Arg(1)
    mbTrans532 := thrift.NewTMemoryBufferLen(len(arg531))
    defer mbTrans532.Close()
    _, err533 := mbTrans532.WriteString(arg531)
    if err533 != nil {
      Usage()
      return
    }
    factory534 := thrift.NewTJSONProtocolFactory()
    jsProt535 := factory534.GetProtocol(mbTrans532)
    argvalue0 := rpc.NewCommitTransformJobReq()
    err536 := argvalue0.Read(context.Background(), jsProt535)
    if err536 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CommitTransformJob(context.Background(), value0))
    fmt.Print("\n")
    break
  case "queryTransformJobStatus":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "QueryTransformJobStatus requires 1 args")
      flag.Usage()
    }
    arg537 := flag.Arg(1)
    mbTrans538 := thrift.NewTMemoryBufferLen(len(arg537))
    defer mbTrans538.Close()
    _, err539 := mbTrans538.WriteString(arg537)
    if err539 != nil {
      Usage()
      return
    }
    factory540 := thrift.NewTJSONProtocolFactory()
    jsProt541 := factory540.GetProtocol(mbTrans538)
    argvalue0 := rpc.NewQueryTransformJobStatusReq()
    err542 := argvalue0.Read(context.Background(), jsProt541)
    if err542 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.QueryTransformJobStatus(context.Background(), value0))
    fmt.Print("\n")
    break
  case "showEligibleJob":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ShowEligibleJob requires 1 args")
      flag.Usage()
    }
    arg543 := flag.Arg(1)
    mbTrans544 := thrift.NewTMemoryBufferLen(len(arg543))
    defer mbTrans544.Close()
    _, err545 := mbTrans544.WriteString(arg543)
    if err545 != nil {
      Usage()
      return
    }
    factory546 := thrift.NewTJSONProtocolFactory()
    jsProt547 := factory546.GetProtocol(mbTrans544)
    argvalue0 := rpc.NewShowEligibleJobReq()
    err548 := argvalue0.Read(context.Background(), jsProt547)
    if err548 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ShowEligibleJob(context.Background(), value0))
    fmt.Print("\n")
    break
  case "cancelTransformJob":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CancelTransformJob requires 1 args")
      flag.Usage()
    }
    arg549 := flag.Arg(1)
    mbTrans550 := thrift.NewTMemoryBufferLen(len(arg549))
    defer mbTrans550.Close()
    _, err551 := mbTrans550.WriteString(arg549)
    if err551 != nil {
      Usage()
      return
    }
    factory552 := thrift.NewTJSONProtocolFactory()
    jsProt553 := factory552.GetProtocol(mbTrans550)
    argvalue0 := rpc.NewCancelTransformJobReq()
    err554 := argvalue0.Read(context.Background(), jsProt553)
    if err554 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CancelTransformJob(context.Background(), value0))
    fmt.Print("\n")
    break
  case "registerTask":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "RegisterTask requires 1 args")
      flag.Usage()
    }
    arg555 := flag.Arg(1)
    mbTrans556 := thrift.NewTMemoryBufferLen(len(arg555))
    defer mbTrans556.Close()
    _, err557 := mbTrans556.WriteString(arg555)
    if err557 != nil {
      Usage()
      return
    }
    factory558 := thrift.NewTJSONProtocolFactory()
    jsProt559 := factory558.GetProtocol(mbTrans556)
    argvalue0 := rpc.NewRegisterTaskReq()
    err560 := argvalue0.Read(context.Background(), jsProt559)
    if err560 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.RegisterTask(context.Background(), value0))
    fmt.Print("\n")
    break
  case "dropTask":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DropTask requires 1 args")
      flag.Usage()
    }
    arg561 := flag.Arg(1)
    mbTrans562 := thrift.NewTMemoryBufferLen(len(arg561))
    defer mbTrans562.Close()
    _, err563 := mbTrans562.WriteString(arg561)
    if err563 != nil {
      Usage()
      return
    }
    factory564 := thrift.NewTJSONProtocolFactory()
    jsProt565 := factory564.GetProtocol(mbTrans562)
    argvalue0 := rpc.NewDropTaskReq()
    err566 := argvalue0.Read(context.Background(), jsProt565)
    if err566 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DropTask(context.Background(), value0))
    fmt.Print("\n")
    break
  case "getRegisterTaskInfo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "GetRegisterTaskInfo requires 1 args")
      flag.Usage()
    }
    arg567 := flag.Arg(1)
    mbTrans568 := thrift.NewTMemoryBufferLen(len(arg567))
    defer mbTrans568.Close()
    _, err569 := mbTrans568.WriteString(arg567)
    if err569 != nil {
      Usage()
      return
    }
    factory570 := thrift.NewTJSONProtocolFactory()
    jsProt571 := factory570.GetProtocol(mbTrans568)
    argvalue0 := rpc.NewGetRegisterTaskInfoReq()
    err572 := argvalue0.Read(context.Background(), jsProt571)
    if err572 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.GetRegisterTaskInfo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "curveMatch":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "CurveMatch requires 1 args")
      flag.Usage()
    }
    arg573 := flag.Arg(1)
    mbTrans574 := thrift.NewTMemoryBufferLen(len(arg573))
    defer mbTrans574.Close()
    _, err575 := mbTrans574.WriteString(arg573)
    if err575 != nil {
      Usage()
      return
    }
    factory576 := thrift.NewTJSONProtocolFactory()
    jsProt577 := factory576.GetProtocol(mbTrans574)
    argvalue0 := rpc.NewCurveMatchReq()
    err578 := argvalue0.Read(context.Background(), jsProt577)
    if err578 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.CurveMatch(context.Background(), value0))
    fmt.Print("\n")
    break
  case "debugInfo":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "DebugInfo requires 1 args")
      flag.Usage()
    }
    arg579 := flag.Arg(1)
    mbTrans580 := thrift.NewTMemoryBufferLen(len(arg579))
    defer mbTrans580.Close()
    _, err581 := mbTrans580.WriteString(arg579)
    if err581 != nil {
      Usage()
      return
    }
    factory582 := thrift.NewTJSONProtocolFactory()
    jsProt583 := factory582.GetProtocol(mbTrans580)
    argvalue0 := rpc.NewDebugInfoReq()
    err584 := argvalue0.Read(context.Background(), jsProt583)
    if err584 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.DebugInfo(context.Background(), value0))
    fmt.Print("\n")
    break
  case "loadAvailableEndPoints":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "LoadAvailableEndPoints requires 1 args")
      flag.Usage()
    }
    arg585 := flag.Arg(1)
    mbTrans586 := thrift.NewTMemoryBufferLen(len(arg585))
    defer mbTrans586.Close()
    _, err587 := mbTrans586.WriteString(arg585)
    if err587 != nil {
      Usage()
      return
    }
    factory588 := thrift.NewTJSONProtocolFactory()
    jsProt589 := factory588.GetProtocol(mbTrans586)
    argvalue0 := rpc.NewLoadAvailableEndPointsReq()
    err590 := argvalue0.Read(context.Background(), jsProt589)
    if err590 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.LoadAvailableEndPoints(context.Background(), value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
