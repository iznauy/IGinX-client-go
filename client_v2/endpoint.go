package client_v2

import (
	"strconv"
	"strings"
)

type EndPoint struct {
	Ip   string
	Port int32
}

func NewEndPoint(ip string, port int32) *EndPoint {
	return &EndPoint{
		Ip:   ip,
		Port: port,
	}
}

func NewEndPointFromConnectionString(connectionString string) *EndPoint {
	parts := strings.Split(connectionString, ":")
	ip := parts[0]
	port, _ := strconv.Atoi(parts[1])
	return &EndPoint{
		Ip:   ip,
		Port: int32(port),
	}
}
