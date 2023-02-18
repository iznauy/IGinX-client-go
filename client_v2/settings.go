package client_v2

import (
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	DefaultEnableRetry                 = true
	DefaultMaxRetryTimes               = 2
	DefaultUsername                    = "root"
	DefaultPassword                    = "root"
	DefaultEnableHighAvailable         = true
	DefaultEnableAutoLoadAvailableList = false
	DefaultMaxSwitchTimes              = 3
	DefaultLoadAvailableListInterval   = 30
	DefaultSocketTimeout               = 0
	DefaultConnectionTimeout           = 0
)

type SessionSettings struct {
	Username                  string
	Password                  string
	EnableRetry               bool
	MaxRetryTimes             int
	EnableHighAvailable       bool
	AutoLoadAvailableList     bool
	MaxSwitchTimes            int
	LoadAvailableListInterval int
	SocketTimeout             time.Duration
	ConnectionTimeout         time.Duration

	endPoints           []EndPoint
	endPointsIndex      int
	endPointsStartIndex int
	hasMoreEndPoint     bool
	mu                  sync.Mutex
}

func NewSessionSettings(connectionString string) (*SessionSettings, error) {
	endPointStrings := strings.Split(connectionString, ",")
	if len(endPointStrings) == 0 {
		return nil, errors.New("you should provide at lease one endpoint")
	}
	endPoints := make([]EndPoint, 0, len(endPointStrings))
	for _, endPointString := range endPointStrings {
		endPoints = append(endPoints, *NewEndPointFromConnectionString(endPointString))
	}
	return &SessionSettings{
		Username:                  DefaultUsername,
		Password:                  DefaultPassword,
		EnableRetry:               DefaultEnableRetry,
		MaxRetryTimes:             DefaultMaxRetryTimes,
		EnableHighAvailable:       DefaultEnableHighAvailable,
		AutoLoadAvailableList:     DefaultEnableAutoLoadAvailableList,
		MaxSwitchTimes:            DefaultMaxSwitchTimes,
		LoadAvailableListInterval: DefaultLoadAvailableListInterval,
		SocketTimeout:             DefaultSocketTimeout,
		ConnectionTimeout:         DefaultConnectionTimeout,

		endPoints:           endPoints,
		endPointsIndex:      0,
		endPointsStartIndex: 0,
		hasMoreEndPoint:     true,
	}, nil
}

func (s *SessionSettings) SetEndPoints(endPoints []EndPoint) {
	s.mu.Lock()
	if len(endPoints) == 0 {
		return
	}
	s.endPoints = endPoints
	s.endPointsIndex = 0
	s.endPointsStartIndex = 0
	s.hasMoreEndPoint = true
	s.mu.Unlock()
}

func (s *SessionSettings) ResetEndPoints() {
	s.mu.Lock()
	s.endPointsStartIndex = s.endPointsIndex
	s.hasMoreEndPoint = true
	s.mu.Unlock()
}

func (s *SessionSettings) GetEndPoint() *EndPoint {
	s.mu.Lock()
	e := &s.endPoints[s.endPointsIndex]
	s.endPointsIndex = (s.endPointsIndex + 1) % len(s.endPoints)
	if s.endPointsIndex == s.endPointsStartIndex {
		s.hasMoreEndPoint = false
	}
	s.mu.Unlock()
	return e
}

func (s *SessionSettings) HasMoreEndPoint() bool {
	s.mu.Lock()
	hasMore := s.hasMoreEndPoint
	s.mu.Unlock()
	return hasMore
}
