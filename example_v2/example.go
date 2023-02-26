package main

import (
	"fmt"
	"github.com/iznauy/IGinX-client-go/client_v2"
	"github.com/iznauy/IGinX-client-go/rpc"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	session *client_v2.Session
)

func init() {
	settings, err := client_v2.NewSessionSettings("127.0.0.1:6888")
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
	bytes, _ := os.ReadFile("/Users/iznauy/experiment/code/TSBS-IGinX/tsbs-iginx/iginx-iot-scale20-data.txt")
	lines := strings.Split(string(bytes), "\n")[:2000]
	interval := 200
	for i := 0; i < len(lines); i += interval {
		from := i
		to := i + interval
		if len(lines) < to {
			to = len(lines)
		}
		fmt.Printf("Process: %d/%d\n", i, len(lines))
		writeData(lines[from:to])
	}
}

func parseMeasurementAndValues(measurement string, fields string) ([]string, []float64) {
	var paths []string
	var values []float64

	measurement = "type=" + measurement
	fir := strings.Split(measurement, ",")
	device := fir[0] + "."
	for j := 1; j < len(fir); j++ {
		kv := strings.Split(fir[j], "=")
		device += strings.Replace(kv[1], ".", "_", -1)
		device += "."
		device = strings.Replace(device, "-", "_", -1)
	}

	device = device[5 : len(device)-1]
	device = strings.Replace(device, "-", "_", -1)

	sec := strings.Split(fields, ",")
	for j := 0; j < len(sec); j++ {
		kv := strings.Split(sec[j], "=")
		path := device + "." + kv[0]
		path = strings.Replace(path, "-", "_", -1)

		v, err := strconv.ParseFloat(kv[1], 32)
		if err != nil {
			log.Fatal(err)
		}
		paths = append(paths, path)
		values = append(values, v)
	}
	return paths, values
}

func writeData(lines []string) {
	var paths []string
	var timestamps []int64
	var timestampIndices = make(map[int64]int)
	var values [][]interface{}
	var types []rpc.DataType
	var pathIndices = make(map[string]int)

	for _, line := range lines {
		parts := strings.Split(line, " ")
		subPaths, _ := parseMeasurementAndValues(parts[0], parts[1])
		for _, subPath := range subPaths {
			if _, ok := pathIndices[subPath]; ok {
				continue
			}
			pathIndices[subPath] = len(paths)
			paths = append(paths, subPath)
			types = append(types, rpc.DataType_DOUBLE)
		}
		timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
		if _, ok := timestampIndices[timestamp]; !ok {
			timestampIndices[timestamp] = len(timestamps)
			timestamps = append(timestamps, timestamp)
			fmt.Println(timestamp)
		}
	}

	for range paths {
		values = append(values, make([]interface{}, len(timestamps), len(timestamps)))
	}

	for _, line := range lines {
		secondIndex := 0
		parts := strings.Split(line, " ")
		timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
		for i := range timestamps {
			if timestamps[i] == timestamp {
				secondIndex = i
				break
			}
		}
		subPaths, subValues := parseMeasurementAndValues(parts[0], parts[1])
		for i, subPath := range subPaths {
			firstIndex := pathIndices[subPath]
			values[firstIndex][secondIndex] = subValues[i]
		}
	}

	err := session.InsertNonAlignedColumnRecords(paths, timestamps, values, types, nil)
	if err != nil {
		log.Println(err)
		panic(err)
	}
}
