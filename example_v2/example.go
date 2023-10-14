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

	defaultTruck = "unknown"
	defaultTagK  = []string{"fleet", "driver", "model", "device_version"}
	defaultTagV  = []string{"unknown", "unknown", "unknown", "unknown"}
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

func formatName(name string) string {
	parts := strings.Split(name, "_")
	truck := parts[0]
	index, _ := strconv.Atoi(parts[1])
	return fmt.Sprintf("%s_%04d", truck, index)
}

func parseMeasurementAndValues(measurement string, fields string) ([]string, []float64) {
	var paths []string
	var values []float64

	fir := strings.Split(measurement, ",")
	device := fir[0] + "."
	if !strings.Contains(fir[1], "truck") {
		device += defaultTruck
		device += "."
		fir = fir[1:]
	} else {
		device += formatName(strings.Split(fir[1], "=")[1])
		device += "."
		fir = fir[2:]
	}

	index := 0
	for j := 0; j < len(defaultTagK); j++ {
		if index < len(fir) {
			kv := strings.Split(fir[index], "=")
			if defaultTagK[j] == kv[0] {
				device += strings.Replace(kv[1], ".", "_", -1)
				device += "."
				index++
				continue
			}
		}
		device += defaultTagV[j]
		device += "."
	}
	device = strings.Replace(device, "-", "_", -1)

	sec := strings.Split(fields, ",")
	for j := 0; j < len(sec); j++ {
		kv := strings.Split(sec[j], "=")
		path := device + kv[0]
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
