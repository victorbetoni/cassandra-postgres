package main

import (
	"bufio"
	"db3/cassandra-postgres/model"
	"db3/cassandra-postgres/tests"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

func parseBool(str string) int8 {
	if str == "false" {
		return 0
	}
	return 1
}

func main() {

	testCases := make([]tests.TestCase, 0)
	tFile, _ := os.ReadFile("test_cases.json")
	if err := json.Unmarshal(tFile, &testCases); err != nil {
		panic(err)
	}

	data := make([]model.Telemetry, 0)
	file, _ := os.Open("iot_telemetry_data.csv")
	scanner := bufio.NewScanner(file)
	scanner.Scan() //tirar o header
	for scanner.Scan() {
		line := scanner.Text()
		dt := model.Telemetry{}
		vals := strings.Split(line, ",")
		dt.Timestamp, _ = strconv.ParseFloat(strings.ReplaceAll(vals[0], "\"", ""), 64)
		dt.Device = strings.ReplaceAll(vals[1], "\"", "")
		dt.CarbonMonoxide, _ = strconv.ParseFloat(strings.ReplaceAll(vals[2], "\"", ""), 64)
		dt.Humidity, _ = strconv.ParseFloat(strings.ReplaceAll(vals[3], "\"", ""), 64)
		dt.Light = parseBool(strings.ReplaceAll(vals[4], "\"", ""))
		dt.LPG, _ = strconv.ParseFloat(strings.ReplaceAll(vals[5], "\"", ""), 64)
		dt.Motion = parseBool(strings.ReplaceAll(vals[6], "\"", ""))
		dt.Smoke, _ = strconv.ParseFloat(strings.ReplaceAll(vals[7], "\"", ""), 64)
		dt.Temperature, _ = strconv.ParseFloat(strings.ReplaceAll(vals[8], "\"", ""), 64)
		data = append(data, dt)
	}
	file.Close()

	data2 := make([]model.DeviceConfigHistory, 0)
	file, _ = os.Open("device_config_history.csv")
	scanner = bufio.NewScanner(file)
	scanner.Scan() //tirar o header
	for scanner.Scan() {
		line := scanner.Text()
		dt := model.DeviceConfigHistory{}
		vals := strings.Split(line, ",")
		dt.Device = strings.ReplaceAll(vals[0], "\"", "")
		dt.ValidToTimestamp, _ = strconv.ParseFloat(strings.ReplaceAll(vals[1], "\"", ""), 64)
		dt.FirmwareVersion = strings.ReplaceAll(vals[2], "\"", "")
		data2 = append(data2, dt)
	}
	file.Close()

	if os.Args[1:][0] == "load" {
		t, _ := strconv.Atoi(os.Args[1:][1])
		fmt.Println("Iniciando load Cassandra...")
		tempoCassandra := tests.CassandraLoad(data, data2, t)
		fmt.Println("Iniciando load Postgres...")
		//tempoPostgres := tests.PostgresLoad(data, data2, t)
		fmt.Printf("Cassandra: %d milisegundos\n", tempoCassandra)
		//fmt.Printf("Postgres: %d milisegundos\n", tempoPostgres)
	}

	if os.Args[1:][0] == "queries" {
		tests.Queries(testCases)
	}

	if os.Args[1:][0] == "synthetic" {
		lines, _ := strconv.Atoi(os.Args[1:][1])
		b, _ := strconv.ParseBool(os.Args[1:][2])
		writeSyntheticData(lines, b)
	}

}

func writeSyntheticData(lines int, opt bool) {
	fileName := "iot_telemetry_data.csv"
	if opt {
		fileName = "device_config_history.csv"
	}
	tFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(tFile)
	devices := []string{"00:0f:00:70:91:0a", "1c:bf:ce:15:ec:4d", "b8:27:eb:bf:9d:51"}
	if !opt {
		for i := 0; i < lines; i++ {
			timestamp := float64(rand.Intn(1595862195-1595203417+1)+1595203417) + 0.5
			device := devices[rand.Intn(len(devices))]
			carbonMonoxide := rand.Float64() * (0.003)
			humidity := rand.Float64() * 100
			light := "false"
			if rand.Float64() < 0.5 {
				light = "true"
			}
			lpg := rand.Float64() * (0.005)
			motion := "false"
			if rand.Float64() < 0.5 {
				motion = "true"
			}
			smoke := rand.Float64() * (0.05)
			temp := rand.Float64() * 100
			_, err := writer.WriteString(fmt.Sprintf("\"%.2f\",\"%s\",\"%.18f\",\"%.1f\",\"%s\",\"%.18f\",\"%s\",\"%.18f\",\"%.1f\"\n", timestamp, device, carbonMonoxide, humidity, light, lpg, motion, smoke, temp))
			if err != nil {
				panic(err)
			}
		}
	} else {
		for i := 0; i < lines; i++ {
			timestamp := float64(rand.Intn(1595862195-1595203417+1)+1595203417) + 1000.5
			device := devices[rand.Intn(len(devices))]
			version := i
			_, err := writer.WriteString(fmt.Sprintf("\"%s\",\"%.2f\",\"%d\"\n", device, timestamp, version))
			if err != nil {
				panic(err)
			}
		}
	}
	writer.Flush()
	tFile.Close()
}
