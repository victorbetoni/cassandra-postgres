package tests

import (
	"db3/cassandra-postgres/client"
	"db3/cassandra-postgres/model"
	"math"
)

func PostgresLoad(data []model.Telemetry, data2 []model.DeviceConfigHistory, clients int) int {
	postgres := client.PostgresClient{
		User:     "postgres",
		Password: "postgres",
		DB:       "postgres",
		Host:     "127.0.0.1",
		Port:     5432,
	}

	postgres.Connect()

	if _, err := postgres.Connection.Exec("DELETE FROM public.telemetry"); err != nil {
		panic(err)
	}

	if _, err := postgres.Connection.Exec("DELETE FROM public.device_configuration_history"); err != nil {
		panic(err)
	}

	c := make(chan int)
	done := 0
	longest := 0

	offset := int(len(data) / clients)
	offset2 := int(len(data2) / clients)
	for i := 0; i < clients; i++ {
		po := client.PostgresClient{
			User:     "postgres",
			Password: "postgres",
			DB:       "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
		}
		go po.Load(data[i*offset:int(math.Min(float64((i+1)*offset), float64(len(data))))], data2[i*offset2:int(math.Min(float64((i+1)*offset2), float64(len(data2))))], c, i)
	}

	for msg := range c {
		done++
		if msg > longest {
			longest = msg
		}
		if done == clients {
			break
		}
	}

	return longest
}
