package tests

import (
	"db3/cassandra-postgres/client"
	"db3/cassandra-postgres/model"
	"math"

	"github.com/gocql/gocql"
)

func CassandraLoad(data []model.Telemetry, data2 []model.DeviceConfigHistory, clients int) int {
	cassandra := client.CassandraClient{
		ClusterHost: "127.0.0.1",
		Keyspace:    "db3",
		Consistency: gocql.One,
		User:        "cassandra",
		Password:    "cassandra",
	}

	cassandra.Connect()

	if err := cassandra.Session.Query("DROP TABLE IF EXISTS telemetry").Exec(); err != nil {
		panic(err)
	}
	if err := cassandra.Session.Query("DROP TABLE IF EXISTS device_configuration_history").Exec(); err != nil {
		panic(err)
	}

	if err := cassandra.Session.Query(`
		CREATE TABLE IF NOT EXISTS telemetry (
			timestamp      double,
			device         text,
			carbonmonoxide double,
			humidity       double,
			light          tinyint,
			lpg            double,
			motion         tinyint,
			smoke          double,
			temperature    double,
			PRIMARY KEY (device, timestamp) 
		) WITH CLUSTERING ORDER BY (timestamp DESC);`).Exec(); err != nil {
		panic(err)
	}

	if err := cassandra.Session.Query(`
		CREATE TABLE IF NOT EXISTS device_configuration_history (
			device text,
			valid_to_timestamp double,
			firmware_version text,
			PRIMARY KEY (device, valid_to_timestamp)
		) WITH CLUSTERING ORDER BY (valid_to_timestamp DESC);`).Exec(); err != nil {
		panic(err)
	}

	cassandra.Session.Close()

	c := make(chan int)
	longest := 0
	done := 0

	offset := int(len(data) / clients)
	offset2 := int(len(data2) / clients)
	for i := 0; i < clients; i++ {
		ca := client.CassandraClient{
			ClusterHost: "127.0.0.1",
			Keyspace:    "db3",
			Consistency: gocql.One,
			User:        "cassandra",
			Password:    "cassandra",
		}
		go ca.Load(data[i*offset:int(math.Min(float64((i+1)*offset), float64(len(data))))], data2[i*offset2:int(math.Min(float64((i+1)*offset2), float64(len(data2))))], c, i)
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
