package tests

import (
	"db3/cassandra-postgres/client"
	"db3/cassandra-postgres/model"
	"math"

	"github.com/gocql/gocql"
)

func CassandraLoad(data []model.Telemetry, clients int) int {
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

	createTable := `
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
		) WITH CLUSTERING ORDER BY (timestamp DESC);
	`

	if err := cassandra.Session.Query(createTable).Exec(); err != nil {
		panic(err)
	}

	cassandra.Session.Close()

	c := make(chan int)
	longest := 0
	done := 0

	offset := int(len(data) / clients)
	for i := 0; i < clients; i++ {
		ca := client.CassandraClient{
			ClusterHost: "127.0.0.1",
			Keyspace:    "db3",
			Consistency: gocql.One,
			User:        "cassandra",
			Password:    "cassandra",
		}
		go ca.Load(data[i*offset:int(math.Min(float64((i+1)*offset), float64(len(data))))], c, i)
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
