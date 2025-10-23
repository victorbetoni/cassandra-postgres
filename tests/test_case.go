package tests

import (
	"db3/cassandra-postgres/client"
	"fmt"

	"github.com/gocql/gocql"
)

type TestCase struct {
	Desscription      string `json:"description"`
	CassandraQuery    string `json:"cassandra"`
	PostgresQuery     string `json:"postgres"`
	CassandraPoolSize int    `json:"size"`
}

func Queries(q []TestCase) {
	po := client.PostgresClient{
		User:     "postgres",
		Password: "postgres",
		DB:       "postgres",
		Host:     "localhost",
		Port:     5432,
	}
	po.Connect()
	defer po.Connection.Close()

	ca := client.CassandraClient{
		ClusterHost: "127.0.0.1",
		Keyspace:    "db3",
		Consistency: gocql.One,
		User:        "cassandra",
		Password:    "cassandra",
	}
	ca.Connect()
	defer ca.Session.Close()

	for _, t := range q {
		fmt.Println(t.Desscription)
		pgT, pgR := po.RunTimer(t.PostgresQuery)
		fmt.Printf("Postgres: %dms (%d rows)\n", pgT, pgR)
		caT, caR := ca.RunTimer(t.CassandraQuery, t.CassandraPoolSize)
		fmt.Printf("Cassandra: %dms (%d rows)\n", caT, caR)
		fmt.Println()
	}
}
