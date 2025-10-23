package client

import (
	"db3/cassandra-postgres/model"
	"fmt"

	"github.com/gocql/gocql"
)

type CassandraClient struct {
	ClusterHost string
	Keyspace    string
	Consistency gocql.Consistency
	User        string
	Password    string
	Session     *gocql.Session
}

func (ca *CassandraClient) Connect() error {
	cluster := gocql.NewCluster(ca.ClusterHost)
	cluster.Keyspace = ca.Keyspace
	cluster.Consistency = ca.Consistency
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: ca.User,
		Password: ca.Password,
	}
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	ca.Session = session
	return nil
}

func (ca *CassandraClient) RunTimer(query string, size int) (int, int) {
	var iter *gocql.Iter
	time := Timer(func() {
		q := ca.Session.Query(query)
		iter = q.Iter()
	})
	dummies := make([]interface{}, size)
	for i := 0; i < size; i++ {
		var a interface{}
		dummies[i] = a
	}
	count := 0
	for iter.Scan(dummies...) {
		count++
	}
	return time, count
}

func (ca *CassandraClient) Load(data []model.Telemetry, c chan int, id int) {
	ca.Connect()
	fmt.Printf("Worker %d conectou\n", id)
	defer ca.Session.Close()
	query := `
		INSERT INTO telemetry (
			timestamp, device, carbonmonoxide, humidity, light, lpg, motion, smoke, temperature
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	c <- Timer(func() {
		i := 0
		for _, dt := range data {
			q := ca.Session.Query(query,
				dt.Timestamp,
				dt.Device,
				dt.CarbonMonoxide,
				dt.Humidity,
				dt.Light,
				dt.LPG,
				dt.Motion,
				dt.Smoke,
				dt.Temperature,
			)
			if err := q.Exec(); err != nil {
				panic(err)
			}
			i++
			if i%1000 == 0 {
				fmt.Printf("Worker %d: %d/%d\n", id, i, len(data))
			}
		}
	})
}
