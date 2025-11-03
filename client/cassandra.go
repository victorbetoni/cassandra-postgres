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

func (ca *CassandraClient) Load(data []model.Telemetry, data2 []model.DeviceConfigHistory, c chan int, id int) {
	ca.Connect()
	fmt.Println("Postgres ", len(data)+len(data2))
	fmt.Printf("Worker %d conectou\n", id)
	defer ca.Session.Close()
	query1 := `
		INSERT INTO telemetry (
			timestamp, device, carbonmonoxide, humidity, light, lpg, motion, smoke, temperature
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	query2 := `
		INSERT INTO device_configuration_history (
			device, valid_to_timestamp, firmware_version
		) VALUES (?,?,?)
	`
	c <- Timer(func() {
		total := len(data) + len(data2)
		i := 0
		for _, dt := range data {
			q := ca.Session.Query(query1,
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
				fmt.Printf("Worker %d: %.2f/%%\n", id, float64(100*i)/float64(total))
			}
		}
		for _, dt := range data2 {
			q := ca.Session.Query(query2,
				dt.Device,
				dt.ValidToTimestamp,
				dt.FirmwareVersion,
			)
			if err := q.Exec(); err != nil {
				panic(err)
			}
			i++
			if i%1000 == 0 {
				fmt.Printf("Worker %d: %.2f/%%\n", id, float64(100*i)/float64(total))
			}
		}
	})
}
