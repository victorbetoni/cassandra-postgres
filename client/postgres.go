package client

import (
	"database/sql"
	"db3/cassandra-postgres/model"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresClient struct {
	User       string
	Password   string
	DB         string
	Host       string
	Port       int
	Connection *sql.DB
}

func (po *PostgresClient) Connect() {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable", po.User, po.Password, po.DB, po.Host, po.Port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	po.Connection = db
}

func (po *PostgresClient) RunTimer(query string) (int, int) {
	var re *sql.Rows
	time := Timer(func() {
		v, err := po.Connection.Query(query)
		if err != nil {
			panic(err)
		}
		re = v
	})
	count := 0
	for re.Next() {
		count++
	}
	return time, count
}

func (po *PostgresClient) Load(data []model.Telemetry, data2 []model.DeviceConfigHistory, c chan int, id int) {
	po.Connect()
	fmt.Println("Postgres ", len(data)+len(data2))
	defer po.Connection.Close()
	sqlStatement := `
	INSERT INTO public.telemetry (
		timestamp, device, carbonmonoxide, humidity, light, lpg, motion, smoke, temperature
	) 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	query2 := `
	INSERT INTO public.device_configuration_history (
		device, valid_to_timestamp, firmware_version
	) 
	VALUES ($1, $2, $3)`
	c <- Timer(func() {
		total := len(data) + len(data2)
		i := 0
		for _, dt := range data {
			if _, err := po.Connection.Exec(
				sqlStatement,
				dt.Timestamp,
				dt.Device,
				dt.CarbonMonoxide,
				dt.Humidity,
				dt.Light,
				dt.LPG,
				dt.Motion,
				dt.Smoke,
				dt.Temperature); err != nil {
				panic(err)
			}
			i++
			if i%100 == 0 {
				fmt.Printf("Worker %d: %.2f/%%\n", id, float64(100*i)/float64(total))
			}
		}
		for _, dt := range data2 {
			if _, err := po.Connection.Exec(
				query2,
				dt.Device,
				dt.ValidToTimestamp,
				dt.FirmwareVersion); err != nil {
				panic(err)
			}
			i++
			if i%100 == 0 {
				fmt.Printf("Worker %d: %.2f/%%\n", id, float64(100*i)/float64(total))
			}
		}
	})
}
