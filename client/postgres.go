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

func (po *PostgresClient) Load(data []model.Telemetry, c chan int, id int) {
	po.Connect()
	defer po.Connection.Close()
	sqlStatement := `
	INSERT INTO public.telemetry (
		timestamp, device, carbonmonoxide, humidity, light, lpg, motion, smoke, temperature
	) 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	c <- Timer(func() {
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
				fmt.Printf("Worker %d: %d/%d\n", id, i, len(data))
			}
		}
	})
}
