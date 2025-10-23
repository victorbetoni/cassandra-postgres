package model

type Telemetry struct {
	Timestamp      float64 `db:"ts"`
	Device         string  `db:"device"`
	CarbonMonoxide float64 `db:"co"`
	Humidity       float64 `db:"humidity"`
	Light          int8    `db:"light"`
	LPG            float64 `db:"lpg"`
	Motion         int8    `db:"motion"`
	Smoke          float64 `db:"smoke"`
	Temperature    float64 `db:"temperature"`
}
