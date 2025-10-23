package client

import "time"

func Timer(do func()) int {
	start := time.Now().UnixMilli()
	do()
	return int(time.Now().UnixMilli() - start)
}

func TimerRows(do func() int) (int, int) {
	start := time.Now().UnixMilli()
	rows := do()
	return int(time.Now().UnixMilli() - start), rows
}
