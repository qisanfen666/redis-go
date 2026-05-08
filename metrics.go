package main

import (
	"expvar"
	"time"
)

var (
	OpsTotal   = expvar.NewMap("redis_ops")
	HitTotal   = expvar.NewInt("redis_key_hit")
	MissTotal  = expvar.NewInt("redis_key_miss")
	DurationUs = expvar.NewMap("redis_duration_us")
)

func recordCmd(cmd string, dur time.Duration, hit bool) {
	OpsTotal.Add(cmd, 1)
	if hit {
		HitTotal.Add(1)
	} else {
		MissTotal.Add(1)
	}
	DurationUs.Add(cmd, dur.Microseconds())
}
