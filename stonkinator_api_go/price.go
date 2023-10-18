package main

import (
	"time"
)

type Price struct {
	InstrumentId string `json:"instrument_id"`
	Symbol string `json:"symbol"`
	DateTime time.Time `json:"date_time"`
	Open int32 `json:"open"`
	High int32 `json:"high"`
	Low int32 `json:"low"`
	Close int32 `json:"close"`
	Volume int64 `json:"volume"`
}
