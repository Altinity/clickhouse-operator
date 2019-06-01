# ClickHouse [![Build Status](https://travis-ci.org/mailru/go-clickhouse.svg?branch=master)](https://travis-ci.org/mailru/go-clickhouse) [![Go Report Card](https://goreportcard.com/badge/github.com/mailru/go-clickhouse)](https://goreportcard.com/report/github.com/mailru/go-clickhouse) [![Coverage Status](https://coveralls.io/repos/github/mailru/go-clickhouse/badge.svg?branch=master)](https://coveralls.io/github/mailru/go-clickhouse?branch=master)

Yet another Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/)

## Key features

* Uses official http interface
* Compatibility with [dbr](https://github.com/mailru/dbr)

## DSN
```
schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN
```
### parameters
* timeout - is the maximum amount of time a dial will wait for a connect to complete
* idle_timeout - is the maximum amount of time an idle (keep-alive) connection will remain idle before closing itself.
* read_timeout - specifies the amount of time to wait for a server's response
* location - timezone to parse Date and DateTime
* debug - enables debug logging
* other clickhouse options can be specified as well (except default_format)

example:
```
http://user:password@host:8123/clicks?read_timeout=10&write_timeout=20
```

## Supported data types

* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* Float32, Float64
* String
* FixedString(N)
* Date
* DateTime
* Enum
* [Array(T) (one-dimensional)](https://clickhouse.yandex/reference_en.html#Array(T))

Note:
database/sql does not allow to use big uint64 values.
It is recommended use type `UInt64` which is provided by driver for such kind of values.

## Install
```
go get -u github.com/mailru/go-clickhouse
```

## Example
```go
package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/mailru/go-clickhouse"
)

func main() {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123/default")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}

	tx, err := connect.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			clickhouse.Date(time.Now()),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}
}
```

Use [dbr](https://github.com/mailru/dbr)

```go
package main

import (
	"log"
	"time"

	_ "github.com/mailru/go-clickhouse"
	"github.com/mailru/dbr"
)

func main() {
	connect, err := dbr.Open("clickhouse", "http://127.0.0.1:8123/default", nil)
	if err != nil {
		log.Fatal(err)
	}
	var items []struct {
		CountryCode string    `db:"country_code"`
		OsID        uint8     `db:"os_id"`
		BrowserID   uint8     `db:"browser_id"`
		Categories  []int16   `db:"categories"`
		ActionTime  time.Time `db:"action_time"`
	}
	sess := connect.NewSession(nil)
	query := sess.Select("country_code", "os_id", "browser_id", "categories", "action_time").From("example")
	query.Where(dbr.Eq("country_code", "RU"))
	if _, err := query.Load(&items); err != nil {
		log.Fatal(err)
	}

	for _, item := range items {
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_time: %s", item.CountryCode, item.OsID, item.BrowserID, item.Categories, item.ActionTime)
	}
}
```
