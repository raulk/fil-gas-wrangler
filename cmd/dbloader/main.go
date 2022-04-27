package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"

	"github.com/raulk/fil-gas-wrangler/pkg/model"
)

func main() {
	flag.Parse()
	var (
		pathOrig = flag.Arg(0)
		pathDb   = flag.Arg(1)
	)

	db, err := sqlx.Open("sqlite3", pathDb+"?cache=shared&mode=rwc&_journal_mode=WAL")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	const createDDL string = `
	CREATE TABLE IF NOT EXISTS traces (
	    trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
	    message_id INT NOT NULL,
		context_id INT NOT NULL,
		point_id INT NOT NULL,
		elapsed_rel_ns INT NOT NULL,
		elapsed_cum_ns INT NOT NULL,
		fuel_consumed INT,
		gas_consumed INT,
		FOREIGN KEY(context_id) REFERENCES contexts(id),
		FOREIGN KEY(point_id) REFERENCES points(id)
	);

	CREATE TABLE IF NOT EXISTS contexts (
		id INTEGER NOT NULL PRIMARY KEY,
		code_cid text,
		method_num numeric,
		UNIQUE (code_cid, method_num)
	);

	CREATE TABLE IF NOT EXISTS points (
		id INTEGER NOT NULL PRIMARY KEY,
		event text,
		label text,
		UNIQUE (event, label)
	);
`

	if _, err := db.Exec(createDDL); err != nil {
		panic(err)
	}

	contexts := loadContexts(db)
	points := loadPoints(db)

	orig, err := os.Open(pathOrig)
	if err != nil {
		panic(err)
	}
	defer orig.Close()

	insert, err := db.Preparex("INSERT INTO traces(message_id, context_id, point_id, elapsed_rel_ns, elapsed_cum_ns, fuel_consumed, gas_consumed) VALUES(?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(orig)

	var i uint
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 512*1024*1024)
	for scanner.Scan() {
		scanner.Bytes()
		var traces model.Spans
		if err := json.Unmarshal(scanner.Bytes(), &traces); err != nil {
			fmt.Printf("skipping line %d: %s\n", i, err)
			continue
		}

		tx, err := db.Beginx()
		if err != nil {
			panic(err)
		}

		for _, t := range traces {
			var context uint
			var point uint
			var ok bool

			if context, ok = contexts[t.Context]; !ok {
				res, err := tx.Exec("INSERT INTO contexts(code_cid, method_num) VALUES(?,?)", t.Context.CodeCID, t.Context.MethodNum)
				if err != nil {
					panic(err)
				}
				id, err := res.LastInsertId()
				if err != nil {
					panic(err)
				}
				contexts[t.Context] = uint(id)
				context = uint(id)
			}

			if point, ok = points[t.Point]; !ok {
				res, err := tx.Exec("INSERT INTO points(event, label) VALUES(?,?)", t.Point.Event, t.Point.Label)
				if err != nil {
					panic(err)
				}
				id, err := res.LastInsertId()
				if err != nil {
					panic(err)
				}
				points[t.Point] = uint(id)
				point = uint(id)
			}

			_, err := tx.Stmtx(insert).Exec(
				i,
				context,
				point,
				t.Timing.ElapsedRelNs,
				t.Timing.ElapsedCumNs,
				t.Consumption.FuelConsumed,
				t.Consumption.GasConsumed)
			if err != nil {
				panic(err)
			}
		}

		if err := tx.Commit(); err != nil {
			panic(err)
		}

		i++
		fmt.Printf("processed line %d\n", i)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("scanner finished error: %s\n", err)
	}

}

func loadContexts(db *sqlx.DB) map[model.Context]uint {
	ret := map[model.Context]uint{}

	type context struct {
		Id uint
		model.Context
	}

	var contexts []context
	if err := db.Select(&contexts, "SELECT * FROM contexts"); err != nil {
		panic(err)
	}
	for _, context := range contexts {
		ret[context.Context] = context.Id
	}

	return ret
}

func loadPoints(db *sqlx.DB) map[model.Point]uint {
	ret := map[model.Point]uint{}

	type point struct {
		Id uint
		model.Point
	}

	var points []point
	if err := db.Select(&points, "SELECT * FROM points"); err != nil {
		panic(err)
	}
	for _, point := range points {
		ret[point.Point] = point.Id
	}

	return ret
}
