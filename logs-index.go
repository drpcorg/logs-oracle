package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type LogsIndex struct {
	dsn string
	api string

	mu_ sync.Mutex
	db_ *sql.DB
}

func MakeLogsIndex(dsn, api string) (*LogsIndex, error) {
	index := LogsIndex{dsn: dsn, api: api}

	index.mu_.Lock()
	defer index.mu_.Unlock()

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	index.db_ = db

	initSQL := `
		CREATE TABLE IF NOT EXISTS Blocks (
			Number       INT  PRIMARY KEY,
			Count        INT  NOT NULL,
			TopicsBloom  BLOB NOT NULL,
			AddressBloom BLOB NOT NULL
		);
	`

	if _, err := db.Exec(initSQL); err != nil {
		return nil, err
	}

	return &index, nil
}

func intToHex(n int) string {
	return fmt.Sprintf("0x%x", n)
}

func hexToInt(n string) (int64, error) {
	return strconv.ParseInt(strings.Replace(n, "0x", "", -1), 16, 64)
}

func (index LogsIndex) IndexSegment(from, to int) (int, error) {
	params := GetLogsParams{{FromBlock: intToHex(from), ToBlock: intToHex(from + 100)}}

	logs, err := ethGetLogs(index.api, params)
	if err != nil {
		return 0, err
	}

	logsMap := make(map[int64]int)

	for _, it := range *logs {
		number, err := hexToInt(it.BlockNumber)
		if err != nil {
			return 0, err
		}

		logsMap[number]++
	}

	index.mu_.Lock()
	defer index.mu_.Unlock()

	tx, err := index.db_.Begin()
	if err != nil {
		return 0, err
	}

	stmt, err := tx.Prepare(`INSERT INTO Blocks(Number, Count, TopicsBloom, AddressBloom) VALUES(?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for number, count := range logsMap {
		_, err = stmt.Exec(number, count, 0, 0)
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(logsMap), nil
}

func (index LogsIndex) GetLastIndexedBlockNumber() (int64, error) {
	index.mu_.Lock()
	defer index.mu_.Unlock()

	var last int64
	rows, err := index.db_.Query(`SELECT MAX(NUMBER) FROM Blocks`)
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		rows.Scan(&last)
	}

	return last, nil
}

func (index LogsIndex) Fill() {
	blockNumber, err := ethBlockNumber(index.api, BlockNumberParams{})
	if err != nil {
		fmt.Printf("[ERROR] failed start index: %v\n", err)
		return
	}

	last, err := index.GetLastIndexedBlockNumber()
	if err != nil {
		fmt.Printf("[ERROR] failed start index: %v\n", err)
		return
	}

	max, err := hexToInt(string(*blockNumber))
	if err != nil {
		fmt.Printf("[ERROR] failed start index: %v\n", err)
		return
	}

	from := int(last) + 1
	for from < int(max) {
		to := from + 100

		count, err := index.IndexSegment(from, to)
		if err != nil {
			fmt.Printf("[ERROR] failed index segment from %d to %d:\n %v\n", from, to, err)
		} else {
			fmt.Printf("[LOG] added %d blocks in index\n", count)
		}

		from = to + 2
	}
}

func (index LogsIndex) Calc(from, to string) (int64, error) {
	index.mu_.Lock()
	defer index.mu_.Unlock()

	fromNumber, err := hexToInt(from)
	if err != nil {
		return 0, err
	}

	toNumber, err := hexToInt(to)
	if err != nil {
		return 0, err
	}

	rows, err := index.db_.Query(`SELECT SUM(Count) FROM Blocks WHERE Number >= ? AND Number <= ?`, fromNumber, toNumber)
	if err != nil {
		return 0, err
	}

	var count int64
	for rows.Next() {
		rows.Scan(&count)
	}

	return count, nil
}

func (index LogsIndex) Close() {
	index.db_.Close()
}
