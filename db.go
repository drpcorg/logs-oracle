package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"math/big"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

//go:embed db.sql
var dbInitSQL string

type DB struct {
	db clickhouse.Conn
}

type Address [20]byte
type Hash [32]byte

type Log struct {
	BlockNumber uint64
	Address     Address
	Topics      [4]Hash
}

func NewDB(dsn string) (*DB, error) {
	ctx := context.Background()

	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{dsn},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return nil, err
	}

	if err := db.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, fmt.Errorf("exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}

		return nil, err
	}

	if err := db.Exec(ctx, dbInitSQL); err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

func (db DB) AddLogs(logs *[]Log) error {
	if len(*logs) <= 0 {
		return nil
	}

	batch, err := db.db.PrepareBatch(context.Background(), `INSERT INTO Logs`)
	if err != nil {
		return err
	}

	for _, log := range *logs {
		topics := []string{}
		for _, t := range log.Topics {
			topics = append(topics, string(t[:]))
		}

		err := batch.Append(log.BlockNumber, string(log.Address[:]), topics)
		if err != nil {
			return err
		}
	}

	if err = batch.Send(); err != nil {
		return err
	}

	return nil
}

func (db DB) GetLastIndexedBlockNumber() (uint64, error) {
	var last uint64
	err := db.db.QueryRow(context.Background(), `SELECT MAX(BlockNumber) FROM Logs`).Scan(&last)

	switch {
	case err == sql.ErrNoRows:
		return 0, nil

	case err != nil:
		return 0, err
	}

	return last + 1, nil
}

func (db DB) GetLogsCount(
	fromBlock, toBlock *big.Int,
	addresses []Address,
	topics [][]Hash,
) (int64, error) {
	filters := []string{}
	args := []interface{}{}

	if fromBlock != nil {
		args = append(args, fromBlock)
		filters = append(filters, "BlockNumber >= ?")
	}
	if toBlock != nil {
		args = append(args, toBlock)
		filters = append(filters, "BlockNumber <= ?")
	}
	if len(addresses) > 0 {
		addrs := make([]string, 0, len(addresses))
		for _, t := range addresses {
			addrs = append(addrs, string(t[:]))
		}

		args = append(args, addrs)
		filters = append(filters, "has(?, Address)")
	}
	if len(topics) > 0 {
		args = append(args, topics)
		filters = append(filters, "hasAny(?, Topics)")
	}

	request := `SELECT Count(*) FROM Logs`
	if len(filters) > 0 {
		request += " WHERE " + strings.Join(filters, " AND ")
	}

	rows, err := db.db.Query(context.Background(), request, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count sql.NullInt64
	for rows.Next() {
		rows.Scan(&count)
	}

	return count.Int64, nil
}
