package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/ethclient"

	"drpc-logs-oracle/db"
)

func background(db_conn *db.Conn, eth *ethclient.Client) {
	for ; ; time.Sleep(time.Minute) {
		err := sync_node(db_conn, eth)
		if err != nil { // ignore errors because we can continue to serve requests
			log.Printf("failed sync with node: %v\n", err)
			continue
		}
	}
}

func sync_node(db_conn *db.Conn, eth *ethclient.Client) error {
	ctx := context.Background()

	start, err := db_conn.GetLastBlock()
	if err != nil {
		return fmt.Errorf("get last indexed block: %v\n", err)
	}

	last, err := eth.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get last block from eth: %v\n", err)
	}

	from, to := start+1, start
	for from <= last {
		wg := sync.WaitGroup{}

		data := make([](*[]db.Log), *crawler_concurency)
		errors := make([]error, *crawler_concurency)

		i := uint64(0)
		for ; i < *crawler_concurency && from <= last; i++ {
			to = from + *crawler_batch
			if to > last {
				to = last
			}

			wg.Add(1)
			go func(i, from, to uint64) {
				defer wg.Done()
				data[i], errors[i] = load_blocks(db_conn, eth, from, to)
			}(i, from, to)

			from = to + 1
		}

		wg.Wait()

		for j := uint64(0); j < *crawler_concurency; j++ {
			if errors[j] != nil {
				return fmt.Errorf("eth_getLogs: %v\n", errors[j])
			}

			if data[j] == nil {
				continue
			}

			err := db_conn.Insert(*data[j])
			if err != nil {
				return fmt.Errorf("insert in db: %v\n", err)
			}
		}
	}

	return nil
}

func load_blocks(db_conn *db.Conn, eth *ethclient.Client, from, to uint64) (*[]db.Log, error) {
	ethLogs, err := eth.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
	})
	if err != nil {
		return nil, err
	}

	logs := make([]db.Log, len(ethLogs))
	for i, log := range ethLogs {
		logs[i].BlockNumber = log.BlockNumber
		logs[i].Address = db.Address(log.Address.Bytes())

		for j, topic := range log.Topics {
			logs[i].Topics[j] = db.Hash(topic.Bytes())
		}
	}

	return &logs, nil
}