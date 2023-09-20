package main

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"

	"drpc-logs-oracle/db"
)

func background(ctx context.Context, app *App) {
	log := zerolog.Ctx(ctx)

	for ; ; time.Sleep(time.Second * 5) {
		err := sync_node(app)
		if err != nil { // ignore errors because we can continue to serve requests
			log.Error().Err(err).Msg("Failed sync with node")
			continue
		}
	}
}

func sync_node(app *App) error {
	ctx := context.Background()

	start, err := app.DataClient.GetLastBlock()
	if err != nil {
		return fmt.Errorf("get last indexed block: %v\n", err)
	}

	last, err := app.NodeClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get last block from eth: %v\n", err)
	}

	batch, concurency := app.Config.NodeBatch, app.Config.NodeConcurency

	from, to := start, start
	for from <= last {
		wg := sync.WaitGroup{}

		data := make([](*[]db.Log), concurency)
		errors := make([]error, concurency)

		i := uint64(0)
		for ; i < concurency && from <= last; i++ {
			to = from + batch
			if to > last {
				to = last
			}

			wg.Add(1)
			go func(i, from, to uint64) {
				defer wg.Done()
				data[i], errors[i] = load_blocks(app.DataClient, app.NodeClient, from, to)
			}(i, from, to)

			from = to + 1
		}

		wg.Wait()

		for j := uint64(0); j < concurency; j++ {
			if errors[j] != nil {
				return fmt.Errorf("eth_getLogs: %v\n", errors[j])
			}

			if data[j] == nil {
				continue
			}

			err := app.DataClient.Insert(*data[j])
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
