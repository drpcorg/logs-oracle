package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"vethin/db"
)

var (
	addr     = flag.String("addr", ":8000", "address to serve")
	endpoint = flag.String("endpoint", "", "ethereum json rpc endpoint url")
	dsn      = flag.String("dsn", "", "DSN for connect to ClickHouse")
	batch    = flag.Uint64("batch-size", 512, "count of block for one request to node")
)

type Filter struct {
	BlockHash string     `json:"blockHash,omitempty"`
	FromBlock string     `json:"fromBlock,omitempty"`
	ToBlock   string     `json:"toBlock,omitempty"`
	Address   []string   `json:"address,omitempty"`
	Topics    [][]string `json:"topics,omitempty"`
}

func main() {
	flag.Parse()
	if *dsn == "" {
		log.Fatal("required -dsn")
	}
	if *endpoint == "" {
		log.Fatal("required -endpoint")
	}

	db_conn, err := db.New(*dsn)
	if err != nil {
		log.Fatal("failed to load db", err)
	}
	defer db_conn.Close()

	eth, err := ethclient.DialContext(context.Background(), *endpoint)
	if err != nil {
		log.Fatal(err)
	}

	go background(db_conn, eth)

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filter Filter
		if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// parse params
		fromBlock, err := parseBlockNumber(eth, filter.FromBlock)
		if err != nil {
			http.Error(w, "Invalid record 'fromBlock'", http.StatusBadRequest)
			return
		}

		toBlock, err := parseBlockNumber(eth, filter.ToBlock)
		if err != nil {
			http.Error(w, "Invalid record 'toBlock'", http.StatusBadRequest)
			return
		}

		addresses := make([]db.Address, len(filter.Address))
		for i, adr := range filter.Address {
			addresses[i] = db.Address(common.HexToAddress(adr))
		}

		topics := make([][]db.Hash, len(filter.Topics))
		for i, item := range filter.Topics {
			for _, topic := range item {
				topics[i] = append(topics[i], db.Hash(common.HexToHash(topic)))
			}
		}

		// validate params
		if fromBlock.Cmp(&toBlock) == 1 {
			http.Error(w, "required fromBlock <= toBlock", http.StatusBadRequest)
			return
		}

		if len(topics) > 4 {
			http.Error(w, "allowed only 4 topic filters", http.StatusBadRequest)
			return
		}

		// calc count
		count, err := db_conn.GetLogsCount(fromBlock, toBlock, addresses, topics)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		fmt.Fprintf(w, `{"result":%d}` + "\n", count)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func parseBlockNumber(eth *ethclient.Client, str string) (big.Int, error) {
	var result big.Int

	switch str {
	// count approximately to the last block :)
	case "", "earliest", "latest", "safe", "finalized", "pending":
		value, err := eth.BlockNumber(context.Background())
		if err != nil {
			return result, err
		}

		result.SetUint64(value)
		return result, nil

	// parse hex string
	default:
		value, ok := new(big.Int).SetString(str, 0)
		if !ok {
			return result, fmt.Errorf("failed to parse the value")
		}
		return *value, nil
	}

	return result, nil
}

func background(db_conn *db.Conn, eth *ethclient.Client) {
	ctx := context.Background()

	// ignore errors because we can continue to serve requests
	for ; ; time.Sleep(time.Minute) {
		start, err := db_conn.GetLastBlock()
		if err != nil {
			log.Printf("get last indexed block: %v\n", err)
			continue
		}

		last, err := eth.BlockNumber(ctx)
		if err != nil {
			log.Printf("get last block from eth: %v\n", err)
			continue
		}

		for from, to := start + 1, start; from <= last; from = to + 1 {
			to = from + *batch

			ethLogs, err := eth.FilterLogs(ctx, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(from),
				ToBlock:   new(big.Int).SetUint64(to),
			})
			if err != nil {
				log.Println(err)
				break
			}

			logs := make([]db.Log, len(ethLogs))
			for i, log := range ethLogs {
				logs[i].BlockNumber = log.BlockNumber
				logs[i].Address = db.Address(log.Address.Bytes())

				for j, topic := range log.Topics {
					logs[i].Topics[j] = db.Hash(topic.Bytes())
				}
			}

			if err := db_conn.Insert(logs); err != nil {
				log.Println(err)
				break
			}
		}
	}
}
