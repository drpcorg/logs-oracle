package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"sync"
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
	batch    = flag.Uint64("batch-size", 256, "count of block for one request to node")
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

		addresses, topics := []db.Address{}, [][]db.Hash{}

		if len(filter.Address) > 0 {
			for _, adr := range filter.Address {
				addresses = append(addresses, db.Address(common.HexToAddress(adr)))
			}
		}

		if len(filter.Topics) > 0 {
			for _, item := range filter.Topics {
				line := []db.Hash{}
				for _, topic := range item {
					line = append(line, db.Hash(common.HexToHash(topic)))
				}
				topics = append(topics, line)
			}
		}

		count, err := db_conn.GetLogsCount(
			fromBlock,
			toBlock,
			addresses,
			topics,
		)

		fmt.Println(count)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "%d", count)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func parseBlockNumber(eth *ethclient.Client, str string) (big.Int, error) {
	var result big.Int

	if str == "" {
		str = "latest"
	}

	switch str {
	// count approximately to the last block :)
	case "earliest", "latest", "safe", "finalized", "pending":
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
	defer func() {
		if err := recover(); err != nil {
			log.Printf("paniced crawler: %v\n", err)
		}
	}()

	// run crawler every minute
	for {
		syncWithETH(db_conn, eth)

		time.Sleep(time.Minute)
	}
}

func syncWithETH(db_conn *db.Conn, eth *ethclient.Client) {
	// get last loaded block number
	start, err := db_conn.GetLastBlock()
	if err != nil {
		log.Printf("get last indexed block: %v\n", err)
		return
	}

	// get last block in node
	last, err := eth.BlockNumber(context.Background())
	if err != nil {
		log.Printf("get last block in eth: %v\n", err)
		return
	}

	ch := make(chan uint64)
	wg := sync.WaitGroup{}

	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			for number := range ch {
				logs, err := getBlocksLogs(eth, number)
				if err != nil {
					log.Printf("index: failed load '%d' block:\n %v\n", number, err)
					continue
				}
				db_conn.Insert(logs)
			}

			wg.Done()
		}()
	}

	for number := start + 1; number <= last; number += *batch {
		ch <- number
	}

	close(ch)
	wg.Wait()
}

func getBlocksLogs(eth *ethclient.Client, blockNumber uint64) (*[]db.Log, error) {
	ethLogs, err := eth.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(blockNumber),
		ToBlock:   new(big.Int).SetUint64(blockNumber + *batch - 1),
	})
	if err != nil {
		return nil, err
	}

	logs := make([]db.Log, 0, len(ethLogs))

	for _, it := range ethLogs {
		log := db.Log{
			BlockNumber: it.BlockNumber,
			Address:     db.Address(it.Address.Bytes()),
		}

		for i, t := range it.Topics {
			log.Topics[i] = db.Hash(t.Bytes()) // not more 4
		}

		logs = append(logs, log)
	}

	return &logs, nil
}
