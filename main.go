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
)

var (
	addr     = flag.String("addr", ":8000", "address to serve")
	endpoint = flag.String("endpoint", "", "ethereum json rpc endpoint url")
	db       = flag.String("dsn", "", "DSN for connect to ClickHouse")
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
	if *db == "" {
		log.Fatal("required -db")
	}
	if *endpoint == "" {
		log.Fatal("required -endpoint")
	}

	db, err := NewDB(*db)
	if err != nil {
		log.Fatal("failed to load index", err)
	}

	go background(db, *endpoint)

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filter Filter
		if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var fromBlock, toBlock *big.Int
		addresses, topics := []Address{}, [][]Hash{}

		if filter.FromBlock != "" {
			ok := false
			fromBlock, ok = new(big.Int).SetString(filter.FromBlock, 0)
			if !ok {
				http.Error(w, "Invalid record 'fromBlock'", http.StatusBadRequest)
				return
			}
		}

		if filter.ToBlock != "" {
			ok := false
			toBlock, ok = new(big.Int).SetString(filter.ToBlock, 0)
			if !ok {
				http.Error(w, "Invalid record 'toBlock'", http.StatusBadRequest)
				return
			}
		}

		if len(filter.Address) > 0 {
			for _, adr := range filter.Address {
				addresses = append(addresses, Address(common.HexToAddress(adr)))
			}
		}

		if len(filter.Topics) > 0 {
			for _, item := range filter.Topics {
				line := []Hash{}
				for _, topic := range item {
					line = append(line, Hash(common.HexToHash(topic)))
				}
				topics = append(topics, line)
			}
		}

		count, err := db.GetLogsCount(
			fromBlock,
			toBlock,
			addresses,
			topics,
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "%d", count)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func background(db *DB, endpoint string) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("paniced crawler: %v\n", err)
		}
	}()

	eth, err := ethclient.DialContext(context.Background(), endpoint)
	if err != nil {
		log.Fatal(err)
	}

	// run crawler every minute
	for {
		syncWithETH(db, eth)

		time.Sleep(time.Minute)
	}
}

func syncWithETH(db *DB, eth *ethclient.Client) {
	// get last loaded block number
	start, err := db.GetLastIndexedBlockNumber()
	if err != nil {
		log.Printf("index: %v\n", err)
		return
	}

	// get last block in node
	last, err := eth.BlockNumber(context.Background())
	if err != nil {
		log.Printf("index: %v\n", err)
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
					continue;
				}
				db.AddLogs(logs)
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

func getBlocksLogs(eth *ethclient.Client, blockNumber uint64) (*[]Log, error) {
	ethLogs, err := eth.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(blockNumber),
		ToBlock:   new(big.Int).SetUint64(blockNumber + *batch - 1),
	})
	if err != nil {
		return nil, err
	}

	logs := make([]Log, 0, len(ethLogs))

	for _, it := range ethLogs {
		log := Log{
			BlockNumber: it.BlockNumber,
			Address:     Address(it.Address.Bytes()),
		}

		for i, t := range it.Topics {
			log.Topics[i] = Hash(t.Bytes()) // not more 4
		}

		logs = append(logs, log)
	}

	return &logs, nil
}
