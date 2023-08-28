package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"drpc-logs-oracle/db"
)

var (
	addr         = flag.String("addr", ":8000", "address to serve")
	data_dir     = flag.String("data-dir", "", "dir for save data")
	buffer_limit = flag.String("buffer-limit", "16GB", "RAM limit for disk cache")

	rpc_endpoint   = flag.String("rpc-endpoint", "", "ethereum json rpc endpoint url")
	rpc_batch      = flag.Uint64("rpc-batch", 512, "count of block for one request to node")
	rpc_concurency = flag.Uint64("rpc-concurency", 8, "count of simultaneous requests to node")
)

type Filter struct {
	FromBlock *string `json:"fromBlock"`
	ToBlock   *string `json:"toBlock"`

	Address interface{}   `json:"address"`
	Topics  []interface{} `json:"topics"`
}

func (raw *Filter) ToQuery(latest uint64) (*db.Query, error) {
	q, err := db.Query{}, (error)(nil)

	// Block numbers
	q.FromBlock, err = parseBlockNumber(raw.FromBlock, latest)
	if err != nil {
		return nil, fmt.Errorf("invalid record 'fromBlock'")
	}

	q.ToBlock, err = parseBlockNumber(raw.ToBlock, latest)
	if err != nil {
		return nil, fmt.Errorf("invalid record 'toBlock'")
	}

	if q.FromBlock > q.ToBlock {
		return nil, fmt.Errorf("required fromBlock <= toBlock")
	}

	// Address
	if raw.Address != nil {
		switch r := raw.Address.(type) {
		case []interface{}:
			for i, addr := range r {
				if str, ok := addr.(string); ok {
					q.Addresses = append(q.Addresses, db.Address(common.HexToAddress(str)))
				} else {
					return nil, fmt.Errorf("non-string address at index %d", i)
				}
			}

		case string:
			q.Addresses = append(q.Addresses, db.Address(common.HexToAddress(r)))

		default:
			return nil, fmt.Errorf("invalid addresses in query")
		}
	}

	// Topics
	if raw.Topics != nil {
		if len(raw.Topics) > 4 {
			return nil, fmt.Errorf("allowed only 4 topic filters")
		}

		q.Topics = make([][]db.Hash, len(raw.Topics))

		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:

			case string:
				q.Topics[i] = append(q.Topics[i], db.Hash(common.HexToHash(topic)))

			case []interface{}:
				// or case e.g. [null, "topic0", "topic1"]
				for _, rawTopic := range topic {
					if rawTopic == nil {
						q.Topics[i] = nil // null component, match all
						break
					}

					if topic, ok := rawTopic.(string); ok {
						q.Topics[i] = append(q.Topics[i], db.Hash(common.HexToHash(topic)))
					} else {
						return nil, fmt.Errorf("invalid topic(s)")
					}
				}

			default:
				return nil, fmt.Errorf("invalid topic(s)")
			}
		}
	}

	return &q, nil
}

func main() {
	flag.Parse()
	if *data_dir == "" {
		log.Fatal("required -data-dir")
	}
	if *rpc_endpoint == "" {
		log.Fatal("required -rpc-endpoint")
	}

	ram_limit, err := datasizeParse(*buffer_limit)
	if err != nil {
		log.Fatal("invalid -buffer-limit")
	}

	db_conn, err := db.NewDB(*data_dir, ram_limit)
	if err != nil {
		log.Fatal("failed to load db: ", err)
	}
	defer db_conn.Close()

	eth, err := ethclient.DialContext(context.Background(), *rpc_endpoint)
	if err != nil {
		log.Fatal(err)
	}

	go background(db_conn, eth)

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, db_conn.Status())
	})

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filter Filter
		if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// TODO: don't fetch the node every request
		latestBlock, err := eth.BlockNumber(context.Background())
		if err != nil {
			log.Println(err)

			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		query, err := filter.ToQuery(latestBlock)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%v}`, err), http.StatusBadRequest)
			return
		}

		count, err := db_conn.Query(query)
		if err != nil {
			log.Println(err)

			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		fmt.Fprintf(w, `{"result":%d}`+"\n", count)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

func parseBlockNumber(str *string, latest uint64) (uint64, error) {
	if str == nil {
		return latest, nil
	}

	switch *str {
	case "earliest":
		return 0, nil

	// count approximately to the last block :)
	case "", "latest", "safe", "finalized", "pending":
		return latest, nil
	}

	// parse hex string
	value, ok := new(big.Int).SetString(*str, 0)
	if !ok {
		return 0, fmt.Errorf("failed to parse the value")
	}
	return value.Uint64(), nil
}

func datasizeParse(raw string) (uint64, error) {
	var val uint64

	i := 0
	for ; i < len(raw) && '0' <= raw[i] && raw[i] <= '9'; i++ {
		val = val*10 + uint64(raw[i]-'0')
	}
	if i == 0 {
		return 0, &strconv.NumError{"UnmarshalText", raw, strconv.ErrSyntax}
	}

	unit := strings.ToLower(strings.TrimSpace(raw[i:]))
	switch unit {
	case "", "b", "byte":
		// do nothing - already in bytes

	case "k", "kb", "kilo", "kilobyte", "kilobytes":
		val *= 1 << 10
	case "m", "mb", "mega", "megabyte", "megabytes":
		val *= 1 << 20
	case "g", "gb", "giga", "gigabyte", "gigabytes":
		val *= 1 << 30
	case "t", "tb", "tera", "terabyte", "terabytes":
		val *= 1 << 40
	case "p", "pb", "peta", "petabyte", "petabytes":
		val *= 1 << 50
	case "E", "EB", "e", "eb", "eB":
		val *= 1 << 60

	default:
		return 0, &strconv.NumError{"UnmarshalText", raw, strconv.ErrSyntax}
	}

	return val, nil
}
