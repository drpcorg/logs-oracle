package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"drpc-logs-oracle/db"
)

// Filters
type Filter struct {
	FromBlock *string `json:"fromBlock"`
	ToBlock   *string `json:"toBlock"`

	Address interface{}   `json:"address"`
	Topics  []interface{} `json:"topics"`
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

type App struct {
	Config     *Config
	DataClient *db.Conn
	NodeClient *ethclient.Client
}

func main() {
	flag.Parse()
	config, err := LoadConfig()
	if err != nil {
		log.Fatal("Couldn't parse config", err)
	}

	db_conn, err := db.NewDB(config.DataDir, uint64(config.RamLimit))
	if err != nil {
		log.Fatal("Couldn't load db: ", err)
	}
	defer db_conn.Close()

	eth, err := ethclient.DialContext(context.Background(), config.NodeAddr)
	if err != nil {
		log.Fatal("Couln't connect to node: ", err)
	}

	app := &App{
		Config:     &config,
		DataClient: db_conn,
		NodeClient: eth,
	}

	go background(app)

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, db_conn.Status())
	})

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filters []Filter
		if err := json.NewDecoder(r.Body).Decode(&filters); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(filters) != 1 {
			log.Println(err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		filter := filters[0]

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

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", config.BindPort), nil))
}
