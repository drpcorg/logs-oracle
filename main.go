package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

var (
	addr     = flag.String("addr", ":8000", "address to serve")
	endpoint = flag.String("endpoint", "", "ethereum json rpc endpoint url")
	db       = flag.String("db-path", "./index.sqlite", "db path for index")
)

var logs *LogsIndex

type Filter struct {
	BlockHash string   `json:"blockHash,omitempty"`
	FromBlock string   `json:"fromBlock,omitempty"`
	ToBlock   string   `json:"toBlock,omitempty"`
	Address   string   `json:"address,omitempty"`
	Topics    []string `json:"topics,omitempty"`
}

func main() {
	flag.Parse()
	if *endpoint == "" {
		log.Fatal("required -endpoint")
	}

	var logsErr error
	logs, logsErr = MakeLogsIndex(*db, *endpoint)
	if logsErr != nil {
		log.Fatal("failed to load index", logsErr)
	}
	defer logs.Close()

	go logs.Fill()

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filter Filter
		if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		count, err := logs.Calc(filter.FromBlock, filter.ToBlock)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "%d", count)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}
