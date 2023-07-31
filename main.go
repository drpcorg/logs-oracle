package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

var addr = flag.String("addr", ":8000", "address to serve")

type Filter struct {
	BlockHash string   `json:"blockHash"`
	FromBlock string   `json:"fromBlock"`
	ToBlock   string   `json:"toBlock"`
	Address   string   `json:"address"`
	Topics    []string `json:"topics"`
}

func main() {
	flag.Parse()

	http.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		var filter Filter
		if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "%d", 42)
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}
