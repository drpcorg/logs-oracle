package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/constraints"
)

var limit = flag.Int("limit", math.MaxInt, "count of testsuites")
var workers = flag.Int("workers", 8, "count of workers")
var nodeRpc = flag.String("node-rpc", "", "node http rpc URL")
var oracleRpc = flag.String("oracle-rpc", "", "logs-oracle http URL")

//go:embed fixtures.jsonl
var fixtures string

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Caller().Logger()

	if *nodeRpc == "" {
		log.Fatal().Msg("required node-rpc flag")
	}

	if *oracleRpc == "" {
		log.Fatal().Msg("required oracle-rpc flag")
	}

	requests := strings.Split(strings.Trim(fixtures, "\n"), "\n")

	order := rand.Perm(len(requests))
	count := min(len(requests), *limit)

	var wg sync.WaitGroup
	jobs := make(chan string, *workers)

	for w := 1; w <= *workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for params := range jobs {
				fromNode, err := nodeRequest(params)
				if err != nil {
					log.Panic().Err(err).Msg("failed node request")
				}

				fromOracle, err := oracleRequest(params)
				if err != nil {
					log.Panic().Err(err).Msg("failed oracle request")
				}

				if fromNode != fromOracle {
					log.Panic().Msgf("different results: real=%d, estimate=%d, params=%s", fromNode, fromOracle, params)
				}

				log.Info().
					Str("params", params).
					Int("count", fromOracle).
					Msgf("suite estimate matched")
			}
		}()
	}

	for i := 0; i < count; i++ {
		jobs <- requests[order[i]]
	}

	close(jobs)

	wg.Wait()

	log.Info().Msgf("%d suites successfully finished", count)
}

func request(url string, method string, params string) (any, error) {
	type Error struct {
		Code    int     `json:"code"`
		Message string  `json:"message"`
		Data    *string `json:"data,omitempty"`
	}

	type Response struct {
		Id      int    `json:"id"`
		JsonRpc string `json:"jsonrpc"`
		Result  *any   `json:"result"`
		Error   *Error `json:"error"`
	}

	payload := fmt.Sprintf("{\"method\":\"%s\",\"params\":%s,\"id\":1,\"jsonrpc\":\"2.0\"}", method, params)

	conn, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return "", err
	}

	body, err := io.ReadAll(conn.Body)
	conn.Body.Close()

	if conn.StatusCode > 299 {
		return "", fmt.Errorf("Response failed: status=%d, body=%s", conn.StatusCode, body)
	}

	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}

	if response.Error != nil {
		return "", fmt.Errorf("Response error: code=%d, message=%s", response.Error.Code, response.Error.Message)
	}

	return *(response.Result), nil
}

func nodeRequest(params string) (int, error) {
	response, err := request(*nodeRpc, "eth_getLogs", params)
	if err != nil {
		return 0, err
	}

	logs, ok := response.([]any)
	if !ok {
		return 0, fmt.Errorf("logs is not an array: %v", response)
	}

	return len(logs), nil
}

func oracleRequest(params string) (int, error) {
	response, err := request(*oracleRpc, "drpc_getLogsEstimate", params)
	if err != nil {
		return 0, err
	}

	estimate, ok := response.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("estimate is not an object: %v", response)
	}

	return int(estimate["total"].(float64)), nil
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	if a > b {
		return b
	}
	return a
}
