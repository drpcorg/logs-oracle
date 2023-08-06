package main

import (
	"fmt"
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"strconv"
)

func makeRPCRequest[Rq any, Rs any](url string, method string, params Rq) (*Rs, error) {
	type RPCRequest struct {
		Id      string `json:"id"`
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  Rq     `json:"params"`
	}

	type RPCError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    any    `json:"data"`
	}

	type RPCResponse struct {
		Id      string    `json:"id"`
		JSONRPC string    `json:"jsonrpc`
		Result  *Rs       `json:"result,omitempty`
		Error   *RPCError `json:"error,omitempty"`
	}

	request := RPCRequest{
		Id:      strconv.Itoa(rand.Int()),
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}
	response := RPCResponse{}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	res, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, fmt.Errorf("json rpc error: [%d] %s", response.Error.Code, response.Error.Message)
	}

	return response.Result, nil
}

// eth_getLogs
type GetLogsParams []struct {
	FromBlock string `json:"fromBlock,omitempty"`
	ToBlock   string `json:"toBlock,omitempty"`
	BlockHash string `json:"blockHash,omitempty"`
	Address   string `json:"address,omitempty"`
	Topics    string `json:"topics,omitempty"`
}

type ETHLog struct {
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
	Address          string   `json:"address"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

type GetLogsResult []ETHLog

func ethGetLogs(url string, params GetLogsParams) (*GetLogsResult, error) {
	return makeRPCRequest[GetLogsParams, GetLogsResult](url, "eth_getLogs", params)
}

type BlockNumberParams []struct{}
type BlockNumberResult string

func ethBlockNumber(url string, params BlockNumberParams) (*BlockNumberResult, error) {
	return makeRPCRequest[BlockNumberParams, BlockNumberResult](url, "eth_blockNumber", params)
}
