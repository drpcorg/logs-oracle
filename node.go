package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/rs/zerolog"
)

var (
	SafeBlockNumber      = big.NewInt(-4)
	FinalizedBlockNumber = big.NewInt(-3)
	LatestBlockNumber    = big.NewInt(-2)
	PendingBlockNumber   = big.NewInt(-1)
	EarliestBlockNumber  = big.NewInt(0)
)

type Node struct {
	addr  string
	batch uint64

	safe      atomic.Pointer[big.Int]
	finalized atomic.Pointer[big.Int]
	latest    atomic.Pointer[big.Int]
	pending   atomic.Pointer[big.Int]

	client *ethclient.Client
}

func CreateNode(ctx context.Context, addr string, batch uint64) (*Node, error) {
	eth, err := ethclient.DialContext(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("Couln't connect to node: %w", err)
	}

	node := &Node{
		client: eth,
		batch:  batch,
	}

	if err := node.UpdateHead(ctx); err != nil {
		return nil, err
	}

	return node, nil
}

func (h *Node) SafeBlock() *big.Int {
	return h.safe.Load()
}

func (h *Node) FinalizedBlock() *big.Int {
	return h.finalized.Load()
}

func (h *Node) LatestBlock() *big.Int {
	return h.latest.Load()
}

func (h *Node) PendingBlock() *big.Int {
	return h.pending.Load()
}

func (h *Node) UpdateHead(ctx context.Context) error {
	safe, err := h.client.HeaderByNumber(ctx, SafeBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get safe block: %w", err)
	}

	finalized, err := h.client.HeaderByNumber(ctx, FinalizedBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get finalized block: %w", err)
	}

	latest, err := h.client.HeaderByNumber(ctx, LatestBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get latest block: %w", err)
	}

	pending, err := h.client.HeaderByNumber(ctx, PendingBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get pending block: %w", err)
	}

	h.safe.Store(safe.Number)
	h.finalized.Store(finalized.Number)
	h.latest.Store(latest.Number)
	h.pending.Store(pending.Number)

	return nil
}

func (n *Node) SubscribeNewHead(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}

	log := zerolog.Ctx(ctx)

	ch := make(chan *types.Header)

	var sub ethereum.Subscription
	var err error

	for ; ; time.Sleep(time.Second * 5) {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}

		sub, err = n.client.SubscribeNewHead(ctx, ch)
		if err != nil {
			log.Error().Err(err).Msg("Failed subscribe to NewHead")
		} else {
			break
		}
	}
	defer sub.Unsubscribe()

	for {
		select {
		case err := <-sub.Err():
			log.Error().Err(err).Msg("couldn't get new head")

		case <-ctx.Done():
			return

		case <-ch:
			if err := n.UpdateHead(ctx); err != nil {
				log.Error().Err(err).Msg("couldn't load current head")
			}

			log.Debug().
				Str("safe", n.SafeBlock().String()).
				Str("finalized", n.FinalizedBlock().String()).
				Str("latest", n.LatestBlock().String()).
				Str("pending", n.PendingBlock().String()).
				Msg("new head")
		}
	}
}

func (n *Node) forceLoadLogs(ctx context.Context, start uint64, cl chan<- []types.Log, ce chan<- error) {
	last := n.LatestBlock().Uint64()

	attempt := 0
	for from := start; from <= last && !errors.Is(ctx.Err(), context.Canceled); {
		to := from + n.batch
		if to > last {
			to = last
		}

		data, err := n.client.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(from),
			ToBlock:   new(big.Int).SetUint64(to),
		})

		if err != nil {
			if attempt < 8 {
				attempt++
				time.Sleep(time.Second * 5)

				continue
			} else {
				ce <- fmt.Errorf("couldn't load logs: %w", err)
				return
			}
		} else {
			attempt = 0
		}

		from = to + 1
		if len(data) > 0 {
			cl <- data
		}
	}
}

func (n *Node) SubscribeBlocks(ctx context.Context, wg *sync.WaitGroup, start uint64, cl chan<- []types.Log, ce chan<- error) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}

	log := zerolog.Ctx(ctx)

	n.forceLoadLogs(ctx, start, cl, ce)

	ch := make(chan types.Log)

	var sub ethereum.Subscription
	var err error

	for ; ; time.Sleep(time.Second * 5) {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}

		sub, err = n.client.SubscribeFilterLogs(ctx, ethereum.FilterQuery{}, ch)
		if err != nil {
			log.Error().Err(err).Msg("Failed subscribe to NewHead")
		} else {
			break
		}
	}
	defer sub.Unsubscribe()

	bufferSize := 1024
	buffer := make([]types.Log, 0, bufferSize)

	for {
		select {
		case err := <-sub.Err():
			ce <- err
			return

		case <-ctx.Done():
			return

		case log := <-ch:
			buffer = append(buffer, log)

			if len(buffer) >= bufferSize {
				cl <- buffer

				buffer = make([]types.Log, 0, bufferSize)
			}
		}
	}
}

func (n *Node) Close() {
	n.client.Close()
}
