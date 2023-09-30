package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	liboracle "github.com/p2p-org/drpc-logs-oracle"
)

type App struct {
	Config *Config
	Node   *Node
	Db     *liboracle.Conn
}

func CreateApp(ctx context.Context) (*App, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse config: %w", err)
	}

	db_conn, err := liboracle.NewDB(config.DataDir, uint64(config.RamLimit))
	if err != nil {
		return nil, fmt.Errorf("Couldn't load db: %w", err)
	}

	node, err := CreateNode(ctx, config.NodeAddr, config.NodeBatch)
	if err != nil {
		return nil, err
	}

	app := &App{
		Config: &config,
		Node:   node,
		Db:     db_conn,
	}
	return app, nil
}

func (app *App) Close() {
	app.Db.Close()
	app.Node.Close()
}

func initLogger(dev bool) {
	var logger zerolog.Logger

	if dev {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
			Level(zerolog.DebugLevel).
			With().Timestamp().Caller().Int("pid", os.Getpid()).
			Logger()
	} else {
		zerolog.TimeFieldFormat = time.RFC3339Nano

		logger = zerolog.New(os.Stdout).
			Level(zerolog.InfoLevel).
			With().Timestamp().Caller().
			Logger()
	}

	log.Logger = logger
	zerolog.DefaultContextLogger = &logger
}

func main() {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	// Prepare env
	flag.Parse()

	app, err := CreateApp(ctx)
	if err != nil {
		panic(err.Error())
	}
	defer app.Close()

	initLogger(app.Config.IsDev())

	// Background
	go app.Node.SubscribeNewHead(ctx, &wg)

	go func() {
		wg.Add(1)
		defer wg.Done()

		start := app.Db.GetBlocksCount()

		errs := make(chan error)
		logs := make(chan []types.Log, 64)
		go app.Node.SubscribeBlocks(ctx, &wg, start, logs, errs)

		for {
			select {
			case err := <-errs:
				log.Error().Err(err).Msg("Failed sync with node")
				continue

			case data := <-logs:
				dbLogs := make([]liboracle.Log, len(data))

				for i, log := range data {
					dbLogs[i].BlockNumber = log.BlockNumber
					dbLogs[i].Address = liboracle.Address(log.Address.Bytes())

					for j, topic := range log.Topics {
						dbLogs[i].Topics[j] = liboracle.Hash(topic.Bytes())
					}
				}

				if err := app.Db.Insert(dbLogs); err != nil {
					log.Error().Err(err).Msg("couldn't insert logs in db")
					return
				} else {
					log.Debug().Int("count", len(dbLogs)).Msg("inserted new logs in db")
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// Metrics server
	metrics := echo.New()
	metrics.HidePort = true
	metrics.HideBanner = true

	metrics.GET("/metrics", echoprometheus.NewHandler())

	go func() {
		wg.Add(1)
		defer wg.Done()

		if err := metrics.Start(fmt.Sprintf(":%d", app.Config.MetricsPort)); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				log.Debug().Msg("metrics server / shutdowned")
			} else {
				log.Error().Err(err).Msg("metrics server / failed")
			}
		}
	}()

	// RPC Server
	e := echo.New()

	e.Debug = app.Config.IsDev()
	e.HidePort = true
	e.HideBanner = true

	e.Use(middleware.Recover())
	e.Use(middleware.Decompress())
	e.Use(echoprometheus.NewMiddleware("oracle"))

	if app.Config.AccessLog {
		e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
			HandleError: true,

			LogLatency:   true,
			LogProtocol:  true,
			LogRemoteIP:  true,
			LogHost:      true,
			LogMethod:    true,
			LogURI:       true,
			LogRequestID: true,
			LogStatus:    true,
			LogError:     true,

			LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
				var clog *zerolog.Event
				if v.Error == nil {
					clog = log.Info()
				} else {
					clog = log.Error().Err(v.Error)
				}

				clog.
					Str("latency", v.Latency.String()).
					Str("protocol", v.Protocol).
					Str("remote_ip", v.RemoteIP).
					Str("host", v.Host).
					Str("method", v.Method).
					Str("uri", v.URI).
					Str("request_id", v.RequestID).
					Int("status", v.Status).
					Msg("@@accesslog@@")

				return nil
			},
		}))
	}

	e.GET("/status", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf(
			"blocks_count: %lu\nlogs_count:  %lu\n",
			app.Db.GetBlocksCount(),
			app.Db.GetLogsCount(),
		))
	})
	e.POST("/rpc", func(c echo.Context) error {
		return handleHttp(c, app)
	})

	go func() {
		wg.Add(1)
		defer wg.Done()

		if err := e.Start(fmt.Sprintf(":%d", app.Config.BindPort)); err != nil {
			if err == http.ErrServerClosed {
				log.Debug().Msg("rpc server / shutdowned")
			} else {
				log.Error().Err(err).Msg("rpc server / failed")
			}
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	go func() {
		time.Sleep(60 * time.Second)
		os.Exit(1) // force exit after timeout
	}()

	cancel() // notifying everyone about the shutdown

	if err := e.Shutdown(ctx); err != nil { // wait rpc server
		log.Error().Err(err).Msg("couldn't shutdown rpc server")
	}

	if err := metrics.Shutdown(ctx); err != nil { // wait metrics server
		log.Error().Err(err).Msg("couldn't shutdown metrics server")
	}

	wg.Wait() // wait background workers

	log.Info().Msg("server stopped")
}

type Filter struct {
	FromBlock *string `json:"fromBlock"`
	ToBlock   *string `json:"toBlock"`

	Address interface{}   `json:"address"`
	Topics  []interface{} `json:"topics"`
}

func parseBlockNumber(str *string, node *Node) (uint64, error) {
	if str == nil {
		return node.LatestBlock().Uint64(), nil
	}

	switch *str {
	case "earliest":
		return 0, nil
	case "", "latest":
		return node.LatestBlock().Uint64(), nil
	case "safe":
		return node.SafeBlock().Uint64(), nil
	case "finalized":
		return node.FinalizedBlock().Uint64(), nil
	case "pending":
		return node.PendingBlock().Uint64(), nil
	}

	// parse hex string
	value, ok := new(big.Int).SetString(*str, 0)
	if !ok {
		return 0, fmt.Errorf("parse error")
	}

	return value.Uint64(), nil
}

func (raw *Filter) ToQuery(node *Node) (*liboracle.Query, error) {
	q, err := liboracle.Query{}, (error)(nil)

	// Block numbers
	q.FromBlock, err = parseBlockNumber(raw.FromBlock, node)
	if err != nil {
		return nil, fmt.Errorf("parse error 'fromBlock'")
	}

	q.ToBlock, err = parseBlockNumber(raw.ToBlock, node)
	if err != nil {
		return nil, fmt.Errorf("parse error 'toBlock'")
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
					q.Addresses = append(q.Addresses, liboracle.Address(common.HexToAddress(str)))
				} else {
					return nil, fmt.Errorf("non-string address at index %d", i)
				}
			}

		case string:
			q.Addresses = append(q.Addresses, liboracle.Address(common.HexToAddress(r)))

		default:
			return nil, fmt.Errorf("invalid addresses in query")
		}
	}

	// Topics
	if raw.Topics != nil {
		if len(raw.Topics) > 4 {
			return nil, fmt.Errorf("allowed only 4 topic filters")
		}

		q.Topics = make([][]liboracle.Hash, len(raw.Topics))

		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:

			case string:
				q.Topics[i] = append(q.Topics[i], liboracle.Hash(common.HexToHash(topic)))

			case []interface{}:
				// or case e.g. [null, "topic0", "topic1"]
				for _, rawTopic := range topic {
					if rawTopic == nil {
						q.Topics[i] = nil // null component, match all
						break
					}

					if topic, ok := rawTopic.(string); ok {
						q.Topics[i] = append(q.Topics[i], liboracle.Hash(common.HexToHash(topic)))
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

type (
	Request []Filter

	Response struct {
		Result *uint64 `json:"result,omitempty"`
		Error  *string `json:"error,omitempty"`
	}
)

func CreateResponseError(message string) Response {
	return Response{Error: &message}
}

func handleHttp(c echo.Context, app *App) error {
	ctx := c.Request().Context()
	log := zerolog.Ctx(ctx)

	var filters Request
	if err := c.Bind(&filters); err != nil {
		log.Error().Err(err).Msg("Couldn't parse request")
		return c.JSON(http.StatusBadRequest, CreateResponseError("parse error"))
	}

	if len(filters) != 1 {
		return c.JSON(http.StatusBadRequest, CreateResponseError("too many arguments, want at most 1"))
	}

	query, err := filters[0].ToQuery(app.Node)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't convert request to internal filter")
		return c.JSON(http.StatusInternalServerError, CreateResponseError(err.Error()))
	}

	result, err := app.Db.Query(query)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't query in db")
		return c.JSON(http.StatusInternalServerError, CreateResponseError("internal server error"))
	}

	return c.JSON(http.StatusOK, Response{Result: &result})
}
