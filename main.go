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
	"time"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"drpc-logs-oracle/db"
)

var (
	SafeBlockNumber      = big.NewInt(-4)
	FinalizedBlockNumber = big.NewInt(-3)
	LatestBlockNumber    = big.NewInt(-2)
	PendingBlockNumber   = big.NewInt(-1)
	EarliestBlockNumber  = big.NewInt(0)
)

type Head struct {
	safe      atomic.Pointer[big.Int]
	finalized atomic.Pointer[big.Int]
	latest    atomic.Pointer[big.Int]
	pending   atomic.Pointer[big.Int]
}

func (h *Head) Load(ctx context.Context, client *ethclient.Client) error {
	safe, err := client.HeaderByNumber(ctx, SafeBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get safe block: %w", err)
	}

	finalized, err := client.HeaderByNumber(ctx, FinalizedBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get finalized block: %w", err)
	}

	latest, err := client.HeaderByNumber(ctx, LatestBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get latest block: %w", err)
	}

	pending, err := client.HeaderByNumber(ctx, PendingBlockNumber)
	if err != nil {
		return fmt.Errorf("couldn't get pending block: %w", err)
	}

	h.safe.Store(safe.Number)
	h.finalized.Store(finalized.Number)
	h.latest.Store(latest.Number)
	h.pending.Store(pending.Number)

	return nil
}

func (h *Head) Safe() *big.Int {
	return h.safe.Load()
}

func (h *Head) Finalized() *big.Int {
	return h.finalized.Load()
}

func (h *Head) Latest() *big.Int {
	return h.latest.Load()
}

func (h *Head) Pending() *big.Int {
	return h.pending.Load()
}

type App struct {
	Config     *Config
	Head       *Head
	DataClient *db.Conn
	NodeClient *ethclient.Client
}

func CreateApp(ctx context.Context) (*App, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse config: %w", err)
	}

	db_conn, err := db.NewDB(config.DataDir, uint64(config.RamLimit))
	if err != nil {
		return nil, fmt.Errorf("Couldn't load db: %w", err)
	}

	eth, err := ethclient.DialContext(ctx, config.NodeAddr)
	if err != nil {
		return nil, fmt.Errorf("Couln't connect to node: %w", err)
	}

	app := &App{
		Config:     &config,
		Head:       &Head{},
		DataClient: db_conn,
		NodeClient: eth,
	}

	if err := app.Head.Load(ctx, app.NodeClient); err != nil {
		return nil, err
	}

	return app, nil
}

func (app *App) Close() {
	app.DataClient.Close()
	app.NodeClient.Close()
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
	// Prepare env
	flag.Parse()

	app, err := CreateApp(context.Background())
	if err != nil {
		panic(err.Error())
	}
	defer app.Close()

	initLogger(app.Config.IsDev())

	// Background crawler
	go background(context.Background(), app)

	// Metrics server
	metrics := echo.New()
	metrics.HidePort = true
	metrics.HideBanner = true

	metrics.GET("/metrics", echoprometheus.NewHandler())

	go func() {
		if err := metrics.Start(fmt.Sprintf(":%d", app.Config.MetricsPort)); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				log.Info().Msg("metrics server / shutdowned")
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
		return c.String(http.StatusOK, app.DataClient.Status())
	})
	e.POST("/rpc", func(c echo.Context) error {
		return handleHttp(c, app)
	})

	go func() {
		if err := e.Start(fmt.Sprintf(":%d", app.Config.BindPort)); err != nil {
			if err == http.ErrServerClosed {
				log.Info().Msg("rpc server / shutdowned")
			} else {
				log.Error().Err(err).Msg("rpc server / failed")
			}
		}
	}()

	// shutdown with timeout
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := e.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("couldn't shutdown rpc server")
	}

	if err := metrics.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("couldn't shutdown metrics server")
	}

	log.Info().Msg("server stopped")
}

type Filter struct {
	FromBlock *string `json:"fromBlock"`
	ToBlock   *string `json:"toBlock"`

	Address interface{}   `json:"address"`
	Topics  []interface{} `json:"topics"`
}

func parseBlockNumber(str *string, head *Head) (uint64, error) {
	if str == nil {
		return head.Latest().Uint64(), nil
	}

	switch *str {
	case "earliest":
		return 0, nil
	case "", "latest":
		return head.Latest().Uint64(), nil
	case "safe":
		return head.Safe().Uint64(), nil
	case "finalized":
		return head.Finalized().Uint64(), nil
	case "pending":
		return head.Pending().Uint64(), nil
	}

	// parse hex string
	value, ok := new(big.Int).SetString(*str, 0)
	if !ok {
		return 0, fmt.Errorf("failed to parse the value")
	}

	return value.Uint64(), nil
}

func (raw *Filter) ToQuery(head *Head) (*db.Query, error) {
	q, err := db.Query{}, (error)(nil)

	// Block numbers
	q.FromBlock, err = parseBlockNumber(raw.FromBlock, head)
	if err != nil {
		return nil, fmt.Errorf("invalid record 'fromBlock'")
	}

	q.ToBlock, err = parseBlockNumber(raw.ToBlock, head)
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

	query, err := filters[0].ToQuery(app.Head)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't convert request to internal filter")
		return c.JSON(http.StatusInternalServerError, CreateResponseError(err.Error()))
	}

	result, err := app.DataClient.Query(query)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't query in db")
		return c.JSON(http.StatusInternalServerError, CreateResponseError("internal server error"))
	}

	return c.JSON(http.StatusOK, Response{Result: &result})
}
