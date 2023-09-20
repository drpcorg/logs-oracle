package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"drpc-logs-oracle/db"
)

type App struct {
	Config     *Config
	DataClient *db.Conn
	NodeClient *ethclient.Client
}

func (app *App) Init(ctx context.Context) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("Couldn't parse config: %v", err)
	}

	db_conn, err := db.NewDB(config.DataDir, uint64(config.RamLimit))
	if err != nil {
		return fmt.Errorf("Couldn't load db: %v", err)
	}

	eth, err := ethclient.DialContext(ctx, config.NodeAddr)
	if err != nil {
		return fmt.Errorf("Couln't connect to node: %v", err)
	}

	app.Config = &config
	app.DataClient = db_conn
	app.NodeClient = eth

	return nil
}

func (app *App) Close() {
	app.DataClient.Close()
	app.NodeClient.Close()
}

func InitLogger(app *App) {
	var logger zerolog.Logger

	if app.Config.Env == "development" {
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
	flag.Parse()

	app := &App{}
	if err := app.Init(context.Background()); err != nil {
		panic(err.Error())
	}
	defer App.Close()

	InitLogger(app)

	go background(context.Background(), app)

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Decompress())
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
	e.GET("/status", func(c echo.Context) error {
		return c.String(http.StatusOK, app.DataClient.Status())
	})
	e.POST("/rpc", func(c echo.Context) error {
		return handleHttp(c, app)
	})

	if err := e.Start(fmt.Sprintf(":%d", app.Config.BindPort)); err != nil {
		if err == http.ErrServerClosed {
			log.Info().Msg("rpc server / shutdowned")
		} else {
			log.Error().Err(err).Msg("rpc server / failed")
		}
	}
}

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

	// TODO: don't fetch the node every request
	latestBlock, err := app.NodeClient.BlockNumber(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't get latest block number")
		return c.JSON(http.StatusInternalServerError, CreateResponseError("internal server error"))
	}

	query, err := filters[0].ToQuery(latestBlock)
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
