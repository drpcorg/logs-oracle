package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/kelseyhightower/envconfig"

	liboracle "github.com/p2p-org/drpc-logs-oracle"
)

type Config struct {
	Env         string `default:"production"`
	BindPort    int    `default:"8000"`
	MetricsPort int    `default:"8001"`

	DataDir  string `required:"true"`
	RamLimit uint64  `default:"0"` // bytes

	NodeRPC  string `required:"true"`
	NodeWS   string `required:"true"`
}

func NewConfig() (*Config, error) {
	var config Config
	err := envconfig.Process("oracle", &config)
	return &config, err
}

func (c *Config) IsDev() bool {
	return c.Env == "development"
}

func runBackground(ctx context.Context, wg sync.WaitGroup) {
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Prepare env
	flag.Parse()

	config, err := NewConfig()
	if err != nil {
		panic(err)
	}

	var logger zerolog.Logger
	if config.IsDev() {
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

	db, err := liboracle.NewDB(config.DataDir, config.RamLimit)
	if err != nil {
		log.Panic().Err(err).Msg("couldn't load db")
	}
	defer db.Close()

	node, err := NewNode(ctx, config.NodeWS)
	if err != nil {
		log.Panic().Err(err).Msg("couldn't connect to node")
	}
	defer node.Close()

	// run
	wg := sync.WaitGroup{}

	go func() {
		wg.Add(1)
		defer wg.Done()

		db.SetUpstream(config.NodeRPC)

		headch := make(chan *big.Int)
		go node.SubscribeNewHead(ctx, &wg, headch)

		for {
			select {
			case data := <-headch:
				if err := db.UpdateHeight(data.Uint64()); err != nil {
					log.Error().Err(err).Msg("couldn't update height in db")
					return
				} else {
					log.Debug().Uint64("head", data.Uint64()).Msg("updated head in db")
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	app := echo.New()
	go func() {
		wg.Add(1)
		defer wg.Done()

		app.Debug = config.IsDev()
		app.HidePort = true
		app.HideBanner = true

		app.Use(middleware.Recover())
		app.Use(middleware.Decompress())
		app.Use(echoprometheus.NewMiddleware("oracle"))

		app.POST("/rpc", func(c echo.Context) error {
			return handleHttp(c, node, db)
		})

		if err := app.Start(fmt.Sprintf(":%d", config.BindPort)); err != nil {
			if err != http.ErrServerClosed {
				log.Error().Err(err).Msg("rpc server failed")
			}
		}
	}()

	metrics := echo.New()
	go func() {
		wg.Add(1)
		defer wg.Done()

		metrics.Debug = config.IsDev()
		metrics.HidePort = true
		metrics.HideBanner = true

		metrics.GET("/metrics", echoprometheus.NewHandler())

		if err := metrics.Start(fmt.Sprintf(":%d", config.MetricsPort)); err != nil {
			if err != http.ErrServerClosed {
				log.Error().Err(err).Msg("metrics server failed")
			}
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Info().Msg("received a interrupt signal")

	if err := app.Shutdown(ctx); err != nil { // wait rpc server
		log.Error().Err(err).Msg("couldn't shutdown rpc server")
	} else {
		log.Info().Msg("rpc server stopped")
	}

	if err := metrics.Shutdown(ctx); err != nil { // wait metrics server
		log.Error().Err(err).Msg("couldn't shutdown metrics server")
	} else {
		log.Info().Msg("metrics server stopped")
	}

	cancel() // notifying everyone about the shutdown
	wg.Wait() // wait background workers
}

type Filter struct {
	Limit     *uint64  `json:"limit"`
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

	q.Limit = raw.Limit

	// Address
	if raw.Address != nil {
		switch r := raw.Address.(type) {
		case []interface{}:
			for i, addr := range r {
				if str, ok := addr.(string); ok {
					q.Addresses = append(q.Addresses, str)
				} else {
					return nil, fmt.Errorf("non-string address at index %d", i)
				}
			}

		case string:
			q.Addresses = append(q.Addresses, r)

		default:
			return nil, fmt.Errorf("invalid addresses in query")
		}
	}

	// Topics
	if raw.Topics != nil {
		if len(raw.Topics) > 4 {
			return nil, fmt.Errorf("allowed only 4 topic filters")
		}

		q.Topics = make([][]string, len(raw.Topics))

		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:

			case string:
				q.Topics[i] = append(q.Topics[i], topic)

			case []interface{}:
				// or case e.g. [null, "topic0", "topic1"]
				for _, rawTopic := range topic {
					if rawTopic == nil {
						q.Topics[i] = nil // null component, match all
						break
					}

					if topic, ok := rawTopic.(string); ok {
						q.Topics[i] = append(q.Topics[i], topic)
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

func handleHttp(c echo.Context, node *Node, db *liboracle.Conn) error {
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

	query, err := filters[0].ToQuery(node)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't convert request to internal filter")
		return c.JSON(http.StatusInternalServerError, CreateResponseError(err.Error()))
	}

	result, err := db.Query(query)
	if err != nil {
		log.Error().Err(err).Msg("Couldn't query in db")
		return c.JSON(http.StatusInternalServerError, CreateResponseError("internal server error"))
	}

	return c.JSON(http.StatusOK, Response{Result: &result})
}
