package main

import (
	"fmt"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Env         string `default:"production"`
	BindPort    int    `default:"8000"`
	MetricsPort int    `default:"8001"`
	AccessLog   bool   `default:"false"`

	DataDir  string   `required:"true"`
	RamLimit Datasize `default:"0"`

	NodeRPC  string `required:"true"` // ethereum json rpc endpoint url
	NodeWS   string `required:"true"` // ethereum json rpc endpoint url
}

func LoadConfig() (Config, error) {
	var config Config
	err := envconfig.Process("oracle", &config)
	return config, err
}

func (c *Config) IsDev() bool {
	return c.Env == "development"
}

type Datasize uint64

func (d *Datasize) Decode(raw string) error {
	var val uint64

	i := 0
	for ; i < len(raw) && '0' <= raw[i] && raw[i] <= '9'; i++ {
		val = val*10 + uint64(raw[i]-'0')
	}
	if i == 0 {
		return fmt.Errorf("parse error")
	}

	unit := strings.ToLower(strings.TrimSpace(raw[i:]))
	switch unit {
	case "", "b", "byte":
		// do nothing - already in bytes

	case "k", "kb", "kilo", "kilobyte", "kilobytes":
		val *= 1 << 10
	case "m", "mb", "mega", "megabyte", "megabytes":
		val *= 1 << 20
	case "g", "gb", "giga", "gigabyte", "gigabytes":
		val *= 1 << 30
	case "t", "tb", "tera", "terabyte", "terabytes":
		val *= 1 << 40
	case "p", "pb", "peta", "petabyte", "petabytes":
		val *= 1 << 50
	case "E", "EB", "e", "eb", "eB":
		val *= 1 << 60

	default:
		return fmt.Errorf("parse error")
	}

	*d = Datasize(val)
	return nil
}
