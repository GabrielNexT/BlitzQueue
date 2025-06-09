package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	Datastore map[string]DatastoreConnection `mapstructure:"datastore"`
}

var config Config
var cached = false

func ReadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	err = viper.Unmarshal(&config)

	if err != nil {
		panic(fmt.Errorf("fatal error unmarshalling config: %w", err))
	}

	cached = true
}

func GetConfig() *Config {
	if !cached {
		ReadConfig()
	}
	return &config
}
