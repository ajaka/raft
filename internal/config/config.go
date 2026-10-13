package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Seed struct {
	Id   string
	Addr string
	Port string
}

func FetchSeeds() []Seed {
	v := viper.New()

	v.SetConfigName("cluster_config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")

	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Failed to read service config file: error -> %v", err)
		panic(err)
	}

	var seeds []Seed
	if err := v.Unmarshal(&seeds); err != nil {
		fmt.Printf("Failed to unmarshal service config: error -> %v ", err)
		panic(err)
	}
	return seeds
}
