package models

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	ThreadsCount int            `json:"threads_count"`
	Database     ConfigDatabase `json:"database"`
}

type ConfigDatabase struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	Host            string `json:"host"`
	Port            string `json:"port"`
	DbName          string `json:"db_name"`
	InsertBatchSize int    `json:"insert_batch_size"`
}

func ParseConfigFile(filename string) (config *Config, err error) {
	file, err := os.Open(filename)
	if err != nil {
		err = fmt.Errorf("os.Open: %w", err)
		return
	}
	if err = json.NewDecoder(file).Decode(&config); err != nil {
		err = fmt.Errorf("json.NewDecoder: %w", err)
		return
	}
	if err = file.Close(); err != nil {
		err = fmt.Errorf("file.Close: %w", err)
	}
	return
}
