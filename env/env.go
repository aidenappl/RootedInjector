package env

import (
	"fmt"
	"os"
)

var (
	DBPassword = getEnvOrPanic("DB_PASSWORD")
	DBHost     = getEnv("DB_HOST", "localhost")
)

func getEnv(key string, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}

	return fallback
}

func getEnvOrPanic(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("❌ missing required environment variable: '%v'\n", key))
	}
	return value
}
