package pokedex

import (
	"os"
	"strconv"
	"testing"

	. "gopkg.in/check.v1"
)

func GetHostPortFromEnv() (string, int) {
	pokedexHost := os.Getenv("POKEDEX_HOST")
	pokedexPort := os.Getenv("POKEDEX_PORT")

	if pokedexHost == "" {
		panic("Must set POKEDEX_HOST")
	}

	port, err := strconv.Atoi(pokedexPort)
	if err != nil {
		panic("Must set POKEDEX_PORT")
	}

	return pokedexHost, port
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }
