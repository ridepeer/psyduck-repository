package pokedex

import (
	"math/rand"
	"os"
	"strconv"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
)

var skipPokedex func() string
var pokedexDriverConstructor func(rootDir string) (*PokedexStorageDriver, error)

func init() {
	pokedexHost := os.Getenv("POKEDEX_HOST")
	pokedexPort := os.Getenv("POKEDEX_PORT")

	// use this to generate a random dir name for the tests
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	root := "driver-" + strconv.Itoa(r1.Intn(100000))

	pokedexDriverConstructor = func(rootDir string) (*PokedexStorageDriver, error) {
		params := make(map[string]interface{})
		params["host"] = pokedexHost
		params["port"] = pokedexPort
		params["rootDir"] = rootDir

		return FromParameters(params)
	}

	// Skip Pokedex storage driver tests if environment variable parameters are
	// not provided
	skipPokedex = func() string {
		if pokedexHost == "" || pokedexPort == "" {
			return "Must set POKEDEX_HOST and POKEDEX_PORT to run PokedexDriver tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return pokedexDriverConstructor(root)
	}, skipPokedex)
}
