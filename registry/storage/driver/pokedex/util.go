package pokedex

import (
	"fmt"
	"github.com/franela/goreq"
	"time"
)

var pokedexTimeLayout = "2006-01-02T15:04:05"

type PokedexTime struct {
	time.Time
}

func (ct *PokedexTime) UnmarshalJSON(b []byte) (err error) {
	if b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	ct.Time, err = time.Parse(pokedexTimeLayout, string(b))
	return err
}

func (ct *PokedexTime) MarshalJSON() ([]byte, error) {
	return []byte(ct.Time.Format(pokedexTimeLayout)), nil
}

type KeyDoesNotExist struct {
	Name string
}

func (err KeyDoesNotExist) Error() string {
	return fmt.Sprintf("Key not found: %s", err.Name)
}

type RequestError struct {
	StatusCode int
	Reason     string
}

func (err RequestError) Error() string {
	return fmt.Sprintf("Request Exception: %s", err.Reason)
}

func CheckHTTPResponse(resp *goreq.Response, expectedStatus int) error {
	if resp.StatusCode != expectedStatus {
		return RequestError{
			StatusCode: resp.StatusCode,
			Reason:     resp.Status,
		}
	} else {
		return nil
	}
}

func StringSliceContains(s []string, n string) bool {
	for _, a := range s {
		if a == n {
			return true
		}
	}
	return false
}
