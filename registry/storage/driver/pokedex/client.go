package pokedex

import (
	"errors"
	"fmt"
	"github.com/franela/goreq"
	"io/ioutil"
	"net/url"
	"time"
)

type PokedexClient struct {
	Host string
	Port int
}

func (c *PokedexClient) CreateKey(name string) (*PokedexKey, error) {
	return c.RequestKey("POST", name)
}

func (c *PokedexClient) GetKey(name string) (*PokedexKey, error) {
	return c.RequestKey("GET", name)
}

func (c *PokedexClient) RequestKey(method string, name string) (*PokedexKey, error) {
	resp, err := c.req(method, fmt.Sprintf("keys/%s", name), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return nil, respErr
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return PokedexKeyFromJson(data, c)
}

func (c *PokedexClient) List(prefix string) ([]*PokedexKey, error) {
	params := url.Values{}
	params.Set("prefix", prefix)

	resp, err := c.req("GET", "keys", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return nil, respErr
	}

	var keys []*PokedexKey
	err = resp.Body.FromJsonTo(&keys)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		k.Initialize(c)
	}
	return keys, nil
}

func (c *PokedexClient) Delete(prefix string) (int, error) {
	if prefix == "" || prefix == "/" {
		return 0, errors.New("PokedexClient.Delete: prefix must not be the root")
	}

	params := url.Values{}
	params.Set("prefix", prefix)

	resp, err := c.req("DELETE", "keys", params)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return 0, respErr
	}

	type Result struct {
		NumDeleted int `json:"num_deleted"`
	}
	var result Result
	err = resp.Body.FromJsonTo(&result)
	if err != nil {
		return 0, err
	}
	return result.NumDeleted, nil
}

func (c *PokedexClient) req(method string, path string, params interface{}) (*goreq.Response, error) {
	fullPath := c.MakeUrl(path)
	return goreq.Request{
		Method:       method,
		Uri:          fullPath,
		Timeout:      500 * time.Second,
		QueryString:  params,
		MaxRedirects: 10,
	}.Do()
}

func (c *PokedexClient) MakeUrl(path string) string {
	return fmt.Sprintf("http://%s:%d/%s", c.Host, c.Port, path)
}
