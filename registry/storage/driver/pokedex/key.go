package pokedex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/franela/goreq"
	"io"
	"io/ioutil"
	"net/url"
	"time"
)

type PokedexKey struct {
	Id             int64       `json:"id"`
	Name           string      `json:"name"`
	Created        PokedexTime `json:"created"`
	Modified       PokedexTime `json:"modified"`
	FileserverId   string      `json:"fileserver_id"`
	FileserverHost string      `json:"host"`
	FileserverPort int         `json:"port"`
	ContentLength  *int64      `json:"content_length"`
	ContentType    *string     `json:"content_type"`
	Checksum       *string     `json:"checksum"`

	deleted bool
	client  *PokedexClient
}

func PokedexKeyFromJson(data []byte, client *PokedexClient) (*PokedexKey, error) {
	if string(data) == "null" {
		return nil, nil
	}
	var k PokedexKey
	if err := json.Unmarshal(data, &k); err != nil {
		return nil, err
	}
	k.Initialize(client)
	return &k, nil
}

func (k *PokedexKey) Initialize(client *PokedexClient) {
	k.client = client
	k.deleted = false
}

func (k *PokedexKey) GetContentsAsStream(offset int64) (io.ReadCloser, error) {
	if err := k.checkExists(); err != nil {
		return nil, err
	}

	req := goreq.Request{
		Method:          "GET",
		Uri:             k.DataUrl(),
		Timeout:         500 * time.Second,
		MaxRedirects:    10,
		RedirectHeaders: true,
	}

	req.AddHeader("Range", fmt.Sprintf("bytes=%d-", offset))

	resp, err := req.Do()
	if err != nil {
		return nil, err
	}

	// special case for Range not satisfiable
	if resp.StatusCode == 416 {
		defer resp.Body.Close()
		var junk []byte
		return ioutil.NopCloser(bytes.NewReader(junk)), nil
	}

	// Note: expect a 206 Partial Content response here
	if respErr := CheckHTTPResponse(resp, 206); respErr != nil {
		defer resp.Body.Close()
		return nil, respErr
	}
	return resp.Body, nil
}

func (k *PokedexKey) GetContentsAsBytes() ([]byte, error) {
	stream, err := k.GetContentsAsStream(0)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	return ioutil.ReadAll(stream)
}

func (k *PokedexKey) GetContentsAsString() (string, error) {
	body, err := k.GetContentsAsBytes()
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (k *PokedexKey) SetContentsFromStream(offset int64, reader io.Reader, contentType string) (int64, error) {
	if err := k.checkExists(); err != nil {
		return 0, err
	}

	req := goreq.Request{
		Method:          "POST",
		Uri:             k.DataUrl(),
		Timeout:         30 * time.Minute,
		ContentType:     contentType,
		Body:            reader,
		MaxRedirects:    10,
		RedirectHeaders: true,
	}

	if offset != 0 {
		// if we have a nonzero offset, tell Pokedex to overwrite any existing
		// data starting at the provided byte offset.
		req.AddHeader("X-Overwrite-Offset", fmt.Sprintf("%d", offset))
	}

	resp, err := req.Do()

	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return 0, respErr
	}

	// update the key with returned metadata
	var metaKey PokedexKey
	if err = resp.Body.FromJsonTo(&metaKey); err != nil {
		return 0, err
	}

	k.ContentLength = metaKey.ContentLength
	k.ContentType = metaKey.ContentType
	k.Checksum = metaKey.Checksum
	k.Modified = metaKey.Modified

	return *metaKey.ContentLength - offset, nil
}

func (k *PokedexKey) SetContentsFromBytes(newContents []byte, contentType string) error {
	reader := bytes.NewReader(newContents)
	_, err := k.SetContentsFromStream(0, reader, contentType)
	return err
}

func (k *PokedexKey) SetContentsFromString(newContents string, contentType string) error {
	return k.SetContentsFromBytes([]byte(newContents), contentType)
}

func (k *PokedexKey) Move(newName string) error {
	params := url.Values{}
	params.Set("key_name", k.Name)
	params.Set("new_key_name", newName)

	resp, err := k.req("PUT", k.KeyUrl(), params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return respErr
	}
	k.Name = newName
	return nil
}

func (k *PokedexKey) Delete() error {
	resp, err := k.req("DELETE", k.KeyUrl(), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if respErr := CheckHTTPResponse(resp, 200); respErr != nil {
		return respErr
	}
	k.deleted = true
	return nil
}

func (k *PokedexKey) KeyUrl() string {
	return k.client.MakeUrl(fmt.Sprintf("keys/%s", k.Name))
}

func (k *PokedexKey) DataUrl() string {
	return fmt.Sprintf("http://%s:%d/files/%d", k.FileserverHost, k.FileserverPort, k.Id)
}

// Private methods
func (c *PokedexKey) req(method string, url string, params interface{}) (*goreq.Response, error) {
	return goreq.Request{
		Method:          method,
		Uri:             url,
		Timeout:         500 * time.Second,
		QueryString:     params,
		MaxRedirects:    10,
		RedirectHeaders: true,
	}.Do()
}

func (k *PokedexKey) checkExists() error {
	if k.deleted {
		return KeyDoesNotExist{Name: k.Name}
	}
	maybeKey, err := k.client.GetKey(k.Name)
	if err != nil {
		return err
	}
	if maybeKey == nil {
		return KeyDoesNotExist{Name: k.Name}
	} else {
		return nil
	}
}
