package pokedex

import (
	"bytes"
	"fmt"
	. "gopkg.in/check.v1"
	"io"
	"io/ioutil"
	"math/rand"
	"strconv"
	"time"
)

type KeySuite struct {
	client     *PokedexClient
	root       string
	key        *PokedexKey
	randomData string
}

var _ = Suite(&KeySuite{})

func (s *KeySuite) SetUpTest(c *C) {
	host, port := GetHostPortFromEnv()
	s.client = &PokedexClient{Host: host, Port: port}
	s.root = "pokedex_repo_test/"
	s.client.Delete(s.root)
	k, err := s.client.CreateKey(s.root + "test")
	if err != nil {
		c.Fatal("Failed to create test key")
	}
	s.key = k

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	s.randomData = strconv.Itoa(r1.Intn(10000000))
}

func (s *KeySuite) TestUrls(c *C) {
	c.Check(s.key.KeyUrl(), Matches, fmt.Sprintf("http://.*/keys/%s", s.key.Name))
	c.Check(s.key.DataUrl(), Matches, fmt.Sprintf("http://.*/files/%d", s.key.Id))
}

func (s *KeySuite) TestStringContent(c *C) {
	var data string
	var err error

	// no contents
	data, err = s.key.GetContentsAsString()
	c.Assert(err, FitsTypeOf, RequestError{})
	c.Assert(err, ErrorMatches, ".*404 Not Found.*")

	// sleep a second to ensure that modified is changed
	time.Sleep(1 * time.Second)

	err = s.key.SetContentsFromString(s.randomData, "text/plain")
	c.Assert(err, IsNil)

	c.Check(*s.key.ContentType, Equals, "text/plain")
	c.Check(*s.key.ContentLength, Equals, int64(len(s.randomData)))
	c.Check(s.key.Checksum, NotNil)
	c.Check(s.key.Modified, Not(Equals), s.key.Created)

	data, err = s.key.GetContentsAsString()
	c.Assert(err, IsNil)
	c.Assert(data, Equals, s.randomData)
}

func (s *KeySuite) TestGetContentsAsStream(c *C) {
	var data []byte
	var stream io.ReadCloser
	var err error

	// no contents
	_, err = s.key.GetContentsAsStream(0)
	c.Assert(err, FitsTypeOf, RequestError{})
	c.Assert(err, ErrorMatches, ".*404 Not Found.*")

	// set some contents
	err = s.key.SetContentsFromString(s.randomData, "text/plain")
	c.Assert(err, IsNil)

	// read it back
	stream, err = s.key.GetContentsAsStream(0)
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(stream)
	c.Assert(err, IsNil)
	c.Assert(string(data), Equals, s.randomData)
	stream.Close()

	// read part of it starting at an offset
	partialData := s.randomData[len(s.randomData)/2:]
	stream, err = s.key.GetContentsAsStream(int64(len(s.randomData) - len(partialData)))
	c.Assert(err, IsNil)
	data, err = ioutil.ReadAll(stream)
	c.Assert(err, IsNil)
	c.Assert(string(data), Equals, partialData)
	stream.Close()
}

func (s *KeySuite) TestSetContentFromStreamWithOffset(c *C) {
	var data []byte
	var count int64
	var err error

	partOne := []byte(s.randomData[:len(s.randomData)/2])
	partTwo := []byte(s.randomData[len(s.randomData)/2:])

	// set first half of data
	count, err = s.key.SetContentsFromStream(0, bytes.NewReader(partOne), "text/plain")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(len(partOne)))

	// set second half of data
	count, err = s.key.SetContentsFromStream(int64(len(partOne)), bytes.NewReader(partTwo), "text/plain")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(len(partTwo)))

	// read it back
	data, err = s.key.GetContentsAsBytes()
	c.Assert(err, IsNil)
	c.Assert(string(data), Equals, s.randomData)

	// append data to itself
	count, err = s.key.SetContentsFromStream(int64(len(s.randomData)), bytes.NewReader([]byte(s.randomData)), "text/plain")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(len(s.randomData)))

	// read it back
	data, err = s.key.GetContentsAsBytes()
	c.Assert(err, IsNil)
	c.Assert(string(data[:len(s.randomData)]), Equals, s.randomData)
	c.Assert(string(data[len(s.randomData):]), Equals, s.randomData)
}

func (s *KeySuite) TestMove(c *C) {
	var err error
	var k *PokedexKey
	var data string

	keyName := s.key.Name
	newKeyName := keyName + "_new"

	// give the key some data
	err = s.key.SetContentsFromString(s.randomData, "text/plain")
	c.Assert(err, IsNil)

	// move to a new location
	err = s.key.Move(newKeyName)
	c.Assert(err, IsNil)

	c.Assert(s.key.Name, Equals, newKeyName)

	// assert the key is gone
	k, err = s.client.GetKey(keyName)
	c.Assert(err, IsNil)
	c.Assert(k, IsNil)

	// assert the new key can be looked up
	k, err = s.client.GetKey(newKeyName)
	c.Assert(err, IsNil)
	c.Assert(k, NotNil)
	c.Check(k.Name, Equals, newKeyName)

	// assert content is right
	data, err = k.GetContentsAsString()
	c.Assert(err, IsNil)
	c.Assert(data, Equals, s.randomData)
}

func (s *KeySuite) TestDelete(c *C) {
	var err error
	var k *PokedexKey

	// give the key some data
	err = s.key.SetContentsFromString(s.randomData, "text/plain")
	c.Assert(err, IsNil)

	err = s.key.Delete()
	c.Assert(err, IsNil)

	// assert the key is gone
	k, err = s.client.GetKey(s.key.Name)
	c.Assert(err, IsNil)
	c.Assert(k, IsNil)
}
