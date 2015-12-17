package pokedex

import (
	. "gopkg.in/check.v1"
)

type PokedexClientSuite struct {
	client *PokedexClient
	root   string
}

var _ = Suite(&PokedexClientSuite{})

func (s *PokedexClientSuite) SetUpTest(c *C) {
	host, port := GetHostPortFromEnv()
	s.client = &PokedexClient{Host: host, Port: port}
	s.root = "pokedex_repo_test/"
	s.client.Delete(s.root)
}

func (s *PokedexClientSuite) TestCreateKey(c *C) {
	k, err := s.client.CreateKey(s.root + "hi")
	c.Assert(err, IsNil)
	c.Assert(k, NotNil)

	c.Check(k.Name, Equals, s.root+"hi")
	c.Check(k.ContentLength, IsNil)
	c.Check(k.ContentType, IsNil)
	c.Check(k.Checksum, IsNil)
}

func (s *PokedexClientSuite) TestGetKey(c *C) {
	// test not found
	k, err := s.client.GetKey(s.root + "hi")
	c.Assert(err, IsNil)
	c.Assert(k, IsNil)

	_, err = s.client.CreateKey(s.root + "hi")
	c.Assert(err, IsNil)

	k, err = s.client.GetKey(s.root + "hi")
	c.Assert(err, IsNil)
	c.Assert(k, NotNil)

	c.Check(k.Name, Equals, s.root+"hi")
	c.Check(k.ContentLength, IsNil)
	c.Check(k.ContentType, IsNil)
	c.Check(k.Checksum, IsNil)
}

func (s *PokedexClientSuite) TestListKeys(c *C) {
	_, err := s.client.CreateKey(s.root + "hi/1")
	c.Assert(err, IsNil)
	_, err = s.client.CreateKey(s.root + "hi/2")
	c.Assert(err, IsNil)

	keys, err := s.client.List(s.root)
	c.Assert(err, IsNil)
	c.Assert(keys, NotNil)

	c.Check(keys, HasLen, 2)
	c.Check(keys[0].Name, Equals, s.root+"hi/1")
	c.Check(keys[1].Name, Equals, s.root+"hi/2")
}

func (s *PokedexClientSuite) TestDelete(c *C) {
	_, err := s.client.CreateKey(s.root + "hi/1")
	c.Assert(err, IsNil)
	_, err = s.client.CreateKey(s.root + "hi/2")
	c.Assert(err, IsNil)

	num, err := s.client.Delete(s.root)
	c.Assert(err, IsNil)
	c.Assert(num, Equals, 2)
}
