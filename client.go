package zk

import (
	"errors"
	"math/rand"
	"time"
)

// Client ...
type Client struct {
	randSrc rand.Source
}

// Option ...
type Option func(c *Client)

// NewClient ...
func NewClient(servers []string, sessionTimeout time.Duration, options ...Option) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("zk: server list must not be empty")
	}
	if sessionTimeout < 1*time.Second {
		return nil, errors.New("zk: session timeout must not be too small")
	}
	return &Client{}, nil
}
