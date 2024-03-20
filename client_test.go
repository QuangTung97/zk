package zk

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient_Validate(t *testing.T) {
	t.Run("empty servers", func(t *testing.T) {
		client, err := NewClient(nil, 0)
		assert.Equal(t, errors.New("zk: server list must not be empty"), err)
		assert.Nil(t, client)
	})

	t.Run("timeout too small", func(t *testing.T) {
		client, err := NewClient([]string{"localhost"}, 0)
		assert.Equal(t, errors.New("zk: session timeout must not be too small"), err)
		assert.Nil(t, client)
	})
}
