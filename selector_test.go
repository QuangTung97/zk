package zk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelector(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := NewServerListSelector(1235)
		s.Init([]string{"server01", "server02:1234", "server03"})

		assert.Equal(t, SelectNextOutput{
			Server: "server03:2181",
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server: "server02:1234",
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server:     "server03:2181",
			RetryStart: true,
		}, s.Next())
	})

	t.Run("with notify", func(t *testing.T) {
		s := NewServerListSelector(1235)
		s.Init([]string{"server01", "server02:1234", "server03"})

		assert.Equal(t, SelectNextOutput{
			Server: "server03:2181",
		}, s.Next())

		s.NotifyConnected()

		assert.Equal(t, SelectNextOutput{
			Server: "server03:2181",
		}, s.Next())
	})

	t.Run("with notify multi times", func(t *testing.T) {
		s := NewServerListSelector(1235)
		s.Init([]string{"server01", "server02:1234", "server03"})

		assert.Equal(t, SelectNextOutput{
			Server: "server03:2181",
		}, s.Next())

		s.NotifyConnected()
		s.NotifyConnected()
		s.NotifyConnected()

		assert.Equal(t, SelectNextOutput{
			Server: "server03:2181",
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server: "server02:1234",
		}, s.Next())

		s.NotifyConnected()

		assert.Equal(t, SelectNextOutput{
			Server: "server02:1234",
		}, s.Next())
		assert.Equal(t, SelectNextOutput{
			Server: "server01:2181",
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server:     "server03:2181",
			RetryStart: true,
		}, s.Next())
		assert.Equal(t, SelectNextOutput{
			Server:     "server02:1234",
			RetryStart: true,
		}, s.Next())
		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())

		s.NotifyConnected()

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: false,
		}, s.Next())
	})

	t.Run("single node", func(t *testing.T) {
		s := NewServerListSelector(1235)
		s.Init([]string{"server01"})

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())

		s.NotifyConnected()

		assert.Equal(t, SelectNextOutput{
			Server:     "server01:2181",
			RetryStart: true,
		}, s.Next())
	})
}
