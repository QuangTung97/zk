package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCurator(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		steps := make([]string, 0)
		c := New(func(sess *Session) {
			steps = append(steps, "start")
		})

		assert.Equal(t, []string{}, steps)

		c.Begin(nil)

		assert.Equal(t, []string{"start"}, steps)
	})

	t.Run("retry", func(t *testing.T) {
		steps := make([]string, 0)
		c := New(func(sess *Session) {
			steps = append(steps, "start")
			sess.AddRetry(func(sess *Session) {
				steps = append(steps, "retry")
			})
		})

		c.Begin(nil)
		c.Retry()

		assert.Equal(t, []string{"start", "retry"}, steps)
	})

	t.Run("end and then begin", func(t *testing.T) {
		steps := make([]string, 0)
		c := New(func(sess *Session) {
			steps = append(steps, "start")
			sess.AddRetry(func(sess *Session) {
				steps = append(steps, "retry")
			})
		})

		c.Begin(nil)
		c.End()
		c.Retry()
		c.Begin(nil)
		c.Retry()

		assert.Equal(t, []string{"start", "start", "retry"}, steps)
	})
}

func TestCurator_Chain(t *testing.T) {
	t.Run("without call next", func(t *testing.T) {
		steps := make([]string, 0)
		c := NewChain(
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func01")
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func02")
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func03")
			},
		)

		c.Begin(nil)

		assert.Equal(t, []string{"func01"}, steps)
	})

	t.Run("with call next", func(t *testing.T) {
		steps := make([]string, 0)
		c := NewChain(
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func01")
				next(sess)
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func02")
				next(sess)
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func03")
				next(sess)
			},
		)

		c.Begin(nil)

		assert.Equal(t, []string{"func01", "func02", "func03"}, steps)
	})

	t.Run("end and then begin again", func(t *testing.T) {
		steps := make([]string, 0)

		var sessList []*Session

		c := NewChain(
			func(sess *Session, next func(sess *Session)) {
				sessList = append(sessList, sess)
				steps = append(steps, "func01")
				next(sess)
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func02")
				next(sess)
			},
			func(sess *Session, next func(sess *Session)) {
				steps = append(steps, "func03")
				next(sess)
			},
		)

		c.Begin(nil)
		c.End()
		c.Begin(nil)

		assert.Equal(t, []string{
			"func01", "func02", "func03",
			"func01", "func02", "func03",
		}, steps)

		assert.Equal(t, 2, len(sessList))
		assert.NotSame(t, sessList[0], sessList[1])
	})
}
