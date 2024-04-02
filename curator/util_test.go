package curator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParallelRunner(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		var steps []string
		r := NewParallelRunner(
			New(func(sess *Session) {
				steps = append(steps, "init01")
				sess.Run(func(client Client) {
					sess.AddRetry(func(sess *Session) {
						steps = append(steps, "retry01")
					})
				})
			}),
			New(func(sess *Session) {
				steps = append(steps, "init02")
			}),
		)

		assert.Equal(t, 0, len(steps))

		r.Begin(nil)
		assert.Equal(t, []string{
			"init01",
			"init02",
		}, steps)

		r.Retry()
		assert.Equal(t, []string{
			"init01",
			"init02",
			"retry01",
		}, steps)

		r.End()
		assert.Equal(t, []string{
			"init01",
			"init02",
			"retry01",
		}, steps)

		// Session Begin Again
		r.Begin(nil)
		assert.Equal(t, []string{
			"init01",
			"init02",
			"retry01",
			"init01",
			"init02",
		}, steps)
	})

}
