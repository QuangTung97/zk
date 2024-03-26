package zk

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestACLConsts(t *testing.T) {
	t.Run("world", func(t *testing.T) {
		assert.Equal(t, []ACL{
			{
				Perms:  31,
				Scheme: "world",
				ID:     "anyone",
			},
		}, WorldACL(PermAll))

		assert.Equal(t, []ACL{
			{
				Perms:  31,
				Scheme: "world",
				ID:     "anyone",
			},
		}, WorldACL(PermRead|PermWrite|PermCreate|PermDelete|PermAdmin))
	})

	t.Run("auth", func(t *testing.T) {
		assert.Equal(t, []ACL{
			{
				Perms:  1,
				Scheme: "auth",
				ID:     "",
			},
		}, AuthACL(PermRead))
	})

	t.Run("digest", func(t *testing.T) {
		assert.Equal(t, []ACL{
			{
				Perms:  31,
				Scheme: "digest",
				ID:     "user01:xOvH6JR5ZbRiGoGVEnAfhLL1vZA=",
			},
		}, DigestACL(PermAll, "user01", "password01"))
	})
}

func TestFormatServers(t *testing.T) {
	t.Run("without port", func(t *testing.T) {
		assert.Equal(t, []string{
			"localhost:2181",
		}, FormatServers([]string{"localhost"}))
	})

	t.Run("with port", func(t *testing.T) {
		assert.Equal(t, []string{
			"localhost:1221",
		}, FormatServers([]string{"localhost:1221"}))
	})

	t.Run("multi", func(t *testing.T) {
		assert.Equal(t, []string{
			"localhost:1234",
			"localhost:2181",
			"123.0.8.1:2345",
			"123.1.9.1:2181",
		}, FormatServers([]string{
			"localhost:1234",
			"localhost",
			"123.0.8.1:2345",
			"123.1.9.1",
		}))
	})
}

func TestStringShuffle(t *testing.T) {
	r := rand.New(rand.NewSource(3210))
	arr := []string{"a", "b", "c", "d"}
	stringShuffleRand(arr, r)
	assert.Equal(t, []string{"b", "d", "c", "a"}, arr)
}

func TestValidatePath(t *testing.T) {
	tt := []struct {
		path  string
		seq   bool
		valid bool
	}{
		{"/this is / a valid/path", false, true},
		{"/", false, true},
		{"", false, false},
		{"not/valid", false, false},
		{"/ends/with/slash/", false, false},
		{"/sequential/", true, true},
		{"/test\u0000", false, false},
		{"/double//slash", false, false},
		{"/single/./period", false, false},
		{"/double/../period", false, false},
		{"/double/..ok/period", false, true},
		{"/double/alsook../period", false, true},
		{"/double/period/at/end/..", false, false},
		{"/name/with.period", false, true},
		{"/test\u0001", false, false},
		{"/test\u001f", false, false},
		{"/test\u0020", false, true}, // first allowable
		{"/test\u007e", false, true}, // last valid ascii
		{"/test\u007f", false, false},
		{"/test\u009f", false, false},
		{"/test\uf8ff", false, false},
		{"/test\uffef", false, true},
		{"/test\ufff0", false, false},
	}

	for _, tc := range tt {
		err := validatePath(tc.path, tc.seq)
		if (err != nil) == tc.valid {
			t.Errorf("failed to validate path %q", tc.path)
		}
	}
}

func TestValidatePath2(t *testing.T) {
	assert.Equal(t, ErrInvalidPath, validatePath("home", false))
	assert.Equal(t, nil, validatePath("/", false))
	assert.Equal(t, ErrInvalidPath, validatePath("", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/config/", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/\x00", false))
	assert.Equal(t, ErrInvalidPath, validatePath("//hello", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/\x01", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/hello/./config", false))
	assert.Equal(t, nil, validatePath("/hello/.config", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/hello/config/.", false))
	assert.Equal(t, ErrInvalidPath, validatePath("/hello/../config", false))
	assert.Equal(t, nil, validatePath("/hello/..config", false))
	assert.Equal(t, nil, validatePath("/data/", true))
}
