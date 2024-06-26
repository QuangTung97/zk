package zk

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodePacket(t *testing.T) {
	t.Parallel()
	encodeDecodeTest(t, &requestHeader{-2, 5})
	encodeDecodeTest(t, &connectResponse{1, 2, 3, nil})
	encodeDecodeTest(t, &connectResponse{1, 2, 3, []byte{4, 5, 6}})
	encodeDecodeTest(t, &getAclResponse{[]ACL{{12, "s", "anyone"}}, Stat{}})
	encodeDecodeTest(t, &getChildrenResponse{[]string{"foo", "bar"}})
	encodeDecodeTest(t, &pathWatchRequest{"path", true})
	encodeDecodeTest(t, &pathWatchRequest{"path", false})
	encodeDecodeTest(t, &CheckVersionRequest{"/", -1})
	encodeDecodeTest(t, &reconfigRequest{nil, nil, nil, -1})
	encodeDecodeTest(t, &multiRequest{
		Ops: []multiRequestOp{
			{
				Header: multiHeader{Type: opCheck, Done: false, Err: -1},
				Op:     &CheckVersionRequest{Path: "/", Version: -1},
			},
		},
	})
}

func TestRequestStructForOp(t *testing.T) {
	for op, name := range opNames {
		if op != opNotify && op != opWatcherEvent {
			if s := requestStructForOp(op); s == nil {
				t.Errorf("No struct for op %s", name)
			}
		}
	}
}

func encodeDecodeTest(t *testing.T, r any) {
	buf := make([]byte, 1024)
	n, err := encodePacket(buf, r)
	if err != nil {
		t.Errorf("encodePacket returned non-nil error %+v\n", err)
		return
	}
	t.Logf("%+v %x", r, buf[:n])
	r2 := reflect.New(reflect.ValueOf(r).Elem().Type()).Interface()
	n2, err := decodePacket(buf[:n], r2)
	if err != nil {
		t.Errorf("decodePacket returned non-nil error %+v\n", err)
		return
	}
	if n != n2 {
		t.Errorf("sizes don't match: %d != %d", n, n2)
		return
	}
	if !reflect.DeepEqual(r, r2) {
		t.Errorf("results don't match: %+v != %+v", r, r2)
		return
	}
}

func TestEncodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := encodePacket([]byte{}, &requestHeader{1, 2})
	if err != ErrShortBuffer {
		t.Errorf("encodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func TestDecodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := decodePacket([]byte{}, &responseHeader{})
	if err != ErrShortBuffer {
		t.Errorf("decodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func BenchmarkEncode(b *testing.B) {
	buf := make([]byte, 4096)
	st := &connectRequest{Passwd: []byte("1234567890")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := encodePacket(buf, st); err != nil {
			b.Fatal(err)
		}
	}
}

func testEncodeDecode[O any](
	t *testing.T, obj O, cmpData []byte,
) {
	var codec codecBuffer
	var buf bytes.Buffer

	n, err := encodeObject[O](&obj, &codec, &buf)
	assert.Equal(t, nil, err)

	assert.Equal(t, len(cmpData), n)
	assert.Equal(t, cmpData, buf.Bytes())

	// Decode
	var newObj O
	err = decodeObject[O](&newObj, &codec, &buf)
	assert.Equal(t, nil, err)
	assert.Equal(t, obj, newObj)
}

func TestEncode(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		req := connectRequest{
			ProtocolVersion: protocolVersion,
			LastZxidSeen:    123,
			TimeOut:         2000,
			SessionID:       41,
			Passwd:          []byte("pass01"),
		}
		cmpData := []byte{
			0, 0, 0, 0x22,
			0, 0, 0, 0, // protocol
			0, 0, 0, 0, 0, 0, 0, 123, // last zxid seen
			0, 0, 2000 / 256, 2000 % 256, // timeout
			0, 0, 0, 0, 0, 0, 0, 41, // session id
			0, 0, 0, 6, // password len
			'p', 'a', 's', 's', '0', '1',
		}
		testEncodeDecode[connectRequest](t, req, cmpData)
	})

	t.Run("ping request", func(t *testing.T) {
		data := []byte{
			0, 0, 0, 0,
		}
		testEncodeDecode[pingRequest](t, pingRequest{}, data)
	})

	t.Run("ping response", func(t *testing.T) {
		data := []byte{
			0, 0, 0, 0,
		}
		testEncodeDecode[pingResponse](t, pingResponse{}, data)
	})

	t.Run("connect response", func(t *testing.T) {
		req := connectResponse{
			ProtocolVersion: protocolVersion,
			TimeOut:         2000,
			SessionID:       41,
			Passwd:          []byte("pass01"),
		}
		cmpData := []byte{
			0, 0, 0, 26,
			0, 0, 0, 0, // protocol
			0, 0, 2000 / 256, 2000 % 256, // timeout
			0, 0, 0, 0, 0, 0, 0, 41, // session id
			0, 0, 0, 6, // password len
			'p', 'a', 's', 's', '0', '1',
		}
		testEncodeDecode[connectResponse](t, req, cmpData)
	})
}
