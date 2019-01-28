// +build integration

package riemanngo

import (
	"fmt"
	"testing"
	"time"
)

func TestTCPClient(t *testing.T) {
	c, err := NewTCPClient(
		"127.0.0.1:5555",
		5*time.Second,
		5*time.Second,
	)

	if err != nil {
		t.Errorf(
			"Connection error : %s", err,
		)
	}

	count := int(1e5)

	start := time.Now()

	for i := 0; i < count; i++ {
		i64 := int64(i)

		c.Send(
			&Event{
				Service: fmt.Sprintf(
					"TestTCPClient-%d", i,
				),

				MetricInt64: &i64,
				Tags:        []string{"a", "b"},
				TTL:         30 * time.Second,
				State:       "ok",
			},
		)
	}

	err = c.Close()
	if err != nil {
		t.Errorf("Got %s on close", err)
	}

	t.Logf(
		"Pushed %d events in %s\n",
		count, time.Now().Sub(start),
	)
}
