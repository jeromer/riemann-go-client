package riemanngo

import (
	"testing"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/riemann/riemann-go-client/proto"
)

func TestToProto(t *testing.T) {
	i64 := int64(123)
	f32 := float32(1.23)
	f64 := float64(4.56)

	testCases := []struct {
		desc     string
		event    *Event
		expected *proto.Event
	}{
		{
			desc: "simple event, metric int64",
			event: &Event{
				Description: "aaa",
				Host:        "baz",
				Service:     "foobar",
				MetricInt64: &i64,
				Tags:        []string{"hello"},
				Time:        time.Unix(100, 0),
			},
			expected: &proto.Event{
				Description:  pb.String("aaa"),
				Host:         pb.String("baz"),
				Time:         pb.Int64(100),
				TimeMicros:   pb.Int64(100000000),
				MetricSint64: pb.Int64(i64),
				Service:      pb.String("foobar"),
				Ttl:          pb.Float32(0),
				Tags:         []string{"hello"},
				State:        pb.String(""),
			},
		},
		{
			desc: "event with attributes, metric float32",
			event: &Event{
				Host:          "baz",
				Service:       "foobar",
				MetricFloat32: &f32,
				Tags:          []string{"hello"},
				Time:          time.Unix(100, 0),
				TTL:           10 * time.Second,
				Attributes: map[string]string{
					"foo": "bar",
					"bar": "baz",
				},
			},
			expected: &proto.Event{
				Description: pb.String(""),
				Host:        pb.String("baz"),
				Time:        pb.Int64(100),
				TimeMicros:  pb.Int64(100000000),
				MetricF:     pb.Float32(f32),
				Service:     pb.String("foobar"),
				Tags:        []string{"hello"},
				Ttl:         pb.Float32(10),
				State:       pb.String(""),
				Attributes: []*proto.Attribute{
					{
						Key:   pb.String("bar"),
						Value: pb.String("baz"),
					},
					{
						Key:   pb.String("foo"),
						Value: pb.String("bar"),
					},
				},
			},
		},
		{
			desc: "event with attributes, metric float64",
			event: &Event{
				Host:          "baz",
				Service:       "foobar",
				MetricFloat64: &f64,
				Tags:          []string{"hello"},
				Time:          time.Unix(100, 0),
				TTL:           10 * time.Second,
				Attributes: map[string]string{
					"foo": "bar",
					"bar": "baz",
				},
			},
			expected: &proto.Event{
				Description: pb.String(""),
				Host:        pb.String("baz"),
				Time:        pb.Int64(100),
				TimeMicros:  pb.Int64(100000000),
				MetricD:     pb.Float64(f64),
				Service:     pb.String("foobar"),
				Tags:        []string{"hello"},
				Ttl:         pb.Float32(10),
				State:       pb.String(""),
				Attributes: []*proto.Attribute{
					{
						Key:   pb.String("bar"),
						Value: pb.String("baz"),
					},
					{
						Key:   pb.String("foo"),
						Value: pb.String("bar"),
					},
				},
			},
		},
		{
			desc: "full event",
			event: &Event{
				Host:        "baz",
				Service:     "foobar",
				TTL:         20 * time.Millisecond,
				Description: "desc",
				State:       "critical",
				MetricInt64: &i64,
				Tags:        []string{"hello"},
				Time:        time.Unix(100, 0),
			},
			expected: &proto.Event{
				Host:         pb.String("baz"),
				Time:         pb.Int64(100),
				TimeMicros:   pb.Int64(100000000),
				Ttl:          pb.Float32(0.02),
				Description:  pb.String("desc"),
				State:        pb.String("critical"),
				MetricSint64: pb.Int64(i64),
				Service:      pb.String("foobar"),
				Tags:         []string{"hello"},
			},
		},
		{
			desc: "test int64",
			event: &Event{
				Host:          "baz",
				Service:       "foobar",
				TTL:           20 * time.Second,
				Description:   "desc",
				State:         "critical",
				MetricInt64:   &i64,
				MetricFloat32: &f32,
				MetricFloat64: &f64,
				Tags:          []string{"hello"},
				Time:          time.Unix(100, 0),
			},
			expected: &proto.Event{
				Host:         pb.String("baz"),
				Time:         pb.Int64(100),
				TimeMicros:   pb.Int64(100000000),
				Ttl:          pb.Float32(20),
				Description:  pb.String("desc"),
				State:        pb.String("critical"),
				MetricSint64: pb.Int64(i64),
				Service:      pb.String("foobar"),
				Tags:         []string{"hello"},
			},
		},
		{
			desc: "test float32",
			event: &Event{
				Host:          "baz",
				Service:       "foobar",
				TTL:           20 * time.Second,
				Description:   "desc",
				State:         "critical",
				MetricFloat32: &f32,
				MetricFloat64: &f64,
				Tags:          []string{"hello"},
				Time:          time.Unix(100, 0),
			},
			expected: &proto.Event{
				Host:        pb.String("baz"),
				Time:        pb.Int64(100),
				TimeMicros:  pb.Int64(100000000),
				Ttl:         pb.Float32(20),
				Description: pb.String("desc"),
				State:       pb.String("critical"),
				MetricF:     pb.Float32(f32),
				Service:     pb.String("foobar"),
				Tags:        []string{"hello"},
			},
		},
		{
			desc: "test float64",
			event: &Event{
				Host:          "baz",
				Service:       "foobar",
				TTL:           20 * time.Second,
				Description:   "desc",
				State:         "critical",
				MetricFloat64: &f64,
				Tags:          []string{"hello"},
				Time:          time.Unix(100, 0),
			},
			expected: &proto.Event{
				Host:        pb.String("baz"),
				Time:        pb.Int64(100),
				TimeMicros:  pb.Int64(100000000),
				Ttl:         pb.Float32(20),
				Description: pb.String("desc"),
				State:       pb.String("critical"),
				MetricD:     pb.Float64(f64),
				Service:     pb.String("foobar"),
				Tags:        []string{"hello"},
			},
		},
		{
			desc: "simple event with time in nanosecond",
			event: &Event{
				Host:        "baz",
				Service:     "foobar",
				MetricInt64: &i64,
				Tags:        []string{"hello"},
				Time:        time.Unix(100, 123456789),
			},
			expected: &proto.Event{
				Host:         pb.String("baz"),
				Time:         pb.Int64(100),
				TimeMicros:   pb.Int64(100123456),
				MetricSint64: pb.Int64(i64),
				State:        pb.String(""),
				Description:  pb.String(""),
				Ttl:          pb.Float32(0),
				Service:      pb.String("foobar"),
				Tags:         []string{"hello"},
			},
		},
	}

	for _, tc := range testCases {
		obtained, err := tc.event.toProto()

		if err != nil {
			t.Errorf(
				"Marshal error %s (%s)",
				err, tc.desc,
			)
		}

		if !pb.Equal(obtained, tc.expected) {
			t.Errorf(
				"Error during event to protobuf conversion (%s)",
				tc.desc,
			)
		}
	}
}
