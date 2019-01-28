package riemanngo

import (
	"errors"
	"os"
	"sort"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/riemann/riemann-go-client/proto"
)

type Event struct {
	TTL           time.Duration
	Time          time.Time
	Tags          []string
	Host          string
	State         string
	Service       string
	MetricInt64   *int64
	MetricFloat32 *float32
	MetricFloat64 *float64
	Description   string
	Attributes    map[string]string
}

func (e *Event) toProto() (*proto.Event, error) {
	e.setDefaults()

	pb := &proto.Event{
		Host:        pb.String(e.Host),
		Time:        pb.Int64(e.Time.Unix()),
		TimeMicros:  pb.Int64(e.Time.UnixNano() / int64(time.Microsecond)),
		Service:     pb.String(e.Service),
		State:       pb.String(e.State),
		Description: pb.String(e.Description),
		Tags:        e.Tags,
		Attributes:  e.buildAttributes(),
		Ttl:         pb.Float32(float32(e.TTL.Seconds())),
	}

	err := e.setMetric(pb)
	if err != nil {
		return nil, err
	}

	return pb, nil
}

func (e *Event) setDefaults() {
	if e.Host == "" {
		e.Host, _ = os.Hostname()
	}

	if e.Time.IsZero() {
		e.Time = time.Now()
	}
}

func (e *Event) setMetric(pb *proto.Event) error {
	if e.MetricInt64 != nil {
		pb.MetricSint64 = e.MetricInt64

		return nil
	}

	if e.MetricFloat32 != nil {
		pb.MetricF = e.MetricFloat32

		return nil
	}

	if e.MetricFloat64 != nil {
		pb.MetricD = e.MetricFloat64

		return nil
	}

	return errors.New(
		"At least one of MetricInt64, MetricFloat32, MetricFloat64 must be set",
	)
}

func (e *Event) buildAttributes() []*proto.Attribute {
	// XXX: is sorting attributes worth it ?

	keys := make(
		[]string, len(e.Attributes),
	)

	i := 0

	for attr := range e.Attributes {
		keys[i] = attr
		i++
	}

	sort.Strings(keys)

	// ---

	buff := make(
		[]*proto.Attribute, len(keys),
	)

	for i, k := range keys {
		buff[i] = &proto.Attribute{
			Key: pb.String(k),
			Value: pb.String(
				e.Attributes[k],
			),
		}
	}

	return buff
}
