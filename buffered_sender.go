package riemanngo

import (
	"sync"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/riemann/riemann-go-client/proto"
)

type senderFunc func([]byte) error

type bufferedSender struct {
	in chan *Event
	wg *sync.WaitGroup
	sf senderFunc
}

func newBufferedSender() *bufferedSender {
	bf := &bufferedSender{
		in: make(chan *Event, 1e6),
		wg: new(sync.WaitGroup),
	}

	bf.wg.Add(1)

	go bf.start()

	return bf
}

func (bf *bufferedSender) setSender(f senderFunc) {
	bf.sf = f
}

func (bf *bufferedSender) start() {
	var buff []*Event

	resetBuff := func() {
		buff = make([]*Event, 0, 5e2)
	}

	send := func() {
		msg, err := bf.buildMsg(buff)
		if err != nil {
			return
		}

		bf.sf(msg)
	}

	tkr := time.NewTicker(
		20 * time.Millisecond,
	)

	defer func() {
		tkr.Stop()

		send()

		bf.wg.Done()
	}()

	resetBuff()

	for {
		select {
		case e, open := <-bf.in:
			if !open {
				return
			}

			buff = append(buff, e)

			if len(buff) == cap(buff) {
				send()

				resetBuff()
			}

		case <-tkr.C:
			if len(buff) <= 0 {
				continue
			}

			send()

			resetBuff()
		}
	}
}

func (bf *bufferedSender) stop() {
	close(bf.in)

	bf.wg.Wait()
}

func (bf *bufferedSender) push(e *Event) {
	bf.in <- e
}

func (bf *bufferedSender) buildMsg(events []*Event) ([]byte, error) {
	buff := make(
		[]*proto.Event, len(events),
	)

	for i, e := range events {
		p, err := e.toProto()

		if err != nil {
			return nil, err
		}

		buff[i] = p
	}

	return pb.Marshal(
		&proto.Msg{
			Events: buff,
		},
	)
}
