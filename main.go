// Reproduces a reconnect race in github.com/wagslane/go-rabbitmq, see README.md.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

const (
	image             = "rabbitmq:4.3.7"
	reconnectInterval = 200 * time.Millisecond
	timeout           = 10 * time.Second
)

// instrumentedConn wraps the socket so the program can hang writes, like a peer that stopped reading,
// and wait for the handshake and the close.
type instrumentedConn struct {
	net.Conn
	hangWrites    atomic.Bool
	writeBlocked  chan struct{}
	release       chan struct{}
	handshakeDone chan struct{}
	closeCalled   chan struct{}
	once          [3]sync.Once
}

// SetDeadline with a zero time marks the end of the handshake: amqp091-go clears the deadline then.
func (c *instrumentedConn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		c.once[0].Do(func() { close(c.handshakeDone) })
	}
	return c.Conn.SetDeadline(t)
}

func (c *instrumentedConn) Write(b []byte) (int, error) {
	if c.hangWrites.Load() {
		c.once[1].Do(func() { close(c.writeBlocked) })
		<-c.release
	}
	return c.Conn.Write(b)
}

func (c *instrumentedConn) Close() error {
	c.once[2].Do(func() { close(c.closeCalled) })
	return c.Conn.Close()
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	tclog.SetDefault(tclog.NewNoopLogger())
	ctx := context.Background()
	rmq, err := tcrabbitmq.Run(ctx, image)
	if rmq != nil {
		defer testcontainers.TerminateContainer(rmq)
	}
	if err != nil {
		return err
	}
	url, err := rmq.AmqpURL(ctx)
	if err != nil {
		return err
	}

	conns := make(chan *instrumentedConn, 10)
	var dials atomic.Int32
	dial := func(network, addr string) (net.Conn, error) {
		nc, err := net.DialTimeout(network, addr, 5*time.Second)
		if err != nil {
			return nil, err
		}
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		c := &instrumentedConn{
			Conn:          nc,
			writeBlocked:  make(chan struct{}),
			release:       make(chan struct{}),
			handshakeDone: make(chan struct{}),
			closeCalled:   make(chan struct{}),
		}
		dials.Add(1)
		conns <- c
		return c, nil
	}

	conn, err := rabbitmq.NewConn(url,
		rabbitmq.WithConnectionOptionsConfig(rabbitmq.Config{Dial: dial}),
		rabbitmq.WithConnectionOptionsBaseReconnectInterval(reconnectInterval),
	)
	if err != nil {
		return err
	}
	pub, err := rabbitmq.NewPublisher(conn)
	if err != nil {
		return err
	}
	publish := func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return pub.PublishWithContext(ctx, []byte("hi"), []string{"repro"})
	}

	log.Println("1. connection #1 is up, publishing works")
	if err := publish(); err != nil {
		return err
	}
	c1 := <-conns

	log.Println("2. a publish gets stuck writing to connection #1")
	c1.hangWrites.Store(true)
	go func() { log.Println("stuck publish returned:", publish()) }()
	if err := wait(c1.writeBlocked, "publish to get stuck"); err != nil {
		return err
	}

	log.Println("3. break connection #1, go-rabbitmq reconnects with connection #2")
	_ = c1.Conn.Close()
	c2 := <-conns
	if err := wait(c2.handshakeDone, "connection #2 handshake"); err != nil {
		return err
	}

	log.Println("4. break connection #2")
	_ = c2.Conn.Close()
	if err := wait(c2.closeCalled, "connection #2 to close"); err != nil {
		return err
	}

	log.Println("5. let the stuck publish return")
	close(c1.release)

	log.Println("6. publish every second")
	for {
		time.Sleep(time.Second)
		log.Println("publish:", publish(), "IsClosed:", conn.IsClosed(), "dials:", dials.Load())
	}
}

func wait(ch <-chan struct{}, what string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for %s", what)
	}
}
