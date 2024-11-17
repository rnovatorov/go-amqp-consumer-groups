package amqpconn

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"
)

type Conn struct {
	routineGroup *routine.Group
	logger       *slog.Logger
	url          string
	name         string
	connCh       chan *amqp091.Connection
}

type Params struct {
	Context context.Context
	Logger  *slog.Logger
	URL     string
	Name    string
}

func New(p Params) (*Conn, error) {
	if p.Context == nil {
		p.Context = context.Background()
	}
	if p.Logger == nil {
		p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if p.URL == "" {
		return nil, ErrURLEmpty
	}

	c := &Conn{
		routineGroup: routine.NewGroup(p.Context),
		logger:       p.Logger,
		url:          p.URL,
		name:         p.Name,
		connCh:       make(chan *amqp091.Connection),
	}
	c.routineGroup.Go(c.serveLoop)

	return c, nil
}

func (c *Conn) Close() {
	c.routineGroup.Stop()
}

func (c *Conn) NewChannel(p ChannelParams) *Channel {
	channel := &Channel{
		routineGroup:           routine.NewGroup(c.routineGroup.Context()),
		logger:                 c.logger.With(slog.String("channel_id", p.ID)),
		conn:                   c,
		channelCh:              make(chan *amqp091.Channel),
		id:                     p.ID,
		publisherConfirms:      p.PublisherConfirms,
		topologyBuilder:        p.TopologyBuilder,
		consumerPrefetchCount:  p.ConsumerPrefetchCount,
		consumerPrefetchSize:   p.ConsumerPrefetchSize,
		consumerPrefetchGlobal: p.ConsumerPrefetchGlobal,
	}
	channel.routineGroup.Go(channel.serveLoop)

	return channel
}

func (c *Conn) serveLoop(ctx context.Context) error {
	const retryInterval = 5 * time.Second

	for {
		if err := c.serve(ctx); err != nil {
			c.logger.Warn("failed to serve",
				slog.Duration("retrying_in", retryInterval),
				slog.String("error", err.Error()))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryInterval):
		}
	}
}

func (c *Conn) serve(ctx context.Context) error {
	conn, err := c.dial()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.logger.Debug("connected")
	defer func() {
		if err := conn.Close(); err != nil {
			c.logger.Warn("failed to close conn",
				slog.String("error", err.Error()))
		}
		c.logger.Debug("disconnected")
	}()

	closed := conn.NotifyClose(make(chan *amqp091.Error, 1))

	for {
		select {
		case c.connCh <- conn:
		case err := <-closed:
			if err != nil {
				return fmt.Errorf("closed: %w", err)
			}
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *Conn) get(ctx context.Context) (*amqp091.Connection, error) {
	select {
	case conn := <-c.connCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Conn) dial() (*amqp091.Connection, error) {
	properties := amqp091.NewConnectionProperties()
	if c.name != "" {
		properties.SetClientConnectionName(c.name)
	}

	return amqp091.DialConfig(c.url, amqp091.Config{
		Properties: properties,
	})
}
