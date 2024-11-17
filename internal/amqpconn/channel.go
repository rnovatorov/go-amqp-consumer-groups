package amqpconn

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"
)

type ChannelParams struct {
	ID                     string
	PublisherConfirms      bool
	TopologyBuilder        TopologyBuilder
	ConsumerPrefetchCount  int
	ConsumerPrefetchSize   int
	ConsumerPrefetchGlobal bool
}

type Channel struct {
	routineGroup           *routine.Group
	logger                 *slog.Logger
	conn                   *Conn
	channelCh              chan *amqp091.Channel
	id                     string
	publisherConfirms      bool
	topologyBuilder        TopologyBuilder
	consumerPrefetchCount  int
	consumerPrefetchSize   int
	consumerPrefetchGlobal bool
}

func (c *Channel) Close() {
	c.routineGroup.Stop()
}

type PublishParams struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp091.Publishing
}

func (c *Channel) Publish(ctx context.Context, ps PublishParams) error {
	ch, err := c.get(ctx)
	if err != nil {
		return fmt.Errorf("get channel: %w", err)
	}

	if c.publisherConfirms {
		confirm, err := ch.PublishWithDeferredConfirm(
			ps.Exchange, ps.Key, ps.Mandatory, ps.Immediate, ps.Msg,
		)
		if err != nil {
			return fmt.Errorf("publish with confirm: %w", err)
		}
		ok, err := confirm.WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("wait confirm: %w", err)
		}
		if !ok {
			return ErrPublishNotConfirmed
		}
		return nil
	}

	if err := ch.Publish(
		ps.Exchange, ps.Key, ps.Mandatory, ps.Immediate, ps.Msg,
	); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

type ConsumeParams struct {
	Queue      string
	AutoAck    bool
	Exclusive  bool
	NoLocal    bool
	NoWait     bool
	Args       amqp091.Table
	Deliveries chan amqp091.Delivery
}

func (c *Channel) Consume(p ConsumeParams) {
	c.routineGroup.Go(func(ctx context.Context) error {
		return c.consumeMessagesLoop(ctx, p)
	})
}

func (c *Channel) consumeMessagesLoop(ctx context.Context, p ConsumeParams) error {
	const retryInterval = 5 * time.Second

	for {
		if err := c.consumeMessages(ctx, p); err != nil {
			c.logger.Warn("failed to consume messages",
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

func (c *Channel) consumeMessages(ctx context.Context, p ConsumeParams) error {
	ch, err := c.get(ctx)
	if err != nil {
		return fmt.Errorf("get channel: %w", err)
	}

	deliveries, err := ch.Consume(
		p.Queue, c.id, p.AutoAck, p.Exclusive, p.NoLocal, p.NoWait, p.Args,
	)
	if err != nil {
		return fmt.Errorf("consume from channel: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery := <-deliveries:
			select {
			case <-ctx.Done():
				delivery.Reject(
					true, // requeue
				)
				return nil
			case p.Deliveries <- delivery:
			}
		}
	}
}

func (c *Channel) serveLoop(ctx context.Context) error {
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

func (c *Channel) serve(ctx context.Context) error {
	ch, err := c.open(ctx)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if b := c.topologyBuilder; b != nil {
		if err := b(ctx, ch); err != nil {
			return fmt.Errorf("build topology: %w", err)
		}
	}

	if c.publisherConfirms {
		if err := ch.Confirm(
			false, // no-wait
		); err != nil {
			return fmt.Errorf("enable publisher confirms: %w", err)
		}
	}

	if c.consumerPrefetchCount != 0 || c.consumerPrefetchSize != 0 {
		if err := ch.Qos(
			c.consumerPrefetchCount, c.consumerPrefetchSize,
			c.consumerPrefetchGlobal,
		); err != nil {
			return fmt.Errorf("set qos: %w", err)
		}
	}

	closed := ch.NotifyClose(make(chan *amqp091.Error, 1))

	for {
		select {
		case c.channelCh <- ch:
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

func (c *Channel) get(ctx context.Context) (*amqp091.Channel, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch := <-c.channelCh:
		return ch, nil
	}
}

func (c *Channel) open(ctx context.Context) (*amqp091.Channel, error) {
	conn, err := c.conn.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get conn: %w", err)
	}

	return conn.Channel()
}
