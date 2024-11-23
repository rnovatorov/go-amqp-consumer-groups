package consumergroup

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
)

type Consumer struct {
	routineGroup    *routine.Group
	groupID         string
	id              string
	logger          *slog.Logger
	conn            *amqpconn.Conn
	topologyBuilder amqpconn.TopologyBuilder
	queueWorkers    map[string]*routine.Routine
	deliveries      chan amqp091.Delivery
}

func (c *Consumer) Deliveries() <-chan amqp091.Delivery {
	return c.deliveries
}

func (c *Consumer) addQueue(queue string) {
	if _, ok := c.queueWorkers[queue]; !ok {
		c.queueWorkers[queue] = c.routineGroup.Go(func(ctx context.Context) error {
			c.consumeMessages(ctx, queue)
			return nil
		})
		c.logger.Info("add queue",
			slog.String("name", queue),
			slog.Int("queue_workers", len(c.queueWorkers)))
	}
}

func (c *Consumer) removeQueue(queue string) {
	if w, ok := c.queueWorkers[queue]; ok {
		w.Stop()
		delete(c.queueWorkers, queue)
		c.logger.Info("remove queue",
			slog.String("name", queue),
			slog.Int("queue_workers", len(c.queueWorkers)))
	}
}

func (c *Consumer) consumeMessages(ctx context.Context, queue string) {
	channel := c.conn.NewChannel(amqpconn.ChannelParams{
		ID:                    fmt.Sprintf("%s.%s", c.groupID, c.id),
		TopologyBuilder:       c.topologyBuilder,
		ConsumerPrefetchCount: 1,
	})
	defer channel.Close()

	channel.Consume(amqpconn.ConsumeParams{
		Queue:      queue,
		Exclusive:  true,
		Deliveries: c.deliveries,
	})

	<-ctx.Done()
}
