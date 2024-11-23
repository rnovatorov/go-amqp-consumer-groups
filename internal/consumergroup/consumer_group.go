package consumergroup

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
)

type ConsumerGroup struct {
	routineGroup        *routine.Group
	logger              *slog.Logger
	conn                *amqpconn.Conn
	id                  string
	partitions          int
	mu                  sync.Mutex
	resourceDistributor ResourceDistributor
	consumers           map[string]*Consumer
}

type Params struct {
	Context             context.Context
	Logger              *slog.Logger
	Conn                *amqpconn.Conn
	ID                  string
	Partitions          int
	ResourceDistributor ResourceDistributor
}

func New(p Params) (*ConsumerGroup, error) {
	if p.Context == nil {
		p.Context = context.Background()
	}
	if p.Logger == nil {
		p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if p.Conn == nil {
		return nil, ErrConnMissing
	}
	if p.ID == "" {
		return nil, ErrIDMissing
	}
	if p.Partitions == 0 {
		p.Partitions = 2
	}

	return &ConsumerGroup{
		routineGroup:        routine.NewGroup(p.Context),
		logger:              p.Logger,
		conn:                p.Conn,
		id:                  p.ID,
		partitions:          p.Partitions,
		resourceDistributor: p.ResourceDistributor,
		consumers:           make(map[string]*Consumer),
	}, nil
}

func (g *ConsumerGroup) Close() {
	g.routineGroup.Stop()
}

func (g *ConsumerGroup) AddMember(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.resourceDistributor.AddMember(id)
	g.rebalance()
}

func (g *ConsumerGroup) RemoveMember(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.resourceDistributor.RemoveMember(id)
	g.rebalance()
}

func (g *ConsumerGroup) NewConsumer(consumerID string) *Consumer {
	g.mu.Lock()
	defer g.mu.Unlock()

	c := &Consumer{
		routineGroup:    g.routineGroup,
		groupID:         g.id,
		id:              consumerID,
		logger:          g.logger,
		conn:            g.conn,
		topologyBuilder: g.buildTopology,
		queueWorkers:    make(map[string]*routine.Routine),
		deliveries:      make(chan amqp091.Delivery, g.partitions),
	}

	g.consumers[consumerID] = c
	g.rebalance()

	return c
}

func (g *ConsumerGroup) rebalance() {
	if !g.resourceDistributor.HasMembers() {
		return
	}

	for _, consumer := range g.consumers {
		for queue := range consumer.queueWorkers {
			owner := g.resourceDistributor.ResourceOwner(queue)
			if owner != consumer.id {
				consumer.removeQueue(queue)
			}
		}
	}

	for partition := 1; partition <= g.partitions; partition++ {
		queue := g.queueName(partition)
		owner := g.resourceDistributor.ResourceOwner(queue)
		if consumer, ok := g.consumers[owner]; ok {
			consumer.addQueue(queue)
		}
	}
}

func (g *ConsumerGroup) buildTopology(_ context.Context, ch *amqp091.Channel) error {
	if err := ch.ExchangeDeclare(
		g.exchangeName(),    // name
		"x-consistent-hash", // type
		true,                // durable
		false,               // auto-delete
		false,               // internal
		false,               // no-wait
		nil,                 // args
	); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	for partition := 1; partition <= g.partitions; partition++ {
		q, err := ch.QueueDeclare(
			g.queueName(partition), // name
			true,                   // durable
			false,                  // auto-delete
			false,                  // exclusive
			false,                  // no-wait
			amqp091.Table{
				"x-queue-type": "quorum",
			}, // args
		)
		if err != nil {
			return fmt.Errorf("%d: declare queue: %w", partition, err)
		}
		if err := ch.QueueBind(
			q.Name,           // name
			"1",              // key
			g.exchangeName(), // exchange
			false,            // noWait
			nil,              // args
		); err != nil {
			return fmt.Errorf("%d: bind queue: %w", partition, err)
		}
	}

	return nil
}

func (g *ConsumerGroup) exchangeName() string {
	return fmt.Sprintf("consumer-group.%s", g.id)
}

func (g *ConsumerGroup) queueName(partition int) string {
	return fmt.Sprintf("%s.%d", g.exchangeName(), partition)
}
