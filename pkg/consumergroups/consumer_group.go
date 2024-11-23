package consumergroups

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
	"github.com/rnovatorov/go-amqp-consumer-groups/internal/consistenthashing"
	"github.com/rnovatorov/go-amqp-consumer-groups/internal/consumergroup"
	"github.com/rnovatorov/go-amqp-consumer-groups/internal/memberlist"
)

type ConsumerGroup struct {
	routineGroup      *routine.Group
	logger            *slog.Logger
	amqpURL           string
	id                string
	partitions        int
	consumerID        string
	heartbeatInterval time.Duration
	heartbeatLiveness time.Duration
	started           chan struct{}
	consumer          *consumergroup.Consumer
}

type Params struct {
	Context           context.Context
	Logger            *slog.Logger
	AMQPURL           string
	ID                string
	Partitions        int
	ConsumerID        string
	HeartbeatInterval time.Duration
	HeartbeatLiveness time.Duration
}

func New(p Params) (*ConsumerGroup, error) {
	if p.Context == nil {
		p.Context = context.Background()
	}
	if p.ConsumerID == "" {
		p.ConsumerID = uuid.NewString()
	}

	g := &ConsumerGroup{
		routineGroup:      routine.NewGroup(p.Context),
		logger:            p.Logger,
		amqpURL:           p.AMQPURL,
		id:                p.ID,
		partitions:        p.Partitions,
		consumerID:        p.ConsumerID,
		heartbeatInterval: p.HeartbeatInterval,
		heartbeatLiveness: p.HeartbeatLiveness,
		started:           make(chan struct{}),
		consumer:          nil,
	}

	r := g.routineGroup.Go(g.run)
	select {
	case <-r.Stopped():
		return nil, g.routineGroup.Stop()
	case <-g.started:
		return g, nil
	}
}

func (g *ConsumerGroup) Close() {
	g.routineGroup.Stop()
}

func (g *ConsumerGroup) Consume() <-chan amqp091.Delivery {
	return g.consumer.Deliveries()
}

func (g *ConsumerGroup) run(ctx context.Context) error {
	publisherConn, err := amqpconn.New(amqpconn.Params{
		Context: ctx,
		Logger:  g.logger,
		URL:     g.amqpURL,
		Name:    fmt.Sprintf("%s.%s.publisher", g.id, g.consumerID),
	})
	if err != nil {
		return fmt.Errorf("new publisher conn: %w", err)
	}
	defer publisherConn.Close()

	consumerConn, err := amqpconn.New(amqpconn.Params{
		Context: ctx,
		Logger:  g.logger,
		URL:     g.amqpURL,
		Name:    fmt.Sprintf("%s.%s.consumer", g.id, g.consumerID),
	})
	if err != nil {
		return fmt.Errorf("new publisher conn: %w", err)
	}
	defer consumerConn.Close()

	group, err := consumergroup.New(consumergroup.Params{
		Context:             ctx,
		Logger:              g.logger,
		Conn:                consumerConn,
		ID:                  g.id,
		Partitions:          g.partitions,
		ResourceDistributor: consistenthashing.NewRing(),
	})
	if err != nil {
		return fmt.Errorf("new group: %w", err)
	}
	defer group.Close()

	member, err := memberlist.NewMember(memberlist.MemberParams{
		Context:           ctx,
		Logger:            g.logger,
		Conn:              publisherConn,
		GroupID:           g.id,
		MemberID:          g.consumerID,
		HeartbeatInterval: g.heartbeatInterval,
		HeartbeatLiveness: g.heartbeatLiveness,
	})
	if err != nil {
		return fmt.Errorf("new memberlist member: %w", err)
	}
	defer member.Close()

	list, err := memberlist.NewList(memberlist.ListParams{
		Context:           ctx,
		Logger:            g.logger,
		Conn:              consumerConn,
		GroupID:           g.id,
		HeartbeatInterval: g.heartbeatInterval,
		HeartbeatLiveness: g.heartbeatLiveness,
		EventHandler: memberlist.EventHandler{
			HandleMemberAdded:   group.AddMember,
			HandleMemberRemoved: group.RemoveMember,
		},
	})
	if err != nil {
		return fmt.Errorf("new memberlist list: %w", err)
	}
	defer list.Close()

	g.consumer = group.NewConsumer(g.consumerID)
	close(g.started)

	<-ctx.Done()

	return nil
}
