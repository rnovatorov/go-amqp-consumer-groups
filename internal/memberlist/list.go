package memberlist

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
)

type List struct {
	routineGroup      *routine.Group
	logger            *slog.Logger
	conn              *amqpconn.Conn
	groupID           string
	queueID           string
	heartbeatInterval time.Duration
	heartbeatLiveness time.Duration
	eventHandler      EventHandler
	mu                sync.Mutex
	lastSeen          map[string]time.Time
}

type ListParams struct {
	Context           context.Context
	Logger            *slog.Logger
	Conn              *amqpconn.Conn
	GroupID           string
	HeartbeatInterval time.Duration
	HeartbeatLiveness time.Duration
	EventHandler      EventHandler
}

func NewList(p ListParams) (*List, error) {
	if p.Context == nil {
		p.Context = context.Background()
	}
	if p.Logger == nil {
		p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if p.Conn == nil {
		return nil, ErrConnMissing
	}
	if p.GroupID == "" {
		return nil, ErrGroupIDEmpty
	}
	if p.HeartbeatInterval == 0 {
		p.HeartbeatInterval = time.Second
	}
	if p.HeartbeatLiveness == 0 {
		p.HeartbeatLiveness = 5 * time.Second
	}
	if p.EventHandler.HandleMemberAdded == nil {
		p.EventHandler.HandleMemberAdded = func(string) {}
	}
	if p.EventHandler.HandleMemberRemoved == nil {
		p.EventHandler.HandleMemberRemoved = func(string) {}
	}

	l := &List{
		routineGroup:      routine.NewGroup(p.Context),
		logger:            p.Logger,
		conn:              p.Conn,
		groupID:           p.GroupID,
		queueID:           uuid.NewString(),
		heartbeatInterval: p.HeartbeatInterval,
		heartbeatLiveness: p.HeartbeatLiveness,
		eventHandler: EventHandler{
			HandleMemberAdded: func(id string) {
				p.Logger.Info("member added", slog.String("id", id))
				p.EventHandler.HandleMemberAdded(id)
			},
			HandleMemberRemoved: func(id string) {
				p.Logger.Info("member removed", slog.String("id", id))
				p.EventHandler.HandleMemberRemoved(id)
			},
		},
		lastSeen: make(map[string]time.Time),
	}
	l.routineGroup.Go(l.removeOfflineMembersLoop)
	l.routineGroup.Go(l.consumeHeartbeatsLoop)

	return l, nil
}

func (l *List) Close() {
	l.routineGroup.Stop()
}

func (l *List) Members() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	members := make([]string, 0, len(l.lastSeen))
	for member := range l.lastSeen {
		members = append(members, member)
	}
	sort.Strings(members)

	return members
}

func (l *List) removeOfflineMembersLoop(ctx context.Context) error {
	const interval = time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			l.removeOfflineMembers()
		}
	}
}

func (l *List) removeOfflineMembers() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for member, heartbeat := range l.lastSeen {
		if time.Since(heartbeat) > l.heartbeatLiveness {
			delete(l.lastSeen, member)
			l.eventHandler.HandleMemberRemoved(member)
		}
	}
}

func (l *List) consumeHeartbeatsLoop(ctx context.Context) error {
	deliveries := make(chan amqp091.Delivery)

	channel := l.conn.NewChannel(amqpconn.ChannelParams{
		ID:              l.groupID,
		TopologyBuilder: l.buildTopology,
	})
	defer channel.Close()

	channel.Consume(amqpconn.ConsumeParams{
		Queue:      l.queueName(),
		Deliveries: deliveries,
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery := <-deliveries:
			l.consumeHeartbeat(ctx, delivery)
		}
	}
}

func (l *List) consumeHeartbeat(_ context.Context, delivery amqp091.Delivery) {
	defer delivery.Ack(
		false, // multiple
	)

	var heartbeat Heartbeat
	if err := json.Unmarshal(delivery.Body, &heartbeat); err != nil {
		l.logger.Error("failed to unmarshal heartbeat",
			slog.String("error", err.Error()))
		return
	}
	if time.Since(heartbeat.Timestamp) > l.heartbeatLiveness {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	_, seen := l.lastSeen[heartbeat.MemberID]
	l.lastSeen[heartbeat.MemberID] = heartbeat.Timestamp
	if !seen {
		l.eventHandler.HandleMemberAdded(heartbeat.MemberID)
	}
}

func (l *List) buildTopology(_ context.Context, ch *amqp091.Channel) error {
	q, err := ch.QueueDeclare(
		l.queueName(), // name
		false,         // durable
		true,          // auto-delete
		true,          // exclusive
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return fmt.Errorf("declare heartbeats queue: %w", err)
	}

	if err := ch.QueueBind(
		q.Name,           // name
		"",               // key
		l.exchangeName(), // exchange
		false,            // noWait
		nil,              // args
	); err != nil {
		return fmt.Errorf("bind heartbeats queue: %w", err)
	}

	return nil
}

func (l *List) exchangeName() string {
	return fmt.Sprintf("member-list.%s", l.groupID)
}

func (l *List) queueName() string {
	return fmt.Sprintf("%s.%s", l.exchangeName(), l.queueID)
}
