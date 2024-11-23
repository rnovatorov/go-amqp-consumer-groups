package memberlist

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rnovatorov/go-routine"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
)

type Member struct {
	routineGroup      *routine.Group
	logger            *slog.Logger
	conn              *amqpconn.Conn
	groupID           string
	memberID          string
	heartbeatInterval time.Duration
	heartbeatLiveness time.Duration
}

type MemberParams struct {
	Context           context.Context
	Logger            *slog.Logger
	Conn              *amqpconn.Conn
	GroupID           string
	MemberID          string
	HeartbeatInterval time.Duration
	HeartbeatLiveness time.Duration
}

func NewMember(p MemberParams) (*Member, error) {
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
	if p.MemberID == "" {
		return nil, ErrMemberIDEmpty
	}
	if p.HeartbeatInterval == 0 {
		p.HeartbeatInterval = time.Second
	}
	if p.HeartbeatLiveness == 0 {
		p.HeartbeatLiveness = 5 * time.Second
	}

	m := &Member{
		routineGroup:      routine.NewGroup(p.Context),
		logger:            p.Logger,
		conn:              p.Conn,
		groupID:           p.GroupID,
		memberID:          p.MemberID,
		heartbeatInterval: p.HeartbeatInterval,
		heartbeatLiveness: p.HeartbeatLiveness,
	}
	m.routineGroup.Go(m.publishHeartbeatsLoop)

	return m, nil
}

func (m *Member) Close() {
	m.routineGroup.Stop()
}

func (m *Member) publishHeartbeatsLoop(ctx context.Context) error {
	channel := m.conn.NewChannel(amqpconn.ChannelParams{
		ID:                fmt.Sprintf("%s.%s", m.groupID, m.memberID),
		PublisherConfirms: false,
		TopologyBuilder:   m.buildTopology,
	})
	defer channel.Close()

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.publishHeartbeat(ctx, channel); err != nil {
				m.logger.Error("failed to publish heartbeat",
					slog.String("error", err.Error()))
			}
		}
	}
}

func (m *Member) publishHeartbeat(
	ctx context.Context, channel *amqpconn.Channel,
) error {
	expiration := strconv.Itoa(int(m.heartbeatLiveness / time.Millisecond))

	payload, err := json.Marshal(Heartbeat{
		MemberID:  m.memberID,
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	return channel.Publish(ctx, amqpconn.PublishParams{
		Exchange: m.exchangeName(),
		Msg: amqp091.Publishing{
			Expiration: expiration,
			Body:       payload,
		},
	})
}

func (m *Member) buildTopology(_ context.Context, ch *amqp091.Channel) error {
	if err := ch.ExchangeDeclare(
		m.exchangeName(),       // name
		amqp091.ExchangeFanout, // type
		false,                  // durable
		true,                   // auto-delete
		false,                  // internal
		false,                  // no-wait
		nil,                    // args
	); err != nil {
		return fmt.Errorf("declare heartbeats exchange: %w", err)
	}

	return nil
}

func (m *Member) exchangeName() string {
	return fmt.Sprintf("member-list.%s", m.groupID)
}

func (m *Member) queueName() string {
	return fmt.Sprintf("%s.%s", m.exchangeName(), m.memberID)
}
