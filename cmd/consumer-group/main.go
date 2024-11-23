package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
	"github.com/rnovatorov/go-amqp-consumer-groups/pkg/consumergroups"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	amqpURL := os.Getenv("AMQP_URL")

	pubConn, err := amqpconn.New(amqpconn.Params{
		Context: ctx,
		Logger:  logger,
		URL:     amqpURL,
		Name:    "publisher",
	})
	if err != nil {
		return fmt.Errorf("new pub conn: %w", err)
	}
	defer pubConn.Close()

	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		channel := pubConn.NewChannel(amqpconn.ChannelParams{})
		defer channel.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := channel.Publish(ctx, amqpconn.PublishParams{
					Exchange: "consumer-group.test-group-id",
					Key:      uuid.NewString(),
				}); err != nil {
					panic(err)
				}
			}
		}
	}()

	g, err := consumergroups.New(consumergroups.Params{
		Context:           ctx,
		Logger:            logger,
		AMQPURL:           amqpURL,
		ID:                "test-group-id",
		ConsumerID:        uuid.NewString(),
		Partitions:        60,
		HeartbeatLiveness: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("new group: %w", err)
	}
	defer g.Close()

	for {
		select {
		case d := <-g.Consume():
			// fmt.Printf("%+v\n", d)
			if err := d.Ack(false); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}
