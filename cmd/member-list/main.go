package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/google/uuid"

	"github.com/rnovatorov/go-amqp-consumer-groups/internal/amqpconn"
	"github.com/rnovatorov/go-amqp-consumer-groups/internal/memberlist"
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

	publisherConn, err := amqpconn.New(amqpconn.Params{
		Context: ctx,
		Logger:  logger,
		URL:     amqpURL,
		Name:    "publisher",
	})
	if err != nil {
		return fmt.Errorf("new publisher conn: %w", err)
	}
	defer publisherConn.Close()

	consumerConn, err := amqpconn.New(amqpconn.Params{
		Context: ctx,
		Logger:  logger,
		URL:     amqpURL,
		Name:    "consumer",
	})
	if err != nil {
		return fmt.Errorf("new consumer conn: %w", err)
	}
	defer consumerConn.Close()

	const groupID = "test-group-id"

	member, err := memberlist.NewMember(memberlist.MemberParams{
		Context:  ctx,
		Logger:   logger,
		Conn:     publisherConn,
		GroupID:  groupID,
		MemberID: uuid.NewString(),
	})
	if err != nil {
		return fmt.Errorf("new member list: %w", err)
	}
	defer member.Close()

	list, err := memberlist.NewList(memberlist.ListParams{
		Context: ctx,
		Logger:  logger,
		Conn:    consumerConn,
		GroupID: groupID,
	})
	if err != nil {
		return fmt.Errorf("new memberlist list: %w", err)
	}
	defer list.Close()

	<-ctx.Done()

	return nil
}
