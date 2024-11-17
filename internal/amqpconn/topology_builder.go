package amqpconn

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type TopologyBuilder func(context.Context, *amqp091.Channel) error
