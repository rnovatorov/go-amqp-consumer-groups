package amqpconn

import "errors"

var (
	ErrURLEmpty            = errors.New("URL empty")
	ErrPublishNotConfirmed = errors.New("publish not confirmed")
)
