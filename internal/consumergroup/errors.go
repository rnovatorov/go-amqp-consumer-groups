package consumergroup

import "errors"

var (
	ErrConnMissing = errors.New("conn missing")
	ErrIDMissing   = errors.New("ID missing")
)
