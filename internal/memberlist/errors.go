package memberlist

import "errors"

var (
	ErrGroupIDEmpty         = errors.New("group ID empty")
	ErrMemberIDEmpty        = errors.New("member ID empty")
	ErrPublisherConnMissing = errors.New("publisher conn missing")
	ErrConnPairMissing      = errors.New("conn pair missing")
	ErrConsumerConnMissing  = errors.New("consumer conn missing")
	ErrConnMissing          = errors.New("conn missing")
)
