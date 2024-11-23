package memberlist

import "time"

type Heartbeat struct {
	MemberID  string    `json:"member_id"`
	Timestamp time.Time `json:"timestamp"`
}
