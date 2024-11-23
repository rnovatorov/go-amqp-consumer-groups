package consistenthashing

import (
	"github.com/buraksezer/consistent"
)

type Ring struct {
	c *consistent.Consistent
}

func NewRing() *Ring {
	return &Ring{
		c: consistent.New(nil, consistent.Config{
			Hasher:            hasher{},
			PartitionCount:    17,
			ReplicationFactor: 1000,
			Load:              1.25,
		}),
	}
}

func (r *Ring) AddMember(id string) {
	r.c.Add(member(id))
}

func (r *Ring) RemoveMember(id string) {
	r.c.Remove(id)
}

func (r *Ring) HasMembers() bool {
	return len(r.c.GetMembers()) > 0
}

func (r *Ring) ResourceOwner(resource string) (memberID string) {
	return r.c.LocateKey([]byte(resource)).String()
}
