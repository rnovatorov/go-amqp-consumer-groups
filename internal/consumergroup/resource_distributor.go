package consumergroup

type ResourceDistributor interface {
	AddMember(id string)
	RemoveMember(id string)
	HasMembers() bool
	ResourceOwner(resource string) (memberID string)
}
