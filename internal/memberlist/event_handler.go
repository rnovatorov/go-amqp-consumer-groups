package memberlist

type EventHandler struct {
	HandleMemberAdded   func(id string)
	HandleMemberRemoved func(id string)
}
