package consistenthashing

type member string

func (m member) String() string {
	return string(m)
}
