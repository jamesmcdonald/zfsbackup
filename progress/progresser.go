package progress

type Progresser interface {
	Add(int64)
	Done()
}
