package types

import (
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
)

type (
	MapFunc    func(args *MapArgs, res *MapResults)
	ReduceFunc func(args *ReduceArgs, res *ReduceResults)
)

type Task struct {
	Type       TaskType
	TaskID     int
	ChunkIndex int
	Data       string
}

type MapArgs struct {
	Key     string
	Content string
}

type MapResults struct {
	Word string
	Amt  int
}

type ReduceArgs struct {
	TaskID int
	List   map[string][]int
}

type ReduceResults struct {
	Value map[string]int
}

type TaskState struct {
	State      string
	AssignedAt time.Time
}
