package mr

import "time"

// RPC definitions.

// TaskState
const (
	Waiting = iota
	InProgress
	Done
)

// TaskType
const (
	Map = iota
	Reduce
	Wait
)

type GetTaskArgs struct{}

type Task struct {
	FileName  string
	TaskType  int
	NReduce   int
	Status    int
	TaskNum   int
	StartedAt time.Time
}

type TaskDoneArgs struct {
	TaskNum  int
	TaskType int
}

type TaskDoneReply struct{}
