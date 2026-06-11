package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	mu sync.Mutex

	mapTasks    []*Task
	reduceTasks []*Task

	mapCounter    int
	reduceCounter int

	Phase int
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Phase {
	case MapPhase:
		for _, t := range c.mapTasks {
			if t.Status == Waiting {
				reply.FileName = t.FileName
				reply.TaskType = t.TaskType
				reply.NReduce = t.NReduce
				reply.TaskNum = t.TaskNum
				t.Status = InProgress
				t.StartedAt = time.Now()
				log.Printf("The task was given: type:%v – num:%d", reply.TaskType, reply.TaskNum)
				return nil
			}
		}
		reply.TaskType = Wait
	case ReducePhase:
		for _, t := range c.reduceTasks {
			if t.Status == Waiting {
				reply.FileName = t.FileName
				reply.TaskType = t.TaskType
				reply.NReduce = t.NReduce
				reply.TaskNum = t.TaskNum
				t.Status = InProgress
				t.StartedAt = time.Now()
				log.Printf("The task was given: type:%v – num:%d", reply.TaskType, reply.TaskNum)
				return nil
			}
		}
		reply.TaskType = Wait
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == Map {
		if c.mapTasks[args.TaskNum].Status == Done {
			return nil
		}
		c.mapTasks[args.TaskNum].Status = Done
		c.mapCounter++
		if c.mapCounter == len(c.mapTasks) {
			c.Phase = ReducePhase
		}
		log.Printf("The task was completed: type:%v – num:%d", args.TaskType, args.TaskNum)
	} else {
		if c.reduceTasks[args.TaskNum].Status == Done {
			return nil
		}
		c.reduceTasks[args.TaskNum].Status = Done
		c.reduceCounter++
		if c.reduceCounter == len(c.reduceTasks) {
			c.Phase = AllDone
		}
		log.Printf("The task was completed: type:%v – num:%d", args.TaskType, args.TaskNum)
	}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false

	if c.Phase == AllDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]*Task, 0, len(files)),
		reduceTasks: make([]*Task, 0, nReduce),

		Phase: MapPhase,
	}

	for i, f := range files {
		c.mapTasks = append(c.mapTasks, &Task{
			FileName: f,
			TaskType: Map,
			NReduce:  nReduce,
			TaskNum:  i,
			Status:   Waiting,
		})
	}

	for i := range nReduce {
		c.reduceTasks = append(c.reduceTasks, &Task{
			TaskType: Reduce,
			TaskNum:  i,
			Status:   Waiting,
		})
	}

	c.server(sockname)

	go func() {
		for {
			c.mu.Lock()
			if c.Phase == AllDone {
				c.mu.Unlock()
				break
			}
			c.mu.Unlock()

			c.checkWorkers()
			time.Sleep(5 * time.Second)
		}
	}()

	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %v: %v", sockname, e)
	}
	go http.Serve(l, nil)

	log.Printf("coordinator start listening on %v", sockname)
}

func (c *Coordinator) checkWorkers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.mapTasks {
		if t.Status == InProgress && time.Since(t.StartedAt) >= 10*time.Second {
			c.mapTasks[t.TaskNum].Status = Waiting
			log.Printf("The task was recalled due to TTL exceeded: type:%v – num:%d", t.TaskType, t.TaskNum)
		}
	}
	for _, t := range c.reduceTasks {
		if t.Status == InProgress && time.Since(t.StartedAt) >= 10*time.Second {
			c.reduceTasks[t.TaskNum].Status = Waiting
			log.Printf("The task was recalled due to TTL exceeded: type:%v – num:%d", t.TaskType, t.TaskNum)
		}
	}
}
