package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const AssumeWorkerDeadTimeout = 10 * time.Second

type Coordinator struct {
	MapTasksStatus    map[int]MapTaskStatus
	ReduceTasksStatus map[int]TaskStatus
	Phase             Phase
	NReduce           int
	mu                sync.Mutex
}

type MapTaskStatus struct {
	TaskStatus TaskStatus
	Filename   string
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type Phase int

const (
	MapPhase Phase = iota
	ShufflePhase
	ReducePhase
	Finished
)

func (c *Coordinator) RequestTask(_, reply *CoordinatorCallReply) error {
	switch c.Phase {
	case MapPhase:
		c.mu.Lock()
		defer c.mu.Unlock()

		for k, v := range c.MapTasksStatus {
			if v.TaskStatus == Idle {
				reply.TaskType = Map
				reply.TaskId = k
				reply.Filename = v.Filename
				reply.NReduce = c.NReduce
				taskStatus := c.MapTasksStatus[k]
				taskStatus.TaskStatus = InProgress
				c.MapTasksStatus[k] = taskStatus

				go c.checkTaskStatus(k)
				return nil
			}
		}

		reply.TaskType = Sleep
	case ShufflePhase:
		reply.TaskType = Sleep
	case ReducePhase:
		c.mu.Lock()
		defer c.mu.Unlock()

		for k, v := range c.ReduceTasksStatus {
			if v == Idle {
				reply.TaskType = Reduce
				reply.TaskId = k
				reply.Filename = fmt.Sprintf("mr-%v.txt", k)
				c.ReduceTasksStatus[k] = InProgress

				go c.checkTaskStatus(k)
				return nil
			}
		}

		reply.TaskType = Sleep
	case Finished:
	}
	return nil
}

func (c *Coordinator) ReportTask(args *CoordinatorCallArgs, reply *CoordinatorCallReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Phase {
	case MapPhase:
		if args.WorkStatus == Completed {
			taskStatus := c.MapTasksStatus[args.TaskId]
			taskStatus.TaskStatus = Done
			c.MapTasksStatus[args.TaskId] = taskStatus
			mapDone := true
			for _, v := range c.MapTasksStatus {
				if v.TaskStatus != Done {
					mapDone = false
					break
				}
			}

			if mapDone {
				c.Phase = ShufflePhase
				c.Shuffle()
			}
		} else {
			taskStatus := c.MapTasksStatus[args.TaskId]
			taskStatus.TaskStatus = Idle
			c.MapTasksStatus[args.TaskId] = taskStatus
		}
	case ShufflePhase:
	case ReducePhase:
		if args.TaskType != Reduce {
			return nil
		}

		if args.WorkStatus == Completed {
			c.ReduceTasksStatus[args.TaskId] = Done

			reduceDone := true
			for _, v := range c.ReduceTasksStatus {
				if v != Done {
					reduceDone = false
					break
				}
			}

			if reduceDone {
				c.Phase = Finished
			}

		} else {
			c.ReduceTasksStatus[args.TaskId] = Idle
		}
	case Finished:
	}
	return nil
}

func (c *Coordinator) Shuffle() {
	for i := 0; i < c.NReduce; i++ {
		var partition ByKey
		for j := 0; j < len(c.MapTasksStatus); j++ {
			filename := fmt.Sprintf("../mr-tmp/mr-%v-%v.txt", j, i)
			file, err := os.Open(filename)
			if err != nil {
				continue
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				partition = append(partition, kv)
			}
			if err := file.Close(); err != nil {
				fmt.Printf("Cannot close intermediate file %v\n", filename)
			}
			if err := os.Remove(filename); err != nil {
				fmt.Printf("Cannot remove intermediate file %v\n", filename)
			}
		}
		partitionFilename := fmt.Sprintf("../mr-tmp/mr-%v.txt", i)
		partitionFile, err := os.Create(partitionFilename)
		if err != nil {
			continue
		}

		sort.Sort(partition)

		enc := json.NewEncoder(partitionFile)
		for _, kv := range partition {
			err := enc.Encode(&kv)
			if err != nil {
				continue
			}
		}
		if err := partitionFile.Close(); err != nil {
			fmt.Printf("Cannot close partition file %v\n", partitionFilename)
		}

	}

	for i := 0; i < c.NReduce; i++ {
		c.ReduceTasksStatus[i] = Idle
	}
	c.Phase = ReducePhase
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Phase == Finished
}

func (c *Coordinator) checkTaskStatus(taskId int) {
	time.Sleep(AssumeWorkerDeadTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Phase {
	case MapPhase:
		status := c.MapTasksStatus[taskId]
		if status.TaskStatus != Done {
			status.TaskStatus = Idle
			c.MapTasksStatus[taskId] = status
		}
	case ReducePhase:

		if c.ReduceTasksStatus[taskId] != Done {
			c.ReduceTasksStatus[taskId] = Idle
		}
	case ShufflePhase:
	case Finished:
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:           nReduce,
		Phase:             MapPhase,
		MapTasksStatus:    make(map[int]MapTaskStatus),
		ReduceTasksStatus: make(map[int]TaskStatus),
	}

	for idx, filename := range files {
		c.MapTasksStatus[idx] = MapTaskStatus{TaskStatus: Idle, Filename: filename}
	}

	c.server()

	return &c
}
