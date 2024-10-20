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

const WorkerAssumeDeadTimeout = 10 * time.Second

type Coordinator struct {
	MapTasksStatus    map[string]MapTaskStatus
	ReduceTasksStatus map[int]TaskStatus
	Phase             Phase
	NReduce           int
	mu                sync.Mutex
}

type MapTaskStatus struct {
	TaskStatus TaskStatus
	TaskId     int
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
		filename := ""

		c.mu.Lock()
		defer c.mu.Unlock()

		for k, v := range c.MapTasksStatus {
			if v.TaskStatus == Idle {
				filename = k
				break
			}
		}

		if len(filename) == 0 {
			reply.TaskType = Sleep
			return nil
		}
		reply.TaskType = Map
		reply.TaskId = c.MapTasksStatus[filename].TaskId
		reply.Filename = filename
		reply.NReduce = c.NReduce
		taskStatus := c.MapTasksStatus[filename]
		taskStatus.TaskStatus = InProgress
		c.MapTasksStatus[filename] = taskStatus

		return nil

	case ShufflePhase:
		reply.TaskType = Sleep
	case ReducePhase:
		filename := ""
		var jobId int

		c.mu.Lock()
		defer c.mu.Unlock()

		for k, v := range c.ReduceTasksStatus {
			if v == Idle {
				jobId = k
				filename = fmt.Sprintf("mr-%v.txt", jobId)
				break
			}
		}
		if len(filename) == 0 {
			reply.TaskType = Sleep
			return nil
		}
		reply.TaskType = Reduce
		reply.TaskId = jobId
		reply.Filename = filename
		c.ReduceTasksStatus[jobId] = InProgress
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
			taskStatus := c.MapTasksStatus[args.Filename]
			taskStatus.TaskStatus = Done
			c.MapTasksStatus[args.Filename] = taskStatus
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
			taskStatus := c.MapTasksStatus[args.Filename]
			taskStatus.TaskStatus = Idle
			c.MapTasksStatus[args.Filename] = taskStatus
		}
	case ShufflePhase:
	case ReducePhase:
		if args.TaskType != Reduce {
			return nil
		}

		if args.WorkStatus == Completed {
			c.ReduceTasksStatus[args.JobId] = Done

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
			c.ReduceTasksStatus[args.JobId] = Idle
		}
	case Finished:
	}
	return nil
}

func (c *Coordinator) Shuffle() error {
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

	return nil
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

func checkWorkerStatus(c *Coordinator) {
	inProgressTasks := make(map[int]time.Time)
	for !c.Done() {
		c.mu.Lock()

		switch c.Phase {
		case MapPhase:
			for filename, status := range c.MapTasksStatus {
				startTime, ok := inProgressTasks[status.TaskId]
				if status.TaskStatus == InProgress {
					if !ok {
						inProgressTasks[status.TaskId] = time.Now()
					} else if time.Since(startTime) > WorkerAssumeDeadTimeout {
						taskStatus := c.MapTasksStatus[filename]
						taskStatus.TaskStatus = Idle
						c.MapTasksStatus[filename] = taskStatus
						delete(inProgressTasks, status.TaskId)
					}
				}
			}
		case ShufflePhase:
		case ReducePhase:
			for taskId, status := range c.ReduceTasksStatus {
				startTime, ok := inProgressTasks[taskId]
				if status == InProgress {
					if !ok {
						inProgressTasks[taskId] = time.Now()
					} else if time.Since(startTime) > WorkerAssumeDeadTimeout {
						c.ReduceTasksStatus[taskId] = Idle
						delete(inProgressTasks, taskId)
					}
				}
			}
		case Finished:
			c.mu.Unlock()
			return
		}

		c.mu.Unlock()
		time.Sleep(5 * time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:           nReduce,
		Phase:             MapPhase,
		MapTasksStatus:    make(map[string]MapTaskStatus),
		ReduceTasksStatus: make(map[int]TaskStatus),
	}

	for idx, filename := range files {
		c.MapTasksStatus[filename] = MapTaskStatus{TaskStatus: Idle, TaskId: idx}
	}

	c.server()

	go checkWorkerStatus(&c)
	return &c
}
