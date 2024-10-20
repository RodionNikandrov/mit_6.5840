package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, ok := RequestTaskCall()
		if !ok {
			return
		}

		switch reply.TaskType {
		case Map:
			content, err := os.ReadFile(reply.Filename)
			if err != nil {
				ReportTaskCall(&CoordinatorCallArgs{WorkStatus: Failed, JobId: reply.TaskId})
			}

			intermediate := make([][]KeyValue, reply.NReduce)
			omap := mapf(reply.Filename, string(content))
			for _, kv := range omap {
				partition := ihash(kv.Key) % reply.NReduce
				intermediate[partition] = append(intermediate[partition], kv)
			}

			for idx, kvarr := range intermediate {
				oname := fmt.Sprintf("../mr-tmp/mr-%v-%v.txt", reply.TaskId, idx)
				ofile, _ := os.Create(oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range kvarr {
					err := enc.Encode(&kv)
					if err != nil {
						continue
					}
				}

				ofile.Close()
			}

			ReportTaskCall(&CoordinatorCallArgs{TaskType: Map, WorkStatus: Completed, JobId: reply.TaskId, Filename: reply.Filename})
		case Reduce:
			filename := fmt.Sprintf("../mr-tmp/%v", reply.Filename)
			file, err := os.Open(filename)
			if err != nil {
				ReportTaskCall(&CoordinatorCallArgs{WorkStatus: Failed, JobId: reply.TaskId})
			}

			var reduceIn ByKey
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				reduceIn = append(reduceIn, kv)
			}

			file.Close()

			ofilename := fmt.Sprintf("../mr-tmp/mr-out-%v.txt", reply.TaskId)
			ofile, err := os.Create(ofilename)
			if err != nil {
				ReportTaskCall(&CoordinatorCallArgs{WorkStatus: Failed, JobId: reply.TaskId})
			}

			for i := 0; i < len(reduceIn); {
				j := i + 1
				for j < len(reduceIn) && reduceIn[j].Key == reduceIn[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reduceIn[k].Value)
				}
				output := reducef(reduceIn[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", reduceIn[i].Key, output)

				i = j
			}
			ofile.Close()

			ReportTaskCall(&CoordinatorCallArgs{TaskType: Reduce, WorkStatus: Completed, JobId: reply.TaskId, Filename: reply.Filename})
		case Sleep:
			time.Sleep(5 * time.Second)
		}
	}

}

func RequestTaskCall() (*CoordinatorCallReply, bool) {

	args := CoordinatorCallArgs{}

	reply := CoordinatorCallReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	return &reply, ok
}

func ReportTaskCall(args *CoordinatorCallArgs) {
	reply := CoordinatorCallReply{}
	call("Coordinator.ReportTask", &args, &reply)
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
