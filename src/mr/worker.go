package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	coordSockName = sockname
	for {
		t, err := CallGetTask()
		if err != nil {
			return
		}

		switch t.TaskType {
		case Map:
			file, err := os.Open(t.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", t.FileName)
			}

			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.FileName)
			}

			file.Close()
			kva := mapf(t.FileName, string(content))

			files := make(map[int]*os.File)
			for i := 0; i < t.NReduce; i++ {
				oname := fmt.Sprintf("mr-%d-%d", t.TaskNum, i)
				f, _ := os.Create(oname)
				files[i] = f
			}

			enc := make(map[int]*json.Encoder)
			for i, f := range files {
				enc[i] = json.NewEncoder(f)
			}

			for _, kv := range kva {
				y := ihash(kv.Key) % t.NReduce
				enc[y].Encode(kv)
			}

			for _, f := range files {
				f.Close()
			}

		case Reduce:
			pattern := fmt.Sprintf("mr-*-%d", t.TaskNum)

			files, err := filepath.Glob(pattern)
			if err != nil || files == nil {
				log.Fatalf("no files match pattern %v", pattern)
			}

			intermediate := []KeyValue{}

			for _, fileName := range files {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break // EOF
					}
					intermediate = append(intermediate, kv)
				}

				file.Close()
			}

			sort.Sort(ByKey(intermediate))

			outFile, err := os.Create(fmt.Sprintf("mr-out-%d", t.TaskNum))
			if err != nil {
				log.Fatal(err)
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			outFile.Close()

		case Wait:
			time.Sleep(2 * time.Second)
			continue
		}

		CallTaskDone(t.TaskNum, t.TaskType)
	}
}

// the RPC argument and reply types are defined in rpc.go.
func CallGetTask() (*Task, error) {

	// declare an argument structure.
	args := GetTaskArgs{}

	// declare a reply structure.
	reply := &Task{}

	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		return reply, nil
	} else {
		return reply, errors.New("GetTask call failed")
	}
}

func CallTaskDone(taskNum int, taskType int) {
	args := &TaskDoneArgs{TaskNum: taskNum, TaskType: taskType}

	reply := &TaskDoneReply{}

	ok := call("Coordinator.TaskDone", args, reply)
	if ok {
		return
	} else {
		log.Print("DoneTask call failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args any, reply any) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
