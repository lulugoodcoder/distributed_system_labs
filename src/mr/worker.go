package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallCoordinator(mapf, reducef)
}

func writeFinalFiles(reducef func(string, []string) string, kva []KeyValue, reduceID int) {
	final := "mr-out-" + strconv.Itoa(reduceID)
	tmp, err := os.CreateTemp("", final+"-*")
	if err != nil {
		log.Printf("cannot create file %s: %v", tmp, err)
		return
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// Create a single KeyValue for this result
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
		i = j
	}

	if err := tmp.Close(); err != nil {
		log.Fatal(err)
	}

	if err := os.Rename(tmp.Name(), final); err != nil {
		log.Fatal(err)
	}
}

func writeIntermediateFiles(mapTaskID int, kva []KeyValue, nReduce int) []string {
	IntermediateFileLocation := []string{}
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < nReduce; r++ {
		final := fmt.Sprintf("mr-%d-%d", mapTaskID, r)
		tmp, err := os.CreateTemp("", final+"-*")
		if err != nil {
			return nil
		}

		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				return nil
			}
		}

		if err := tmp.Close(); err != nil {
			return nil
		}

		if err := os.Rename(tmp.Name(), final); err != nil {
			return nil
		}

		IntermediateFileLocation = append(IntermediateFileLocation, final)
	}

	return IntermediateFileLocation
}

func readInputFile(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", fileName)
	}
	file.Close()
	return content
}

func readIntermediateFile(FileLocation []string) []KeyValue {
	kva := []KeyValue{}
	for _, fileName := range FileLocation {
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func reportTaskCompletionWithRetry(args *TaskDoneRequestToCoordinator, reply *TaskDoneResponseFromCordinator) {
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ok := call("Coordinator.ReportTask", args, reply)
		if ok {
			break
		}
		if attempt < maxAttempts {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func CallCoordinator(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := TaskRequestToCoordinator{}
		reply := TaskResponseFromCordinator{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if ok {
			if reply.Done {
				break
			}
			if reply.TaskType == "Wait" {
				time.Sleep(100 * time.Millisecond)
				continue
			} else if reply.TaskType == "Map" {
				fileName := reply.FileLocation[0]
				content := readInputFile(fileName)
				kva := mapf(fileName, string(content))
				// write each bucket to mr-mapID-reduceID
				mapTaskID := reply.TaskId
				nReduce := reply.NReduce
				IntermediateFile := writeIntermediateFiles(mapTaskID, kva, nReduce)

				args := TaskDoneRequestToCoordinator{}
				args.TaskId = mapTaskID
				args.TaskType = "Map"
				args.IntermediateFiles = IntermediateFile
				reply := TaskDoneResponseFromCordinator{}
				reportTaskCompletionWithRetry(&args, &reply)

			} else if reply.TaskType == "Reduce" {
				kva := readIntermediateFile(reply.FileLocation)
				reduceId := reply.TaskId
				sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })
				writeFinalFiles(reducef, kva, reduceId)

				args := TaskDoneRequestToCoordinator{}
				args.TaskType = "Reduce"
				args.TaskId = reduceId
				reply := TaskDoneResponseFromCordinator{}
				reportTaskCompletionWithRetry(&args, &reply)
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
