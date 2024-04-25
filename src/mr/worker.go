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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var expiredTime float64 = 5
	// uncomment to send the Example RPC to the coordinator.
	for {
		args := TaskRequest{}
		args.X = 1
		reply := TaskResponse{}

		ok := call("Coordinator.TaskRequester", &args, &reply)

		if ok {
			if time.Now().Sub(reply.MyTask.StartTime).Seconds() > expiredTime {
				continue
			}
			filename := (reply.MyTask).FileName
			if filename != "" {
				mapId := reply.MyTask.MapId
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				num_reduce := reply.ReduceTaskNum
				bucket := make([][]KeyValue, num_reduce)
				for _, kv := range kva {
					reduceId := ihash(kv.Key) % num_reduce
					bucket[reduceId] = append(bucket[reduceId], kv)
				}
				for i := 0; i < num_reduce; i++ {
					tmp_file, error := ioutil.TempFile("", "mr-map-*")
					if error != nil {
						log.Fatal("cannot open tmp_file")
					}
					enc := json.NewEncoder(tmp_file)
					err := enc.Encode(bucket[i])
					if err != nil {
						log.Fatalf("encode bucket error ")
					}
					out_file := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i)
					os.Rename(tmp_file.Name(), out_file)
				}
				argsTmp := TaskFinRequest{}
				argsTmp.Id = mapId

				replyTmp := TaskResponse{}

				call("Coordinator.MapTaskFin", &argsTmp, &replyTmp)

			} else if reply.MyTask.ReduceId != -1 {
				intermediate := []KeyValue{}
				reducedId := reply.MyTask.ReduceId
				map_num := reply.MapTaskNum
				for i := 0; i < map_num; i++ {
					oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reducedId)
					ofile, err := os.OpenFile(oname, os.O_RDONLY, 0777)
					if err != nil {
						log.Fatal("cannot open reduceTask %v", oname)
					}
					dec := json.NewDecoder(ofile)
					for {
						var kv []KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv...)
					}
				}
				sort.Sort(ByKey(intermediate))
				out_filename := "mr-out-" + strconv.Itoa(reply.MyTask.ReduceId)
				tmp_filename, err := ioutil.TempFile("", "mr_reduce-*")
				if err != nil {
					log.Fatalf("cannot open tmp_file")
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
					// this is  correct format for each line of Reduce output.
					fmt.Fprintf(tmp_filename, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				tmp_filename.Close()
				os.Rename(tmp_filename.Name(), out_filename)

				argsTmp := TaskFinRequest{}
				argsTmp.Id = reducedId

				replyTmp := TaskResponse{}
				call("Coordinator.ReduceTaskFin", &argsTmp, &replyTmp)

			} else {
				break
			}
			time.Sleep(time.Second * 2)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
