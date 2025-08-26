package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                       sync.Mutex
	Files                    []string
	IntermediateFiles        []string
	NReduce                  int
	UnCompletedMapTaskIDs    map[int]int
	MapTaskWaitInSeconds     map[int]int
	ReduceFileMap            map[int][]string
	UnCompletedReduceTaskIDs map[int]int
	ReduceTaskWaitInSeconds  map[int]int
}

const (
	TaskStatusUnassigned = iota
	TaskStatusAssigned
	TaskStatusCompleted
)

func (c *Coordinator) checkExpiredTasks() {
	currentTime := int(time.Now().Unix())
	// Loop through the map and check for expired tasks
	for taskID, deadline := range c.MapTaskWaitInSeconds {
		if currentTime-deadline > 10 {
			// Task has expired, mark as unassigned
			c.UnCompletedMapTaskIDs[taskID] = TaskStatusUnassigned

			// Remove from MapTaskWaitInSeconds
			delete(c.MapTaskWaitInSeconds, taskID)
		}
	}

	for taskID, deadline := range c.ReduceTaskWaitInSeconds {
		if currentTime-deadline > 10 {
			// Task has expired, mark as unassigned
			c.UnCompletedReduceTaskIDs[taskID] = TaskStatusUnassigned
			// Remove from MapTaskWaitInSeconds

			delete(c.ReduceTaskWaitInSeconds, taskID)
		}
	}
}

func (c *Coordinator) findTaskIdWithStatus(taskIds *map[int]int, status int) int {
	for taskID, taskStatus := range *taskIds {
		if taskStatus == status {
			return taskID
		}
	}
	return -1
}

func (c *Coordinator) hasInProgressMapTasks() bool {
	return c.findTaskIdWithStatus(&c.UnCompletedMapTaskIDs, TaskStatusAssigned) != -1
}

func (c *Coordinator) findUnassignedMapTaskID() int {
	return c.findTaskIdWithStatus(&c.UnCompletedMapTaskIDs, TaskStatusUnassigned)
}

func (c *Coordinator) findUnassignedReduceTaskID() int {
	return c.findTaskIdWithStatus(&c.UnCompletedReduceTaskIDs, TaskStatusUnassigned)

}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(arg *TaskRequestToCoordinator, reply *TaskResponseFromCordinator) error {
	//handle map task, see if all the map tasks are done
	// In your main logic:
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkExpiredTasks()
	taskID := c.findUnassignedMapTaskID()

	if taskID >= 0 {
		reply.Done = false
		reply.NReduce = c.NReduce
		reply.TaskType = "Map"
		reply.TaskId = taskID
		fileArray := []string{c.Files[taskID]}
		c.UnCompletedMapTaskIDs[taskID] = TaskStatusAssigned
		c.MapTaskWaitInSeconds[taskID] = int(time.Now().Unix())
		reply.FileLocation = fileArray

		return nil
	}

	// wait for all the map tasks to finish
	if c.hasInProgressMapTasks() {
		reply.Done = false
		reply.TaskType = "Wait" // 或者保持原有字段为空
		return nil
	}

	reduceID := c.findUnassignedReduceTaskID()

	if reduceID >= 0 {
		reply.Done = false
		reply.NReduce = c.NReduce
		reply.TaskType = "Reduce"
		reply.TaskId = reduceID
		c.UnCompletedReduceTaskIDs[reduceID] = TaskStatusAssigned
		c.ReduceTaskWaitInSeconds[reduceID] = int(time.Now().Unix())
		reply.FileLocation = c.ReduceFileMap[reduceID]
		if len(reply.FileLocation) == 0 {
			reply.FileLocation = []string{}
		}

		return nil
	}

	reply.Done = true
	return nil

}

func (c *Coordinator) ReportTask(args *TaskDoneRequestToCoordinator, reply *TaskDoneResponseFromCordinator) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "Map" {
		c.UnCompletedMapTaskIDs[args.TaskId] = TaskStatusCompleted
		delete(c.MapTaskWaitInSeconds, args.TaskId)
		for _, filename := range args.IntermediateFiles {
			parts := strings.Split(filename, "-")
			if len(parts) == 3 {
				if reduceID, err := strconv.Atoi(parts[2]); err == nil {
					c.ReduceFileMap[reduceID] = append(c.ReduceFileMap[reduceID], filename)
				}
			}
		}
	} else if args.TaskType == "Reduce" {
		c.UnCompletedReduceTaskIDs[args.TaskId] = TaskStatusCompleted
		delete(c.ReduceTaskWaitInSeconds, args.TaskId)
	}
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

	// Your code here.
	for _, status := range c.UnCompletedMapTaskIDs {
		if status != TaskStatusCompleted {
			return false
		}
	}

	for _, status := range c.UnCompletedReduceTaskIDs {
		if status != TaskStatusCompleted {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce
	c.UnCompletedMapTaskIDs = make(map[int]int) // Initialize map
	c.MapTaskWaitInSeconds = make(map[int]int)  // Initialize map
	c.ReduceFileMap = make(map[int][]string)
	c.UnCompletedReduceTaskIDs = make(map[int]int)
	c.ReduceTaskWaitInSeconds = make(map[int]int)

	// 初始化Map任务
	for i := 0; i < len(c.Files); i++ {
		c.UnCompletedMapTaskIDs[i] = TaskStatusUnassigned
	}

	// 初始化Reduce任务
	for i := 0; i < nReduce; i++ {
		c.UnCompletedReduceTaskIDs[i] = TaskStatusUnassigned
	}

	c.server()
	return &c
}
