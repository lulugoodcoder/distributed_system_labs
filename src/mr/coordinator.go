package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	FileName  string
	MapId     int
	ReduceId  int
	StartTime time.Time
}

type MapValues struct {
	IsFinished bool
	Task       Task
	StartTime  time.Time
}

type Coordinator struct {
	// Your definitions here.
	mapTaskChan    chan Task
	reduceTaskChan chan Task
	mapTaskNum     int
	reduceTaskNum  int
	mapTasks       sync.Map
	reduceTasks    sync.Map
	isReduce       bool
	mapFinished    bool
	reduceFinished bool
	lock           sync.Mutex
}

func (c *Coordinator) MapTaskFin(args *TaskFinRequest, reply *TaskResponse) error {
	mapValue, ok := c.mapTasks.Load(args.Id)
	if ok && time.Now().Sub(mapValue.(MapValues).StartTime).Seconds() <= 5 {
		isFinished := true
		taskValue := mapValue.(MapValues).Task
		startTime := mapValue.(MapValues).StartTime
		c.mapTasks.Store(args.Id, MapValues{isFinished, taskValue, startTime})
	}
	return nil
}

func (c *Coordinator) ReduceTaskFin(args *TaskFinRequest, reply *TaskResponse) error {
	reduceTaskValue, ok := c.reduceTasks.Load(args.Id)
	if ok && time.Now().Sub(reduceTaskValue.(MapValues).StartTime).Seconds() <= 5 {
		isFinished := true
		taskValue := reduceTaskValue.(MapValues).Task
		startTime := taskValue.StartTime
		c.reduceTasks.Store(args.Id, MapValues{isFinished, taskValue, startTime})
	}
	return nil
}

func (c *Coordinator) TaskRequester(args *TaskRequest, reply *TaskResponse) error {
	// Check if there are map tasks available
	select {
	case myTask, ok := <-c.mapTaskChan:
		if ok {
			reply.MyTask = myTask
			reply.MapTaskNum = c.mapTaskNum
			reply.ReduceTaskNum = c.reduceTaskNum
			return nil
		}
	case myTask, ok := <-c.reduceTaskChan:
		if ok {
			reply.MyTask = myTask
			reply.MapTaskNum = c.mapTaskNum
			reply.ReduceTaskNum = c.reduceTaskNum
			return nil
		}
	default:
		return nil
	}

	// If both channels are empty, return an error
	return errors.New("both mapTaskChan and reduceTaskChan are empty")
}

func (c *Coordinator) checkLoop() {
	for {
		if c.mapFinished && c.reduceFinished {
			break
		}
		c.lock.Lock()
		c.checkFinished()
		c.lock.Unlock()
		time.Sleep(10 * time.Second)
	}
}

func (c *Coordinator) checkFinished() {
	nowTime := time.Now()
	if !c.mapFinished {
		finished := true
		for i := 0; i < c.mapTaskNum; i++ {
			value, _ := c.mapTasks.Load(i)
			if !value.(MapValues).IsFinished {
				finished = false
			}
			if !value.(MapValues).IsFinished && nowTime.Sub(value.(MapValues).StartTime).Seconds() > 5 {
				mytime := time.Now()
				task := value.(MapValues).Task
				task.StartTime = mytime
				c.mapTasks.Store(i, MapValues{false, task, mytime})
				c.mapTaskChan <- task
			}
		}
		c.mapFinished = finished

		if c.mapFinished {
			for i := 0; i < c.reduceTaskNum; i++ {
				mytime := time.Now()
				task := Task{ReduceId: i, StartTime: mytime}
				c.reduceTasks.Store(i, MapValues{false, task, mytime})
				c.reduceTaskChan <- task
			}
		}
	}
	if c.mapFinished && !c.reduceFinished {
		finished := true
		for i := 0; i < c.reduceTaskNum; i++ {
			value, _ := c.reduceTasks.Load(i)
			if !value.(MapValues).IsFinished {
				finished = false
			}
			if !value.(MapValues).IsFinished && nowTime.Sub(value.(MapValues).StartTime).Seconds() > 5 {
				task := value.(MapValues).Task
				myTime := time.Now()
				task.StartTime = myTime
				c.reduceTasks.Store(i, MapValues{false, task, myTime})
				c.reduceTaskChan <- task
			}
		}
		c.reduceFinished = finished
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.reduceFinished && c.mapFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskChan:    make(chan Task, len(files)),
		reduceTaskChan: make(chan Task, nReduce),
		mapTaskNum:     len(files),
		reduceTaskNum:  nReduce,
		mapTasks:       sync.Map{},
		reduceTasks:    sync.Map{},
		reduceFinished: false,
		mapFinished:    false,
	}
	for i, file := range files {
		myTime := time.Now()
		task := Task{FileName: file, MapId: i, ReduceId: -1, StartTime: myTime}
		c.mapTaskChan <- task
		c.mapTasks.Store(i, MapValues{false, task, myTime})
	}
	c.server()
	go c.checkLoop()
	return &c
}
