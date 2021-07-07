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

type TaskType string

const Map TaskType = "map"
const Reduce TaskType = "reduce"

type Task struct {
	index       int
	types       TaskType
	completed   bool // 是否完成
	distributed bool // 是否已分配
}

type Coordinator struct {
	mu       sync.Mutex
	finished bool
	files    []string

	mapTasks    []Task
	reduceTasks []Task

	mapCount    int
	reduceCount int
	stopC       struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(req *AskTaskRequest, resp *AskTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return nil
	}
	var task *Task
	if !c.finishMap() { // 分配 map task
		task = c.getTask(Map)
		resp.Task = task
		go c.waitTaskDone(task)
	} else { // 分配 reduce task
		task = c.getTask(Reduce)
		resp.Task = task
		go c.waitTaskDone(task)
	}
	return nil
}

func (c *Coordinator) SubmitTask(req *SubmitTaskRequest, resp *SubmitTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return
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
	ret := false

	c.mu.Lock()
	ret = c.finished
	c.mu.Unlock()

	return ret
}

func (c *Coordinator) finishMap() bool {
	if c.mapCount >= len(c.mapTasks) {
		return true
	}
	return false
}

func (c *Coordinator) getTask(taskType TaskType) *Task {
	if taskType == Map {
		for i := 0; i < len(c.mapTasks); i++ {
			task := c.mapTasks[i]
			if task.distributed == false {
				return &task
			}
		}
	} else {
		for i := 0; i < len(c.reduceTasks); i++ {
			task := c.reduceTasks[i]
			if task.distributed == false {
				return &task
			}
		}
	}

	return nil
}

func (c *Coordinator) waitTaskDone(task *Task) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C
	c.mu.Lock()
	defer c.mu.Unlock()
	switch task.types {
	case Map:
		if c.mapTasks[task.index].completed == true {
			c.mapCount++
		} else {
			c.mapTasks[task.index].distributed = false
		}
	case Reduce:
		if c.reduceTasks[task.index].completed == true {
			c.reduceCount++
		} else {
			c.reduceTasks[task.index].distributed = false
		}
	}

	return
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		mu:          sync.Mutex{},
		finished:    false,
		mapCount:    0,
		reduceCount: 0,
	}

	for f := 0; f < len(files); f++ {
		c.mapTasks = append(c.mapTasks, Task{
			index:       f,
			types:       Map,
			completed:   false,
			distributed: false,
		})
	}

	for r := 0; r < nReduce; r++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			index:       r,
			types:       Reduce,
			completed:   false,
			distributed: false,
		})
	}

	c.server()
	return &c
}
