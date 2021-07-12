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
	Index       int
	Filename    string
	Types       TaskType
	Completed   bool // 是否完成
	Distributed bool // 是否已分配
}

type Coordinator struct {
	mu       sync.Mutex
	finished bool

	mapTasks    []*Task
	reduceTasks []*Task

	mapCount    int
	reduceCount int
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
		if task != nil {
			go c.waitTaskDone(task)
		}
	} else { // 分配 reduce task
		task = c.getTask(Reduce)
		resp.Task = task
		if task != nil {
			go c.waitTaskDone(task)
		}
	}
	return nil
}

func (c *Coordinator) SubmitTask(req *SubmitTaskRequest, resp *SubmitTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//log.Println("接收到任务完成提交", req.Task)
	index := req.Task.Index
	types := req.Task.Types
	var task *Task

	switch types {
	case Map:
		task = c.mapTasks[index]
		if task.Distributed == true {
			task.Completed = true
			//log.Printf("map task 已完成, %+v\n", req.Task)
			c.mapCount++
		}
	case Reduce:
		task = c.reduceTasks[index]
		if task.Distributed == true {
			task.Completed = true
			//log.Printf("reduce task 已完成, %+v\n", req.Task)
			c.reduceCount++
			if c.reduceCount >= len(c.reduceTasks) {
				c.finished = true // 总的任务完成
			}
		}
	}

	return nil
}

// server start a thread that listens for RPCs from worker.go
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
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
			if task.Distributed == false {
				task.Distributed = true
				return task
			}
		}
	} else {
		for i := 0; i < len(c.reduceTasks); i++ {
			task := c.reduceTasks[i]
			if task.Distributed == false {
				task.Distributed = true
				return task
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
	// 如果未完成，则重新设置为未分配
	switch task.Types {
	case Map:
		if !c.mapTasks[task.Index].Completed {
			c.mapTasks[task.Index].Distributed = false
		}
	case Reduce:
		if !c.reduceTasks[task.Index].Completed {
			c.reduceTasks[task.Index].Distributed = false
		}
	}

	return
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:          sync.Mutex{},
		finished:    false,
		mapCount:    0,
		reduceCount: 0,
	}

	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, &Task{
			Index:       i,
			Filename:    files[i],
			Types:       Map,
			Completed:   false,
			Distributed: false,
		})
	}

	for r := 0; r < nReduce; r++ {
		c.reduceTasks = append(c.reduceTasks, &Task{
			Index:       r,
			Types:       Reduce,
			Completed:   false,
			Distributed: false,
		})
	}

	c.server()
	return &c
}
