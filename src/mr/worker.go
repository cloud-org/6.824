package mr

import (
	"log"
	"time"
)

type worker struct {
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		req := AskTaskRequest{}
		resp := AskTaskResponse{}
		success := call("Coordinator.AskTask", &req, &resp)
		if !success {
			log.Println("任务结束")
			return
		}
		task := resp.Task
		if task == nil { // 没有分配到任务 等等
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if task.types == Map {
			DoMap(task)
			TaskDone(task)
		}
	}
}

func TaskDone(task *Task) {
	req := SubmitTaskRequest{}
	resp := SubmitTaskResponse{}
	success := call("Coordinator.SubmitTask", &req, &resp)
	if !success {
		return
	}
}

func DoMap(task *Task) {

}
