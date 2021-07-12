package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"
)

const middledir = "/tmp/mr"
const middletmpdir = "/tmp/mrtmp"

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	myworker := worker{mapf: mapf, reducef: reducef}
	for {
		req := AskTaskRequest{}
		resp := AskTaskResponse{}
		res := call("Coordinator.AskTask", &req, &resp)
		if !res {
			log.Println("done, worker quit...")
			return
		}
		task := resp.Task
		if task == nil {
			// Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// 注：这里不能开 goroutine
		// 因为 mrapps 中 mtiming/rtiming 会测试相应的并行 task remove {type}-{pid} 文件 并行 remove 会 panic
		if task.Types == Map {
			err := myworker.DoMap(task)
			if err != nil {
				log.Println("doMap err,", err)
			} else {
				TaskDone(task)
			}
		} else {
			err := myworker.DoReduce(task)
			if err != nil {
				log.Println("doReduce err,", err)
			} else {
				TaskDone(task)
			}
		}
	}
}

func (w *worker) DoMap(task *Task) error {
	// 参考 mr 串行 demo
	intermediate := []KeyValue{}
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Printf("cannot open %v", task.Filename)
		return err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v\n", task.Filename)
		return err
	}
	// 对每一个 file 调用 map 方法
	kva := w.mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	if err = w.saveIntermediate(task, intermediate); err != nil {
		return err
	}

	return nil
}

func (w *worker) DoReduce(task *Task) error {
	intermediate, err := w.loadIntermediate(task)
	if err != nil {
		return err
	}

	file, err := ioutil.TempFile(middletmpdir, fmt.Sprintf("reduce_tmp_%d", task.Index))
	if err != nil {
		return err
	}
	defer file.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			// 如果 key 相同则一直加
			j++
		}
		// 开始统计同一个 key 出现的次数 此时的 j 其实是加了一 不能算上 k < j
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 输出到文件
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j // i 设置偏移
	}

	if err = os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", task.Index)); err != nil {
		return err
	}

	return nil
}

func (w *worker) saveIntermediate(task *Task, kvs []KeyValue) error {
	// You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.

	res := make(map[int][]KeyValue)
	// init
	for i := 0; i < 10; i++ {
		res[i] = make([]KeyValue, 0)
	}
	// DONE: 这里的 key 已经进行了排序 所以可以不用算么多次 ihash
	//for i := 0; i < len(kvs); i++ {
	//	kv := kvs[i]
	//	reduceIndex := ihash(kv.Key) % 10 // reduce task length
	//	// mr-mapIndex-reduceIndex
	//	res[reduceIndex] = append(res[reduceIndex], kv)
	//}
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		reduceIndex := ihash(kvs[i].Key) % 10
		res[reduceIndex] = append(res[reduceIndex], kvs[i:j]...)
		i = j
	}

	for index, value := range res {
		file, err := ioutil.TempFile(middletmpdir, fmt.Sprintf("map_tmp_%d_%d_", task.Index, index))
		if err != nil {
			return err
		}
		//log.Println("middle 文件名", file.Name())
		enc := json.NewEncoder(file)
		for _, kv := range value {
			err = enc.Encode(&kv)
			if err != nil {
				log.Println("encode err", err)
				return err
			}
		}
		file.Close()
		if err = os.Rename(file.Name(), fmt.Sprintf("%s/mr-%d-%d", middledir, task.Index, index)); err != nil {
			return err
		}
	}

	return nil
}

//loadIntermediate 加载 reduce 对应的所有 map task 中间结果
func (w *worker) loadIntermediate(task *Task) ([]KeyValue, error) {
	fileInfos, err := ioutil.ReadDir(middledir)
	if err != nil {
		log.Println("readdir err:", err)
		return nil, err
	}
	// 读取对应 reduce task 文件并添加到内存中
	var kvs []KeyValue
	for i := 0; i < len(fileInfos); i++ {
		fileInfo := fileInfos[i]
		var mapIndex int
		var reduceIndex int
		matching, err := fmt.Sscanf(fileInfo.Name(), "mr-%d-%d", &mapIndex, &reduceIndex)
		if reduceIndex == task.Index && matching == 2 && err == nil {
			file, err := os.Open(fmt.Sprintf("%s/%s", middledir, fileInfo.Name()))
			if err != nil {
				log.Printf("文件无法打开 %v\n", fileInfo.Name())
				return nil, err
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break // 表示已经无法再读取了
				}
				kvs = append(kvs, kv)
			}
			file.Close()
		} else if err != nil {
			log.Println("sscanf err:", err)
			continue
		}
	}

	sort.Sort(ByKey(kvs))

	return kvs, nil
}

func TaskDone(task *Task) {
	req := SubmitTaskRequest{Task: task}
	resp := SubmitTaskResponse{}
	success := call("Coordinator.SubmitTask", &req, &resp)
	if !success {
		return
	}
}
