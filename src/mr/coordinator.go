package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

var isInitialized = false
var N = 10

var nameContentMap = map[string]string{}
var status = map[string]int{}       //filename-status	status 0:等待map 1:正在进行map 2:map完
var workers = map[int]int{}         //worker's id-status status 0:空闲 1:正在map 2:正在reduce 3:关机 4:故障
var fileWorker = map[int]string{}   //worker-filename
var reduceStatus = map[string]int{} //filename-reduce status 0:未reduce 1:正在reduce 2:已reduce完

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishMapTask(args Args, reply *Reply) error {
	filename := fileWorker[args.Id]
	fileWorker[args.Id] = ""
	status[filename] = 2
	workers[args.Id] = 0
	return nil
}

func (c *Coordinator) FinishReduceTask(args Args, reply *Reply) error {
	filename := fileWorker[args.Id]
	fileWorker[args.Id] = ""
	reduceStatus[filename] = 2
	workers[args.Id] = 0
	return nil
}

func (c *Coordinator) ApplyTask(args Args, reply *Reply) error {
	//尚未初始化
	if !isInitialized {
		reply.WorkType = 0
		return nil
	}
	//请求工作
	if args.Status == 3 {
		for filename, state := range status {
			//1. 有可map的
			if state == 0 {
				reply.WorkType = 1
				reply.Filename = filename
				reply.Content = nameContentMap[filename]
				status[filename] = 2
				workers[args.Id] = 1
				fileWorker[args.Id] = filename
				return nil
			}
		}
		//2. 无可map的
		for _, state := range status {
			//2.2 map未完成
			if state == 1 {
				reply.WorkType = 0
				workers[args.Id] = 0
				return nil
			}
		}
		//2.1 map已完成
		for filename, state := range reduceStatus {
			//2.1.1 有可reduce的
			if state == 0 {
				reply.WorkType = 2
				reply.ReducePath = filename
				reduceStatus[filename] = 1
				workers[args.Id] = 2
				fileWorker[args.Id] = filename
				return nil
			}
		}
		//2.1.2 无可reduce的
		if c.TaskDone() {
			reply.WorkType = 3
			workers[args.Id] = 3
		} else {
			reply.WorkType = 0
			workers[args.Id] = 0
		}

		return nil
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
	if !c.TaskDone() {
		return false
	}
	for _, state := range workers {
		if state != 3 {
			return false
		}
	}
	return true
}

func (c *Coordinator) TaskDone() bool {
	if !isInitialized {
		return false
	}
	for _, c := range reduceStatus {
		if c != 2 {
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
	//intermediate := []KeyValue{}
	count := 0
	//循环读取文件
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		nameContentMap[filename] = string(content)
		status[filename] = 0
		count++
		file.Close()
		//filename为文件名 content为文件内容
	}
	N = nReduce
	for i := 0; i < N; i++ {
		filename := "mr-" + strconv.Itoa(i)
		reduceStatus[filename] = 0
	}
	for i := 0; i < N; i++ {
		_, err1 := os.Create("mr-" + strconv.Itoa(i))
		if err1 != nil {
			fmt.Println("Cannot create file", err1)
		}
		_, err2 := os.Create("mr-out-" + strconv.Itoa(i))
		if err2 != nil {
			fmt.Println("Cannot create file", err2)
		}
	}
	isInitialized = true

	c.server()
	return &c
}
