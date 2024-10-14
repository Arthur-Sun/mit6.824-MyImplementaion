package mr

import (
	"io/ioutil"
	"log"
	//"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	nameContentMap map[string]string
	fileIndex      map[int]string //mapNum-filename	从map任务编号到文件名的映射
	mapStatus      map[int]int    //mapIndex-status    status 0:等待map 1:正在进行map 2:map完
	workers        map[int]int    //worker's id-status status 0:空闲 1:正在map 2:正在reduce 3:关机 4:故障
	reduceStatus   map[int]int    //reduceIndex-reduce status 0:未reduce 1:正在reduce 2:已reduce完
	N              int
	M              int
}

var isInitialized = false

//var nameContentMap = map[string]string{}
//var status = map[string]int{}       //filename-status	status 0:等待map 1:正在进行map 2:map完
//var workers = map[int]int{}         //worker's id-status status 0:空闲 1:正在map 2:正在reduce 3:关机 4:故障
//var fileWorker = map[int]string{}   //worker-filename
//var reduceStatus = map[string]int{} //filename-reduce status 0:未reduce 1:正在reduce 2:已reduce完

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishMapTask(args Args, reply *Reply) error {
	c.mu.Lock()
	//filename := c.fileWorker[args.Id]
	c.mapStatus[args.MapNum] = 2
	//fmt.Println(args.Id, args.MapNum, c.mapStatus)
	c.workers[args.Id] = 0
	c.mu.Unlock()
	//fmt.Printf("%v, finished map task %v\n", args.Id, args.MapResult)
	return nil
}

func (c *Coordinator) FinishReduceTask(args Args, reply *Reply) error {
	c.mu.Lock()
	//filename := c.fileWorker[args.Id]
	c.reduceStatus[args.ReduceNum] = 2
	c.workers[args.Id] = 0
	c.mu.Unlock()
	//fmt.Printf("finished reduce task %v\n", args.ReduceNum)
	return nil
}

func (c *Coordinator) ApplyTask(args Args, reply *Reply) error {
	//尚未初始化
	if !isInitialized {
		reply.WorkType = 0
		return nil
	}
	//请求工作
	c.mu.Lock()
	if args.Status == 3 {
		for mapNum, state := range c.mapStatus {
			//1. 有可map的
			if state == 0 {
				filename := c.fileIndex[mapNum]
				reply.WorkType = 1
				reply.Filename = filename
				reply.Content = c.nameContentMap[filename]
				reply.MapNum = mapNum
				reply.M = c.M
				reply.N = c.N
				c.mapStatus[mapNum] = 1
				c.workers[args.Id] = 1
				c.mu.Unlock()
				//10秒故障检测
				go func() {
					time.Sleep(time.Duration(15) * time.Second) // wait 10 seconds
					c.mu.Lock()
					if c.mapStatus[mapNum] == 1 {
						//fmt.Println("就你，", filename)
						c.mapStatus[mapNum] = 0
						c.workers[args.Id] = 4
					}
					c.mu.Unlock()
				}()
				return nil
			}
		}
		//2. 无可map的
		for _, state := range c.mapStatus {
			//2.2 map未完成
			if state == 1 {
				reply.WorkType = 0
				c.workers[args.Id] = 0
				c.mu.Unlock()
				return nil
			}
		}
		//2.1 map已完成
		for reduceNum, state := range c.reduceStatus {
			//2.1.1 有可reduce的
			if state == 0 {
				reply.WorkType = 2
				reply.ReduceNum = reduceNum
				reply.M = c.M
				reply.N = c.N
				c.reduceStatus[reduceNum] = 1
				c.workers[args.Id] = 2
				c.mu.Unlock()
				//10秒故障检测
				go func() {
					time.Sleep(time.Duration(15) * time.Second) // wait 10 seconds
					c.mu.Lock()
					if c.reduceStatus[reduceNum] == 1 {
						c.reduceStatus[reduceNum] = 0
						c.workers[args.Id] = 4
					}
					c.mu.Unlock()
				}()
				return nil
			}
		}
		//2.1.2 无可reduce的
		if c.TaskDone() {
			reply.WorkType = 3
			c.workers[args.Id] = 3
		} else {
			reply.WorkType = 0
			c.workers[args.Id] = 0
		}
		c.mu.Unlock()
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
		//fmt.Println(c.status)
		//fmt.Println(c.reduceStatus)
		return false
	}
	//fmt.Println(c.workers)
	for _, state := range c.workers {
		if state == 1 || state == 2 {
			//fmt.Println("worker state: ", state)
			return false
		}
	}
	//fmt.Println("done")
	return true
}

func (c *Coordinator) TaskDone() bool {
	if !isInitialized {
		return false
	}
	for _, c := range c.reduceStatus {
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
	c.workers = make(map[int]int)
	c.mapStatus = make(map[int]int)
	c.reduceStatus = make(map[int]int)
	c.nameContentMap = make(map[string]string)
	c.fileIndex = make(map[int]string)
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
		c.nameContentMap[filename] = string(content)
		c.mapStatus[count] = 0
		c.fileIndex[count] = filename
		count++
		file.Close()
		//filename为文件名 content为文件内容
	}
	c.N = nReduce
	c.M = count
	for i := 0; i < c.N; i++ {
		c.reduceStatus[i] = 0
	}
	//for i := 0; i < N; i++ {
	//	_, err1 := os.Create("mr-" + strconv.Itoa(i))
	//	if err1 != nil {
	//		fmt.Println("Cannot create file", err1)
	//	}
	//	_, err2 := os.Create("mr-out-" + strconv.Itoa(i))
	//	if err2 != nil {
	//		fmt.Println("Cannot create file", err2)
	//	}
	//}
	//time.Sleep(time.Second)
	isInitialized = true

	c.server()
	return &c
}
