package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var id int

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
	currentTime := time.Now().Unix()
	id = int(currentTime)
	// Your worker implementation here.

	//一直循环请求
	for {
		askForTask := Args{
			Id:           id,
			Status:       3,
			MapResult:    "",
			ReduceResult: "",
		}
		reply := Reply{}
		RequestForTask(&askForTask, &reply)
		//如果暂时没有任务 过2s再请求
		for reply.WorkType == 0 {
			time.Sleep(2 * time.Second)
			RequestForTask(&askForTask, &reply)
		}
		//如果已经完成所有工作，就退出
		if reply.WorkType == 3 {
			return
		}
		//map任务
		if reply.WorkType == 1 {
			intermediate := mapf(reply.Filename, reply.Content)
			err := writeToFile(intermediate)
			if err != nil {
				log.Fatalf("cannot write to file: %v", err)
			}
			finishArg := Args{
				Id:           id,
				Status:       1,
				MapResult:    reply.Filename,
				ReduceResult: "",
			}
			finishReply := Reply{}
			FinishMapTask(finishArg, finishReply)
		} else if reply.WorkType == 2 {
			// reduce任务
			filename := reply.ReducePath
			intermediate, err := readFromFile(filename)
			if err != nil {
				log.Fatalf("cannot read from file: %v", err)
			}
			sort.Sort(ByKey(intermediate))

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

				index := ihash(intermediate[i].Key) % N
				path := "mr-out-" + strconv.Itoa(index)

				// 使用 OpenFile 以写入模式打开文件
				ofile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open output file: %v", err)
				}

				// 写入输出
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				ofile.Close() // 记得关闭文件

				i = j
			}
			finishArg := Args{
				Id:           id,
				Status:       2,
				MapResult:    "",
				ReduceResult: "1",
			}
			finishReply := Reply{}
			FinishReduceTask(finishArg, finishReply)
		}
	}
}

func writeToFile(kvs []KeyValue) error {
	for _, kv := range kvs {
		index := ihash(kv.Key) % N
		filename := "mr-" + strconv.Itoa(index)

		// 使用 OpenFile 以写入模式打开文件
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		// 确保写入后关闭文件
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			file.Close() // 出错时也要关闭文件
			return err
		}
		file.Close() // 写入完成后关闭文件
	}
	return nil
}

func readFromFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break // 到达文件末尾或发生错误
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = id

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RequestForTask(args *Args, reply *Reply) {
	ok := call("Coordinator.ApplyTask", args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func FinishMapTask(args Args, reply Reply) {
	ok := call("Coordinator.FinishMapTask", args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func FinishReduceTask(args Args, reply Reply) {
	ok := call("Coordinator.FinishReduceTask", args, &reply)
	if !ok {
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
