package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
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
	rand.Seed(time.Now().UnixNano())
	// 生成一个[0, 100)之间的随机整数
	randomNumber := rand.Intn(100000000)
	id = int(randomNumber)
	// Your worker implementation here.

	//一直循环请求
	for {
		askForTask := Args{
			Id:        id,
			Status:    3,
			MapNum:    0,
			ReduceNum: 0,
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
			buckets := make([][]KeyValue, reply.N)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%reply.N] = append(buckets[ihash(kva.Key)%reply.N], kva)
			}

			// write into intermediate files
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(reply.MapNum) + "-" + strconv.Itoa(i)
				ofile, _ := ioutil.TempFile("", oname+"*")
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets[i] {
					err := enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			finishArg := Args{
				Id:        id,
				Status:    1,
				MapNum:    reply.MapNum,
				ReduceNum: 0,
			}
			finishReply := Reply{}
			FinishMapTask(finishArg, finishReply)
		} else if reply.WorkType == 2 {
			// reduce任务
			intermediate := []KeyValue{}
			for i := 0; i < reply.M; i++ {
				fn := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceNum)
				temp, err := readFromFile(fn)
				if err != nil {
					log.Fatalf("cannot read from %v", fn)
				}
				intermediate = append(intermediate, temp...)
			}
			sort.Sort(ByKey(intermediate))
			// output file
			oname := "mr-out-" + strconv.Itoa(reply.ReduceNum)
			ofile, _ := ioutil.TempFile("", oname+"*")

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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()

			finishArg := Args{
				Id:        id,
				Status:    2,
				MapNum:    0,
				ReduceNum: reply.ReduceNum,
			}
			finishReply := Reply{}
			FinishReduceTask(finishArg, finishReply)
		}
	}
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
