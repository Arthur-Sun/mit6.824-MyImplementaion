package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct {
	Id        int //worker's id
	Status    int //worker's status 0工作中 1map工作完成 2reduce工作完成 3请求工作
	MapNum    int //map任务编号
	ReduceNum int //reduce任务编号
}

type Reply struct {
	WorkType  int    //0等待工作 1map任务 2reduce任务 3结束
	Filename  string //如果是map任务，对应split的文件名
	Content   string //文件内容
	ReduceNum int    //reduce任务编号
	MapNum    int    //map任务编号
	N         int    //reduce任务数量
	M         int    //map任务数量
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
