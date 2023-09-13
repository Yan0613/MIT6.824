//前置知识
//RPC（Remote Procedure Call）是一种计算机通信协议，用于实现分布式系统中的远程通信。
// 它允许一个计算机程序调用另一个地址空间（通常是在不同的机器上运行）的过程或函数，
// 就像调用本地函数一样，而不需要显式地处理底层网络通信细节。
// RPC的主要目标是简化分布式应用程序的编程，
// 使远程过程调用看起来就像本地过程调用一样。

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
// Task worker向coordinator获取task的结构体
// Task worker向coordinator获取task的结构体
type Task struct {
	TaskType   TaskType // 任务类型判断到底是map还是reduce
	TaskId     int      // 任务的id
	ReducerNum int      // 传入的reducer的数量，用于hash
	Filename   string   // 输入文件
}

// TaskArgs rpc应该传入的参数，可实际上应该什么都不用传,因为只是worker获取一个任务
type TaskArgs struct{}

// TaskType 对于下方枚举任务的父类型
type TaskType int

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type State int

// 枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waittingen任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

//coordinatorSock() 函数：这个函数用于生成一个唯一的 UNIX 域套接字名称，用于协调器（coordinator）。
// UNIX 域套接字是一种在同一台机器上的不同进程之间进行本地通信的机制。
// 这个函数通过在文件路径中添加用户ID（通过 os.Getuid() 获取）来生成唯一的套接字名称。
// 这个函数的返回值是一个字符串，表示套接字的路径

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("coordinator.AssignTask",&args,&reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("get task failed!")
	}

	return reply
}
