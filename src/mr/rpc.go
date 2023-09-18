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
type Task struct{
	Filename string
	TaskType int
	// TaskId int
	ReduceNum int //reduce的数量
	State int //0 start, 1 running ,2 2finish
}

type TaskType int 

type TaskArgs struct {

}

type TaskReply struct {
	TaskAddr  *Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}