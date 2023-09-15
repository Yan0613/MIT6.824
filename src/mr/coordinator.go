package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

mu := sync.Mutex

type Coordinator struct {
	// Your definitions here.
	mapFiles     []string   // 输入文件列表
	nReduce      int        // Reduce 任务数量
	mapTasks     []Task     // Map 任务的状态和进度
	reduceTasks  []Task     // Reduce 任务的状态和进度
	mapFinished  int        // 已完成的 Map 任务数量
	reduceFinished int      // 已完成的 Reduce 任务数量																																					
}

// Your code here -- RPC handlers for the worker to call.																				

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {

	
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
