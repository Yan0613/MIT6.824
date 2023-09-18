package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Coordinator struct {
	// Your definitions here.
	State int //map stage or reduce stage? 0 start ,1 map , 2 reduce
	NumMapWorkers int
	NumReducerWorkers int
	MapTask chan Task//很显然，这个地方是需要worker来取任务，因此此处必须保证线程的安全，go里面没有自己的队列实现，所以用channel
	ReduceTask chan Task
	Files []string
	NumMapTask int
	NumReduceTask int
	MapTaskFin	chan bool
	ReduceTaskFin chan bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	if c.State == 0{
		maptask, ok := <-c.MapTask
		if ok{
		reply.TaskAddr = &maptask
		}
	}
	// else if c.State == 1{
	// 	//all map workers finishied
	// }
	return nil
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
	c := Coordinator{
		State:0,
		NumMapWorkers:3,
		NumReducerWorkers:nReduce,
		MapTask: make(chan Task,len(files)),
		ReduceTask: make(chan Task,nReduce),
		Files: files,
		NumMapTask:0,
		NumReduceTask:0,
		MapTaskFin: make(chan bool,len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
	}
//make map task
	for _, file := range (files) {
		// fmt.Println("%v", file)
		task:=Task{
			Filename: file,
			TaskType: 0,
			// TaskId int
			ReduceNum: nReduce, //reduce的数量
			State :0,//0 start, 1 running ,2 finish, 3 waitting
		}

		c.MapTask <- task
		fmt.Println("sucessefully make a map task!")
	}

	// Your code here.


	c.server()
	return &c
}
