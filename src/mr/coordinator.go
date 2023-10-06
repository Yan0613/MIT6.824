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
	MapTask chan Task//很显然，这个地方是需要worker来取任务，因此此处必须保证线程的安全，go里面没有自己的队列实现，所以用channel
	MapTaskNum int
	ReduceTaskNum int
	ReduceTask chan Task
	Files []string
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
	if len(c.MapTaskFin)!= c.MapTaskNum {
		maptask, ok := <-c.MapTask
		if ok{
			reply.TaskAddr = &maptask
			reply.MapTaskNum  = c.MapTaskNum
			reply.ReduceTaskNum = c.ReduceTaskNum
			reply.State = c.State
		} 
	}else {
		reducetask, ok := <-c.ReduceTask
		if ok{
			reply.TaskAddr = &reducetask
			reply.MapTaskNum  = c.MapTaskNum
			reply.ReduceTaskNum = c.ReduceTaskNum
			reply.State = c.State
		}
	}

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

	if len(c.ReduceTaskFin )== c.NumReduceTask{
		ret = true
	}


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
		MapTask: make(chan Task,len(files)),
		ReduceTask: make(chan Task,len(files)*nReduce),
		Files: files,
		MapTaskNum:0,
		ReduceTaskNum:0,
		NumReduceTask:nReduce,
		MapTaskFin: make(chan bool,len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
	}
	if c.State == 0{
//make map task
		for id, file := range (files) {
			// fmt.Println("%v", file)
			maptask:=Task{
				Filename: file,
				TaskType: 0,
				TaskId:id,
				ReduceNum: nReduce, //reduce的数量
			}

			c.MapTask <- maptask
			c.MapTaskNum++
			fmt.Println("sucessefully make a map task!")
		}
	// }else if c.State == 1{
		
	// 	//make reduce tasks
	// 	for id, file := range (files){
	// 		reducetask:=Task{
	// 			Filename: file,
	// 			TaskType: 1,
	// 			TaskId: id,
	// 			ReduceNum: nReduce, //reduce的数量
	// 		}

	// 		c.ReduceTask <- reducetask
	// 		fmt.Println("sucessefully make a reduce task!")
	// 	}
	}

	c.server()
	return &c
}




func (c *Coordinator)MarkDoneTask(args *TaskArgs, reply *TaskReply) error{
	if len(c.MapTaskFin)!= c.MapTaskNum {
		c.MapTaskFin <- true
	}else if len(c.MapTaskFin)== c.MapTaskNum && len(c.ReduceTaskFin) != c.NumReduceTask{
		c.State =  1
		nReduce := c.NumReduceTask
		files := c.Files	
	//make reduce tasks
		for id, file := range (files){
			reducetask:=Task{
				Filename: file,
				TaskType: 1,
				TaskId: id,
				ReduceNum: nReduce, //reduce的数量
			}

			c.ReduceTask <- reducetask
			fmt.Println("sucessefully make a reduce task!")
		}
	}else if len(c.ReduceTaskFin) == c.NumReduceTask{
			c.State = 2
		}
	
	return nil
}
