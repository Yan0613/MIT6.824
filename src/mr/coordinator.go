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
	AllReduceTaask int
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
	// mu.Lock()
	// defer mu.Unlock()
	if c.State == 0{
		maptask, ok := <-c.MapTask
		if ok{
			reply.TaskAddr = &maptask
			reply.MapTaskNum  = c.MapTaskNum
			reply.ReduceTaskNum = c.NumReduceTask
			reply.State = c.State
		} 
	}else if c.State == 1{
		reducetask, ok := <-c.ReduceTask
		if ok{
			reply.TaskAddr = &reducetask
			reply.MapTaskNum  = c.MapTaskNum
			reply.ReduceTaskNum = c.NumReduceTask
			reply.State = c.State
		}
	}else if c.State == 2{
		
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

	if len(c.ReduceTaskFin)== c.AllReduceTaask{
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
		ReduceTask: make(chan Task,nReduce),
		AllReduceTaask: nReduce,
		Files: files,
		MapTaskNum:len(files),
		NumReduceTask:nReduce,
		MapTaskFin: make(chan bool,len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
	}

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
	}
	fmt.Println("sucessefully make all map tasks!")
	
//make reduce tasks

	for i:=0;i<nReduce;i++{
		reducetask:=Task{
			TaskType: 1,
			TaskId: i,
			ReduceNum: nReduce, //reduce的数量
		}
		c.ReduceTask <- reducetask
	}

	fmt.Println("sucessefully make all reduce tasks!")
	c.server()
	return &c
}




func (c *Coordinator)MarkDoneTask(args *TaskArgs, reply *TaskReply) error{
	if len(c.MapTaskFin)!= c.MapTaskNum {
		c.MapTaskFin <- true
		if len(c.MapTaskFin) == c.MapTaskNum{
			c.State = 1
			fmt.Println("all map tasks are done, Start reduce stage")
		}
	}else if len(c.MapTaskFin) == c.MapTaskNum&&len(c.ReduceTaskFin) != c.AllReduceTaask{
		c.ReduceTaskFin <- true
		if len(c.ReduceTaskFin) == c.AllReduceTaask{
			c.State = 2
			fmt.Println("all reduce tasks are done!")
		}
	}
	
	return nil
}
