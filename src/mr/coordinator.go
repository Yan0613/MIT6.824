package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"



var mu sync.Mutex

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
	MapTaskCrashCheck chan TaskMeta
	ReduceTaskCrashCheck chan TaskMeta
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	var mu sync.Mutex
	mu.Lock()
	if len(c.MapTaskFin)!= c.MapTaskNum{
		maptask, ok := <-c.MapTask
		if ok{
			reply.Task = maptask
		} 
	}else if len(c.ReduceTaskFin)!=c.NumReduceTask{
		reducetask, ok := <-c.ReduceTask
		if ok{
			reply.Task = reducetask
		}
	}
	//注意，这里三个值的传递要放在外面，因为在最后一个reducetask取出来之后管道是空的，不会执行ok里面的代码，状态得不到更新。
	//
	reply.MapTaskNum  = c.MapTaskNum
	reply.ReduceTaskNum = c.NumReduceTask
	reply.State = c.State
	mu.Unlock()
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
		State:0,  //0 map, 1 reduce, 2 finish
		MapTask: make(chan Task,len(files)),
		ReduceTask: make(chan Task,nReduce),
		AllReduceTaask: nReduce,
		Files: files,
		MapTaskNum:len(files),
		NumReduceTask:nReduce,
		MapTaskFin: make(chan bool,len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
		MapTaskCrashCheck: make(chan TaskMeta,len(files)),
		ReduceTaskCrashCheck: make(chan TaskMeta,nReduce),
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

		taskMeta := TaskMeta{
			Task:    maptask, // 将 task 赋值给 TaskMeta 结构的 Task 字段
			TaskFin: false, // 设置 TaskFin 字段的值
			TaskStartTime: time.Now().Unix(),
		}
		c.MapTaskCrashCheck <- taskMeta

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

		taskMeta := TaskMeta{
			Task:    reducetask, // 将 task 赋值给 TaskMeta 结构的 Task 字段
			TaskFin: false, // 设置 TaskFin 字段的值
			TaskStartTime: time.Now().Unix(),
		}
		c.ReduceTaskCrashCheck <- taskMeta
	}

	fmt.Println("sucessefully make all reduce tasks!")

	c.server()
//check crash test
	// go c.CrashHandler()

	return &c
}

func (c *Coordinator)MarkDoneTask(args *Task, reply *TaskReply) error{

	mu.Lock()
	if len(c.MapTaskFin)!= c.MapTaskNum {
		c.MapTaskFin <- true
		// for mapmeta := range c.MapTaskCrashCheck {
		// 	if mapmeta.Task.TaskId == args.TaskId {
		// 		mapmeta.TaskFin = true
		// 		break
		// 	}
		// }
		fmt.Println("map task done num", len(c.MapTaskFin))
		if len(c.MapTaskFin)== c.MapTaskNum {
			c.State = 1
		}
	}else if len(c.ReduceTaskFin)!= c.NumReduceTask	{
		c.ReduceTaskFin <- true
		// for mapmeta := range c.ReduceTaskCrashCheck {
		// 	if mapmeta.Task.TaskId == args.TaskId {
		// 		mapmeta.TaskFin = true
		// 		break
		// 	}
		// }
		fmt.Println("reduce task done num", len(c.ReduceTaskFin))
		if len(c.ReduceTaskFin)== c.NumReduceTask {
			fmt.Println("all reduce tasks are done!")
			c.State = 2
		}
	}else{
		c.State = 2
	}
	mu.Unlock()
	return nil
}


// func(c* Coordinator) TimeTick(){
// 	state := c.State
// 	time_now := time.Now().Unix()
// 	if state == 0 {
// 		for metadata := range c.MapTaskCrashCheck {
// 			if time_now - metadata.TaskStartTime > 10 && !metadata.TaskFin{
// 				fmt.Println("map task crash check timeout")
// 				//重发maptask
// 				c.MapTask <- metadata.Task
// 				metadata.TaskStartTime = time_now
// 				metadata.TaskFin = false
// 			}
// 		}
// 	}else if state == 1 {
// 		for metadata := range c.ReduceTaskCrashCheck {
// 			if time_now - metadata.TaskStartTime > 10 && !metadata.TaskFin{
// 				fmt.Println("reduce task crash check timeout")
// 				//重发reducetask
// 				c.ReduceTask <- metadata.Task
// 				metadata.TaskStartTime = time_now
// 				metadata.TaskFin = false
// 			}
// 		}
// 	}
// }