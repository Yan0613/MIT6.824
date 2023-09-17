package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

mu := sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task     // 使用chan保证并发安全
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组																																		
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)




// Your code here -- RPC handlers for the worker to call.																				

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
			// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()
	switch c.Phase{
	case MapPhase:
		reply := c.TaskChannelMap
	case ReducePhase:
		reply := c.TaskChannelReduce
		Exit()
	}
}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			Filename:   v,
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
}

func MakeReduceTasks(c *Coordinator){

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
//Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTasks(files)

	c.server()
	return &c

	// c := Coordinator{}

	// // Your code here.


	// c.server()
	// return &c
}


