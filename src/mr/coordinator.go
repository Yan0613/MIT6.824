package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// 输入文件列表
	Files []string

	// Reduce 任务数量
	NReduce int

	// Map 任务的状态和进度
	MapTasks []TaskInfo

	// Reduce 任务的状态和进度
	ReduceTasks []TaskInfo

	// 协调器的状态
	Status CoordinatorStatus
}

// TaskInfo 表示一个任务的详细信息
type TaskInfo struct {
	TaskID    int
	FileName  string
	Status    TaskStatus
	StartTime time.Time
	// 可以添加其他任务信息字段
}

// CoordinatorStatus 表示协调器的状态
type CoordinatorStatus int

const (
	CoordinatorInitializing CoordinatorStatus = iota//coordinatorini, coordinatorrunning, coordinatorcompleted = 0,1,2 iota用来初始化一系列的整数常量
	CoordinatorRunning
	CoordinatorCompleted
)

// TaskStatus 表示任务的状态
type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskInProgress
	TaskCompleted
	TaskFailed
)


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
// func (c *Coordinator) server() {
// 	rpc.Register(c)
// 	rpc.HandleHTTP()
// 	//l, e := net.Listen("tcp", ":1234")
// 	sockname := coordinatorSock()
// 	os.Remove(sockname)
// 	l, e := net.Listen("unix", sockname)
// 	if e != nil {
// 		log.Fatal("listen error:", e)
// 	}
// 	go http.Serve(l, nil)
// }

func (c *Coordinator) server() {
	// 注册 RPC 方法
	rpc.Register(c)
	rpc.HandleHTTP()

	// 使用 Unix 套接字创建监听器
	sockname := coordinatorSock()
	os.Remove(sockname) // 删除旧的套接字文件
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	defer l.Close()

	// 启动 HTTP 服务器处理 RPC 请求
	go http.Serve(l, nil)
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    // 首先假设任务已完成
    allMapTasksCompleted := true
    allReduceTasksCompleted := true

    // 检查 Map 任务的状态
    for _, task := range c.MapTasks {
        if task.Status != TaskCompleted {
            allMapTasksCompleted = false
            break
        }
    }

    // 检查 Reduce 任务的状态
    for _, task := range c.ReduceTasks {
        if task.Status != TaskCompleted {
            allReduceTasksCompleted = false
            break
        }
    }

    // 根据 Map 任务和 Reduce 任务的状态确定整个任务的状态
    if allMapTasksCompleted && allReduceTasksCompleted {
        c.Status = CoordinatorCompleted
    } else {
        c.Status = CoordinatorRunning
    }

    // 返回整个任务的状态
    return c.Status == CoordinatorCompleted
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// func MakeCoordinator(files []string, nReduce int) *Coordinator {
// 	c := Coordinator{}

// 	// Your code here.


// 	c.server()
// 	return &c
// }


func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化 Coordinator 对象的字段
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make([]TaskInfo, len(files))
	c.reduceTasks = make([]TaskInfo, nReduce)
	c.status = CoordinatorInitializing

	// 初始化 mapTasks 和 reduceTasks，将任务分配给初始状态
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = TaskInfo{
			TaskID:    i,
			FileName:  files[i],
			Status:    TaskPending,
			StartTime: time.Time{},
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskInfo{
			TaskID:    i,
			Status:    TaskPending,
			StartTime: time.Time{},
		}
	}

	c.server() // 启动协调器的服务器

	return &c
}
