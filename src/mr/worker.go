package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		//CallExample()
		keepFlag := true
		for keepFlag {
			task := GetTask()
			switch task.TaskType {
			case MapTask:
				{
					DoMapTask(mapf, &task)
					callDone()
				}
	
			case WaittingTask:
				{
					fmt.Println("All tasks are in progress, please wait...")
					time.Sleep(time.Second)
				}
			case ExitTask:
				{
					fmt.Println("Task about :[", task.TaskId, "] is terminated...")
					keepFlag = false
				}
	
			}
		}
	
		// uncomment to send the Example RPC to the coordinator.
}


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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()

	//在你提供的代码中，c 是一个变量，它用于表示与协调器（Coordinator）的RPC连接。
	//而 c.Call 是RPC库（net/rpc）中的一个方法，用于发送RPC请求并接收RPC响应。————generate by chatgpt
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


// func DoMapTask(mapf,&Task) {

// 	intermediate := []mr.KeyValue{}
// 	for _, filename := range os.Args[2:] {
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			log.Fatalf("cannot open %v", filename)
// 		}
// 		content, err := ioutil.ReadAll(file)
// 		if err != nil {
// 			log.Fatalf("cannot read %v", filename)
// 		}
// 		file.Close()
// 		kva := mapf(filename, string(content))
// 		intermediate = append(intermediate, kva...)
// 	}

// }


// 这段代码是一个 Go 语言函数 DoMapTask，它用于执行 Map 任务的核心逻辑。
// 这个函数的主要功能是读取输入文件，将文件内容传递给指定的 mapf 函数进行处理，
// 然后将处理结果按照哈希分区的方式存储到不同的临时文件中。

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.Filename

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取conten,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map返回一组KV结构体数组
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

// callDone Call RPC to mark the task as completed
func callDone() Task {

	args := Task{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
