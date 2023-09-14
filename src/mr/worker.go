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


func DoMapTask(mapf,&Task) {

	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

}