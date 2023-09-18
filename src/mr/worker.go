package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"encoding/json"
	"strconv"
)



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

	for{
		// Your worker implementation here.
		task:= CallTask().TaskAddr

		// uncomment to send the Task RPC to the coordinator.
		DoMapTask(mapf,task)// TASK is the address of task
	}


}

//
// Task function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallTask() TaskReply{

	// declare an argument structure.
	args := TaskArgs{}

	// fill in the argument(s).
	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Task" tells the
	// receiving server that we'd like to call
	// the Task() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("HERE IS CALL TASK,THE FILE NAME IS : %v\n", reply.TaskAddr.Filename)
	} else {
		fmt.Printf("call failed!\n")
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


func DoMapTask(mapf func(string, string) []KeyValue, task *Task){
	intermediate := []KeyValue{}
	filename:= task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))
	// NOW we got pairs of kv, we need to store and write them in temp files
	reduceNum  := task.ReduceNum
	HashKv := make([][]KeyValue, reduceNum)
	for _, kv := range(intermediate) {
		index := ihash(kv.Key) % reduceNum
		HashKv[index] = append(HashKv[index], kv)	// 将该kv键值对放入对应的下标
	}
	// 放入中间文件
	for i := 0; i < reduceNum; i++ {
		filename := "mr-tmp" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file)	// 创建一个新的JSON编码器
		error := enc.Encode(HashKv[i])
		if error != nil {
			log.Fatal("encode failed:", err)
		}
		out_filename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file.Close()
		os.Rename(new_file.Name(), out_filename)
	}
}