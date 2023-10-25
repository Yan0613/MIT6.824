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
	"sort"
	"time"
)


// for sorting by key.
type ByKey []KeyValue//set up a type named ByKey, which type is a slice, and the element is KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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
		mu.Lock()
		// Your worker implementation here.
		reply:= CallTask()
		if reply.State == 0{
			// uncomment to send the Task RPC to the coordinator.
			DoMapTask(mapf,reply)// TASK is the address of task
			TaskDone()
		}else if reply.State == 1{
			DoReduceTask(reducef,reply)
			TaskDone()
			if reply.State == 2{
				break
			}
		}else{
			break
		}
		time.Sleep(1000 * time.Millisecond)
		mu.Unlock()
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
		// fmt.Println("Call sucessfully :", reply.Task.TaskType)
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


func DoMapTask(mapf func(string, string) []KeyValue, reply TaskReply){
	var intermediate = []KeyValue{}
	task:=reply.Task
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
		filename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file)	// 创建一个新的JSON编码器
		for _, kv := range(HashKv[i]) {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
}

func TaskDone(){

	args := TaskArgs{}

	// fill in the argument(s).
	// declare a reply structure.
	reply := TaskReply{}

	ok := call("Coordinator.MarkDoneTask", &args, &reply)
	if ok {
		fmt.Println("task done!")
	} else {
		fmt.Println("call failed!")
	}
}

func DoReduceTask(reducef func(string, []string) string, reply TaskReply){
	task := reply.Task
	num_map := reply.MapTaskNum
	intermediate := []KeyValue{}
	id := task.TaskId
	for i:=0; i<num_map; i++{
		// fmt.Println(">>>>>>>>>>>>>>i", i)
		map_filename := "mr-" + strconv.Itoa(i)+ "-" + strconv.Itoa(id)
		// fmt.Println("map task id:", i, "map filename:", map_filename)
		inputfile,err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
		if err != nil{
			log.Fatalf("OPEN MAP TEMP FILE '%v FAILED!", map_filename)
		}
		dec := json.NewDecoder(inputfile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			// fmt.Println("err:", err)
			if err!=nil{
				break
			}

			intermediate = append(intermediate, kv)
			// fmt.Println("intermediate:", intermediate)
		}
	}
	// fmt.Println("intermediate:", intermediate)
	sort.Sort(ByKey(intermediate))
	out_file := "mr-out-" + strconv.Itoa(id)
	tmp_file, err := ioutil.TempFile("", "mr-reduce-*")
	if err != nil {
		log.Fatalf("cannot open temp file")
	}	
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp_file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tmp_file.Close()
	os.Rename(tmp_file.Name(), out_file)
}