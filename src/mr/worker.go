package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// Request a task from the coordinator.
		task := GetTask()

		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, &task)
		case ReduceTask:
			DoReduceTask(reducef, &task)
		case ExitTask:
			fmt.Println("Worker exiting...")
			return
		}
	}
}

//
// Task function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// GetTask requests a task from the coordinator.
func GetTask() Task {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		log.Fatal("Failed to get a task from the coordinator.")
	}
	return reply
}
// DoMapTask performs the Map task.
func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// Implement Map task logic here.
	intermediate := []mr.KeyValue{}

	filename := task.Filename

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
	callDone()
}

// DoReduceTask performs the Reduce task.
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	// Implement Reduce task logic here.
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	// After processing, call callDone() to notify the coordinator of task completion.
	callDone()
}

// callDone notifies the coordinator of task completion.
func callDone() {
	args := Task{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if !ok {
		log.Fatal("Failed to notify the coordinator of task completion.")
	}
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