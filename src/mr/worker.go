package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

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

	// Your worker implementation here.
	for {
		task := Task{}
		arg := ExampleArgs{}
		call("Coordinator.HandOutTask", &arg, &task)

		if task.State == Map {
			mapFunc(&task, mapf)
		} else if task.State == Reduce {
			reduceFunc(&task, reducef)
		} else if task.State == Wait {
			time.Sleep(5 * time.Second)
		} else if task.State == Exit {
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapFunc(task *Task, mapf func(string, string) []KeyValue) {
	log.Default().Printf(strconv.Itoa(task.TaskNums))

	file, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Default().Printf(err.Error())
		log.Default().Printf("Task No." + strconv.Itoa(task.TaskNums) + " mapFunc cannot read " + task.Input)
	}

	intermediates := mapf(task.Input, string(file))

	intermediateArray := make([][]KeyValue, task.NReduce)

	for _, intermediate := range intermediates {
		hashRemainder := ihash(intermediate.Key) % task.NReduce
		intermediateArray[hashRemainder] = append(intermediateArray[hashRemainder], intermediate)
	}

	intermediateFileNames := make([]string, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		intermediateFileNames[i] = "mr-" + strconv.Itoa(task.TaskNums) + "-" + strconv.Itoa(i)
		intermediateFile, _ := os.Create("mr-X-Y/" + intermediateFileNames[i])
		enc := json.NewEncoder(intermediateFile)

		for _, kv := range intermediateArray[i] {
			enocodeErr := enc.Encode(&kv)
			if enocodeErr != nil {
				log.Default().Printf(enocodeErr.Error())
				log.Default().Printf("Task No." + strconv.Itoa(task.TaskNums) + " mapFunc cannot encode nReduce" + strconv.Itoa(i))
			}
		}
		intermediateFile.Close()
	}
	task.Intermediate = intermediateFileNames
	arg := ExampleArgs{}
	log.Default().Printf("Coordinator.FinishTask")
	call("Coordinator.FinishTask", &arg, &task)

}

func reduceFunc(task *Task, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, intermediateFileName := range task.Intermediate {
		ifile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf(string(rune(task.TaskNums))+" reduceFunc cannot open %v", intermediateFileName)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		ifile.Close()
	}

	sort.Sort(ByKey(kva))

	dir, _ := os.Getwd()
	tmpFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf(string(rune(task.TaskNums))+"cannot create tmp file", err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		outPut := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, outPut)
		i = j
	}
	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNums)
	os.Rename(tmpFile.Name(), oname)
	task.Output = oname

	arg := ExampleArgs{}
	call("Coordinator.FinishTask", &arg, &task)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
