package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Intermediate struct {
	Key string
	Value []string
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
	
	// initiate a workerId
	workerId := os.Getpid()

	for {
		reply, ok := CallForTasks(workerId)
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		if reply.TaskType == "exit" {
			break
		} else if reply.TaskType == "hold" {
			time.Sleep(time.Second)
		} else if reply.TaskType == "map" {
			runMapTask(mapf, reply, workerId)
			CallBack(workerId, reply)
		} else if reply.TaskType == "reduce" {
			runReduceTask(reducef, reply, workerId)
			CallBack(workerId, reply)
		}
	}


}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallForTasks(workerId int) (*TaskReply, bool) {

	// declare an argument structure.
	args := TaskArgs{}

	// fill in the argument(s).
	args.Id = workerId

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTasks", &args, &reply)
	if ok {
		DPrintf("[workerId: %v] call for tasks successful with taskType (%v) nReduce (%v) filename (%v)\n", workerId, reply.TaskType, reply.NReduce, reply.Filename)
	} else {
		DPrintf("[workerId: %v] call for tasks failed with taskType (%v) nReduce (%v) filename (%v)\n", workerId, reply.TaskType, reply.NReduce, reply.Filename)
	}
	return &reply, ok	
}


func CallBack(workerId int, taskReply *TaskReply) bool {
	taskType := taskReply.TaskType
	args := CallBackArgs{}
	args.Id = workerId
	args.TaskType = taskType
	args.TransactionId = taskReply.TransactionId

	reply := CallBackReply{}

	ok := call("Coordinator.SetTaskDone", &args, &reply)
	if ok {
		DPrintf("[workerId: %v] call back successful with taskType %v\n", workerId, taskType)
	} else {
		DPrintf("[workerId: %v] call back failed with taskType %v, doing necessary file cleaning now!\n", workerId, taskType)
		if taskType == "map" {
			filename := taskReply.Filename[0]
			_, filenameBase := getDirFilename(filename)
			prefix := "mr_out"
			workerIdString := strconv.Itoa(workerId)
			for i := range taskReply.NReduce {
				outputFilename := prefix + "_" + strconv.Itoa(i) + "_" + filenameBase + "_" + workerIdString
				_, err := os.Stat(outputFilename)
				if err != nil {
					if !os.IsNotExist(err) {
						log.Fatal(err)
					} 
					continue
				}
				os.Remove(outputFilename)
			}
		} else if taskType == "reduce" {
			filenamePattern := strings.Join(strings.Split(taskReply.Filename[0], "_")[:3], "_")
			writeTempFile := filenamePattern + "_" + strconv.Itoa(workerId)
			_, err := os.Stat(writeTempFile)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Fatal(err)
				} 
			} else {
				os.Remove(writeTempFile)
			}
		}
	}
	return ok
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

	// log.Println(err)
	DPrintf("%v\n", err)
	return false
}


func runMapTask(mapf func(string, string) []KeyValue, taskDef *TaskReply, workerId int) {
	filename := taskDef.Filename[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// run mapf
	intermediate := mapf(filename, string(content))
	sort.Sort(ByKey(intermediate))

	// create output encoders
	outputEncoders := make([]*json.Encoder, 0)
	_, filenameBase := getDirFilename(filename)
	prefix := "mr_out"
	workerIdString := strconv.Itoa(workerId)
	
	for i := range taskDef.NReduce {
		outputFilename := prefix + "_" + strconv.Itoa(i) + "_" + filenameBase + "_" + workerIdString
		
		writeFile, err := os.Create(outputFilename)
		if err != nil {
			log.Fatalf("cannot create %v", outputFilename)
		}
		defer writeFile.Close()
		encoder := json.NewEncoder(writeFile)
		outputEncoders = append(outputEncoders, encoder)
	}

	//
	// write intermediate outputs
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
		key := intermediate[i].Key
		encoderIdx := ihash(key) % taskDef.NReduce
		outputEncoders[encoderIdx].Encode(Intermediate{Key: key, Value: values})
		i = j
	}
}


func runReduceTask(reducef func(string, []string) string,  taskDef *TaskReply, workerId int) {
	
	// read partisions and group keys together
	kva := make(map[string][]string)
	for _, filename := range taskDef.Filename {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv Intermediate
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := kva[kv.Key]; !ok {
				kva[kv.Key] = []string{}
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value...)
		}
		file.Close()
	}
	if len(kva) == 0 {
		DPrintf("[workerId %v]: zero mapping content for %v", workerId, taskDef.Filename)
	}
	
	// filename example: mr_out_0_saddhajskhda
	filenamePattern := strings.Join(strings.Split(taskDef.Filename[0], "_")[:3], "_")
	writeTempFile := filenamePattern + "_" + strconv.Itoa(workerId)
	ofile, err := os.Create(writeTempFile)
	if err != nil {
		log.Fatal("cannot create temp write file")
	}
	defer ofile.Close()
	// call reduce function
	for k, v := range kva {
		res := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, res)
	}
}