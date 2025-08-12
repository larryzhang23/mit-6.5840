package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/google/uuid"
)


type Coordinator struct {
	// Your definitions here.
	TotalSplits int 
	NReduce int

	MQueue Queue[string]
	RQueue Queue[int]
	DoneQueue Queue[string]
	IntFileMaps *FileStore[int]
	
	WaitTimeouts time.Duration
	IntermediateFilePrefix string

	// for task done communication
	TaskDoneChan *MapChan[string]
	MapTaskDone bool
	Mu sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.

//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTasks(args *TaskArgs, reply *TaskReply) error {
	// default value of task is to let worker stand by
	reply.TaskType = "hold"
	if filename, ok := c.MQueue.Dequeue(); ok {
		reply.TaskType = "map"
		reply.TransactionId = uuid.NewString()
		reply.Filename = append(reply.Filename, filename)
		reply.NReduce = c.NReduce
		DPrintf("assign map task file %v to worker %v, remaining rtasks %v", filename, args.Id, c.MQueue.Len())
		taskDoneKey := strconv.Itoa(args.Id) + "_" + reply.TransactionId
		c.TaskDoneChan.NewChan(taskDoneKey)
		go c.waitTask(reply, args.Id)
		return nil
	} 

	// see if we are ready to start the reduce tasks, if not, just let worker stand by
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if !c.MapTaskDone {
		return nil
	}
	if taskId, ok := c.RQueue.Dequeue(); ok {
		reply.TaskType = "reduce"
		reply.Filename = append(reply.Filename, c.IntFileMaps.GetFiles(taskId)...)
		reply.TransactionId = uuid.NewString()
		reply.NReduce = 1
		DPrintf("assign reduce task id %v to worker %v, remaining rtasks %v", taskId, args.Id, c.RQueue.Len())
		taskDoneKey := strconv.Itoa(args.Id) + "_" + reply.TransactionId
		c.TaskDoneChan.NewChan(taskDoneKey)
		go c.waitTask(reply, args.Id)
		return nil
	}
	if c.DoneQueue.Len() == c.NReduce {
		reply.TaskType = "exit"
		return nil
	}
	return nil
}

func (c *Coordinator) waitTask(reply *TaskReply, workerId int) {
	taskType := reply.TaskType
	if taskType == "hold" || taskType == "exit" {
		return 
	}
	
	workerIdString := strconv.Itoa(workerId)
	taskDoneKey := workerIdString + "_" + reply.TransactionId
	ch := c.TaskDoneChan.GetChan(taskDoneKey)
	switch taskType {
	case "map":
		select {
		case <- ch:
			_, base := getDirFilename(reply.Filename[0])
			var rfilename string
			for i := range c.NReduce {
				rfilename = c.IntermediateFilePrefix + "_" + strconv.Itoa(i) + "_" + base + "_" + workerIdString
				c.IntFileMaps.AddFile(i, rfilename)
			}
			// will be set one time when the RQueue is full
			c.Mu.Lock()
			defer c.Mu.Unlock()
			if !c.MapTaskDone && c.IntFileMaps.IsAllFiles() {
				for i := range c.NReduce {
					c.RQueue.Enqueue(i)
				}
				c.MapTaskDone = true 
			}
			// 
			// assert
			//
			// if c.ProcessedMapTasks > c.TotalSplits * c.NReduce {
			// 	panic("fucking overcooking")
			// }
			c.TaskDoneChan.DeleteChan(taskDoneKey)
			
		case <- time.After(c.WaitTimeouts):
			c.MQueue.Enqueue(reply.Filename[0])
			DPrintf("coordinator stop waiting worker %v for map task file %v", workerId, reply.Filename[0])
			c.TaskDoneChan.DeleteChan(taskDoneKey)
		}
		
	case "reduce":
		select {
			case <- ch:
				// rename the output file
				filenamePattern := strings.Join(strings.Split(reply.Filename[0], "_")[:3], "_")
				tempFilename := filenamePattern + "_" + strconv.Itoa(workerId)
				outputFilename := filepath.Join(".", refactOutputFileName(tempFilename))
				err := os.Rename(tempFilename, outputFilename)
				if err != nil {
					log.Fatalf("Rename error for %v", tempFilename)
				}
				c.DoneQueue.Enqueue(outputFilename)
				c.TaskDoneChan.DeleteChan(taskDoneKey)
			case <- time.After(c.WaitTimeouts):
				parts := strings.Split(reply.Filename[0], "_")
				taskId, _ := strconv.Atoi(parts[2])
				c.RQueue.Enqueue(taskId)
				DPrintf("coordinator stop waiting worker %v for reduce task id %v", workerId, taskId)
				c.TaskDoneChan.DeleteChan(taskDoneKey)

		}
	}
}


func (c *Coordinator) SetTaskDone(args *CallBackArgs, reply *CallBackReply) error {
	taskType := args.TaskType
	workerId := strconv.Itoa(args.Id)
	taskDoneKey := workerId + "_" + args.TransactionId

	if taskType == "map" || taskType == "reduce" {
		// if no chan exists, it means the coordinator gave up the worker
		if ok := c.TaskDoneChan.SendSignal(taskDoneKey); ok {
			return nil
		} else {
			return fmt.Errorf("For task %v: coordinator stop waiting %v", taskType, workerId)
		}
	}
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

	// Your code here.
	if c.DoneQueue.Len() == c.NReduce {
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
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.TotalSplits = len(files)
	c.WaitTimeouts = 10 * time.Second
	c.IntermediateFilePrefix = "mr_out"

	c.TaskDoneChan = NewMapChan[string]()
	// no need to acquire the lock because we are not accepting request yet
	for _, filename := range files {
		c.MQueue.Enqueue(filename)
	}
	c.IntFileMaps = NewMapString[int](len(files), nReduce)

	c.server()
	return &c
}