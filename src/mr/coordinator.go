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
	ProcessedMapTasks int
	MQueue Queue[string]
	RQueue map[int][]string
	DoneQueue Queue[string]
	MTaskDonePool map[string]bool
	RTaskDonePool map[string]bool
	// RTaskStart and RTaskIds are guarded by RQueueLock
	RTaskStart bool
	RTaskIds Queue[int]
	MQueueLock sync.Mutex
	RQueueLock sync.Mutex
	DoneQueueLock sync.Mutex
	WaitTimeouts time.Duration
	IntermediateFilePrefix string
}

// Your code here -- RPC handlers for the worker to call.

//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTasks(args *TaskArgs, reply *TaskReply) error {
	// default value of task is to let worker stand by
	reply.TaskType = "hold"
	c.MQueueLock.Lock()
	defer c.MQueueLock.Unlock()
	if !c.MQueue.IsEmpty() {
		reply.TaskType = "map"
		reply.TransactionId = uuid.NewString()
		filename, _ := c.MQueue.Dequeue()
		reply.Filename = append(reply.Filename, filename)
		reply.NReduce = c.NReduce
		log.Printf("assign map task file %v to worker %v, remaining rtasks %v", filename, args.Id, len(c.MQueue.items))
		go c.waitTask(reply, args.Id)
		return nil
	} 
	c.RQueueLock.Lock()
	defer c.RQueueLock.Unlock()
	// see if we are ready to start the reduce tasks, if not, just let worker stand by
	if !c.RTaskStart {
		return nil
	}
	if !c.RTaskIds.IsEmpty() {
		reply.TaskType = "reduce"
		taskId, _ := c.RTaskIds.Dequeue()
		reply.Filename = append(reply.Filename, c.RQueue[taskId]...)
		reply.TransactionId = uuid.NewString()
		reply.NReduce = 1
		log.Printf("assign reduce task id %v to worker %v, remaining rtasks %v", taskId, args.Id, len(c.RTaskIds.items))
		go c.waitTask(reply, args.Id)
		return nil
	}
	c.DoneQueueLock.Lock()
	defer c.DoneQueueLock.Unlock()
	if len(c.DoneQueue.items) == c.NReduce {
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
	time.Sleep(c.WaitTimeouts)
	workerIdString := strconv.Itoa(workerId)
	taskDoneKey := workerIdString + "_" + reply.TransactionId
	if taskType == "map" {
		c.MQueueLock.Lock()
		defer c.MQueueLock.Unlock()
		
		if _, ok := c.MTaskDonePool[taskDoneKey]; ok {
			_, base := getDirFilename(reply.Filename[0])

			c.RQueueLock.Lock()
			defer c.RQueueLock.Unlock()
			var rfilename string
			for i := range c.NReduce {
				rfilename = c.IntermediateFilePrefix + "_" + strconv.Itoa(i) + "_" + base + "_" + workerIdString
				c.RQueue[i] = append(c.RQueue[i], rfilename)
				c.ProcessedMapTasks += 1
			}
			// will be set one time when the RQueue is full
			if !c.RTaskStart && c.ProcessedMapTasks == c.TotalSplits * c.NReduce {
				c.RTaskStart = true
				for i := range c.NReduce {
					c.RTaskIds.Enqueue(i)
				}
				// log.Printf("---->\n\n c.RQueue: %v\n--------->\n\n", c.RQueue)
			}
			if c.ProcessedMapTasks > c.TotalSplits * c.NReduce {
				panic("fucking overcooking")
			}
			delete(c.MTaskDonePool, taskDoneKey)
		} else {
			c.MTaskDonePool[taskDoneKey] = false
			c.MQueue.Enqueue(reply.Filename[0])
			log.Printf("coordinator stop waiting worker %v for map task file %v", workerId, reply.Filename[0])
		}
	} else if taskType == "reduce" {
		c.RQueueLock.Lock()
		defer c.RQueueLock.Unlock()
		if _, ok := c.RTaskDonePool[taskDoneKey]; ok {
			// rename the output file
			filenamePattern := strings.Join(strings.Split(reply.Filename[0], "_")[:3], "_")
			tempFilename := filenamePattern + "_" + strconv.Itoa(workerId)
			outputFilename := filepath.Join(".", refactOutputFileName(tempFilename))
			
			c.DoneQueueLock.Lock()
			defer c.DoneQueueLock.Unlock()
			err := os.Rename(tempFilename, outputFilename)
			if err != nil {
				log.Fatalf("Rename error for %v", tempFilename)
			}
			c.DoneQueue.Enqueue(outputFilename)
			delete(c.RTaskDonePool, taskDoneKey)
		} else {
			parts := strings.Split(reply.Filename[0], "_")
			taskId, _ := strconv.Atoi(parts[2])
			c.RTaskDonePool[taskDoneKey] = false
			c.RTaskIds.Enqueue(taskId)
			log.Printf("coordinator stop waiting worker %v for reduce task id %v", workerId, taskId)
		}
	}
}


func (c *Coordinator) SetTaskDone(args *CallBackArgs, reply *CallBackReply) error {
	taskType := args.TaskType
	workerId := strconv.Itoa(args.Id)
	taskDoneKey := workerId + "_" + args.TransactionId
	if taskType == "map" {
		c.MQueueLock.Lock()
		defer c.MQueueLock.Unlock()
		// if the key is already there, it means the coordinator gives up the worker
		
		if _, ok := c.MTaskDonePool[taskDoneKey]; !ok {
			c.MTaskDonePool[taskDoneKey] = true 
		} else {
			delete(c.MTaskDonePool, taskDoneKey)
			return fmt.Errorf("For task %v: coordinator stop waiting %v", taskType, workerId)
		}
		
	} else if taskType == "reduce" {
		c.RQueueLock.Lock()
		defer c.RQueueLock.Unlock()
		if _, ok := c.RTaskDonePool[taskDoneKey]; !ok {
			c.RTaskDonePool[taskDoneKey] = true 
		} else {
			delete(c.RTaskDonePool, taskDoneKey)
			return fmt.Errorf("For task %v, coordinator stop waiting %v", taskType, workerId)
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
	c.DoneQueueLock.Lock()
	if len(c.DoneQueue.items) == c.NReduce {
		ret = true
	}
	c.DoneQueueLock.Unlock()

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
	c.ProcessedMapTasks = 0
	c.WaitTimeouts = 10 * time.Second
	c.IntermediateFilePrefix = "mr_out"
	c.MTaskDonePool = make(map[string]bool)
	c.RTaskDonePool = make(map[string]bool)
	c.RTaskStart = false
	// no need to acquire the lock because we are not accepting request yet
	for _, filename := range files {
		c.MQueue.Enqueue(filename)
	}
	c.RQueue = make(map[int][]string)

	c.server()
	return &c
}