package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type ProcessState int

const (
	Map ProcessState = iota
	Reduce
	Exit
	Wait
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	Input        string
	State        ProcessState
	NReduce      int
	TaskNums     int
	Intermediate []string
	Output       string
}

type TaskIdentity struct {
	CoordinatorTaskState TaskState
	StartStamp           time.Time
	TaskReference        *Task
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue     chan *Task
	TaskMap       map[int]*TaskIdentity
	State         ProcessState
	NReduce       int
	InputFiles    []string
	Intermediates [][]string
	Lock          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandOutTask(args *ExampleArgs, reply *Task) error {
	c.Lock.Lock()
	if c.State == Map || c.State == Reduce {
		if len(c.TaskQueue) > 0 {
			*reply = *<-c.TaskQueue
			c.TaskMap[reply.TaskNums].StartStamp = time.Now()
			c.TaskMap[reply.TaskNums].CoordinatorTaskState = InProgress
			log.Default().Printf("c.state"+strconv.Itoa(int(c.State)))
			log.Default().Printf("handout"+strconv.Itoa(reply.TaskNums))
		} else {
			reply = &Task{State: Wait}
		}
	} else if c.State == Exit {
		reply = &Task{State: Exit}
	}
	c.Lock.Unlock()
	return nil
}

func (c *Coordinator) FinishTask(args *ExampleArgs, reply *Task) error {
	c.Lock.Lock()

	if reply.State != c.State || c.TaskMap[reply.TaskNums].CoordinatorTaskState == Completed {

	} else {
		c.TaskMap[reply.TaskNums].CoordinatorTaskState = Completed
		log.Default().Printf(strconv.Itoa(int(reply.TaskNums)))
		log.Default().Printf(strconv.Itoa(int(reply.State)))
		log.Default().Printf("Completed")
		go c.postProcess(reply)
	}
	c.Lock.Unlock()
	return nil
}

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
	c.Lock.Lock()
	if c.State == Exit {
		ret = true
	}
	c.Lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.

	var nTask int
	if len(files) > nReduce {
		nTask = len(files)
	} else {
		nTask = nReduce
	}

	c := Coordinator{
		TaskQueue:     make(chan *Task, nTask),
		TaskMap:       make(map[int]*TaskIdentity),
		State:         Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
		Lock:          sync.Mutex{},
	}

	c.createMapTasks()

	c.server()

	go c.catchTimeOut()

	return &c
}

func (c *Coordinator) createMapTasks() {
	for idx, fileName := range c.InputFiles {
		task := Task{
			Input:    fileName,
			State:    Map,
			NReduce:  c.NReduce,
			TaskNums: idx,
		}
		taskIdentity := TaskIdentity{
			CoordinatorTaskState: Idle,
			StartStamp:           time.Now(),
			TaskReference:        &task,
		}
		c.TaskQueue <- &task
		c.TaskMap[idx] = &taskIdentity
	}
}

func (c *Coordinator) createReduceTasks() {
	c.TaskMap = make(map[int]*TaskIdentity)
	for nReduceNo, intermediateFileNames := range c.Intermediates {
		task := Task{
			// Input:    nil,
			State:        Reduce,
			NReduce:      c.NReduce,
			TaskNums:     nReduceNo,
			Intermediate: intermediateFileNames,
		}
		taskIdentity := TaskIdentity{
			CoordinatorTaskState: Idle,
			StartStamp:           time.Now(),
			TaskReference:        &task,
		}
		c.TaskQueue <- &task
		c.TaskMap[nReduceNo] = &taskIdentity
	}
}

func (c *Coordinator) postProcess(task *Task) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if task.State == Map {
		for nReduceNo, intermediateFileName := range task.Intermediate {
			c.Intermediates[nReduceNo] = append(c.Intermediates[nReduceNo], intermediateFileName)
		}
		for _, v := range c.Intermediates {
			for _, kv := range v {
				log.Printf(kv)
			}
		}

		if c.checkAllTask() {
			c.createReduceTasks()
			c.State = Reduce
		}
	} else if task.State == Reduce {
		if c.checkAllTask() {
			c.State = Exit
		}
	}
}

func (c *Coordinator) checkAllTask() bool {
	for _, taskIdentity := range c.TaskMap {
		if taskIdentity.CoordinatorTaskState != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		c.Lock.Lock()
		if c.State == Exit {
			c.Lock.Unlock()
			return
		}
		for _, taskIdentity := range c.TaskMap {
			if taskIdentity.CoordinatorTaskState == InProgress && time.Since(taskIdentity.StartStamp) > 10*time.Second {
				c.TaskQueue <- taskIdentity.TaskReference
				taskIdentity.CoordinatorTaskState = Idle
			}
		}
		c.Lock.Unlock()
	}
}
