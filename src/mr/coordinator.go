package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAP = iota
	REDUCE
	HANG
	TERMINATE
)

const (
	UNASSIGNED = iota
	ASSIGNED
	COMPLETED
	FAILED
)

type TaskInfo struct {
	Type      int
	Status    int
	Task      KeyValue
	Timestamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	MapTasks        []TaskInfo
	MapCompleted    bool
	ReduceTasks     []TaskInfo
	ReduceCompleted bool
	Mutex           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Assign(args *EmptyArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if !c.MapCompleted {
		cnt := 0
		for i, info := range c.MapTasks {
			st := info.Status
			ts := info.Timestamp
			if st == UNASSIGNED || st == FAILED || (st == ASSIGNED && time.Since(ts) > 10*time.Second) {
				reply.Phase = MAP
				reply.TaskID = i
				reply.Mkey = info.Task.Key
				reply.Mvalue = info.Task.Value
				reply.NReduce = len(c.ReduceTasks)
				c.MapTasks[i].Status = ASSIGNED
				c.MapTasks[i].Timestamp = time.Now()
				return nil
			} else if st == COMPLETED {
				cnt++
			}
		}
		if cnt == len(c.MapTasks) {
			c.MapCompleted = true
		} else {
			reply.Phase = HANG
			return nil
		}
	}

	if !c.ReduceCompleted {
		cnt := 0
		for i, info := range c.ReduceTasks {
			st := info.Status
			ts := info.Timestamp
			if st == UNASSIGNED || st == FAILED || (st == ASSIGNED && time.Since(ts) > 10*time.Second) {
				reply.Phase = REDUCE
				reply.TaskID = i
				files := []string{}
				for id := range len(c.MapTasks) {
					files = append(files, fmt.Sprintf("%v-%v-%v", INTERM, id, i))
				}
				reply.Rfilenames = files
				c.ReduceTasks[i].Status = ASSIGNED
				c.ReduceTasks[i].Timestamp = time.Now()
				return nil
			} else if st == COMPLETED {
				cnt++
			}
		}
		if cnt == len(c.ReduceTasks) {
			c.ReduceCompleted = true
		} else {
			reply.Phase = HANG
			return nil
		}
	}

	reply.Phase = TERMINATE
	return nil
}

func (c *Coordinator) Signal(args *SignalArgs, reply *EmptyReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	phase := args.Phase
	i := args.TaskID

	if phase == MAP {
		c.MapTasks[i].Status = COMPLETED
	} else if phase == REDUCE {
		c.ReduceTasks[i].Status = COMPLETED
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// return true
	return c.MapCompleted && c.ReduceCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.MapCompleted = false
	c.ReduceCompleted = false
	c.Mutex = sync.Mutex{}

	// prepare map tasks
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		rawContent, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read from %v", filename)
		}
		content := string(rawContent)
		// could split content into chunks
		info := TaskInfo{}
		info.Type = MAP
		info.Status = UNASSIGNED
		info.Task = KeyValue{filename, content}
		c.MapTasks = append(c.MapTasks, info)
	}

	// prepare reduce tasks
	for i := range nReduce {
		info := TaskInfo{}
		info.Type = REDUCE
		info.Status = UNASSIGNED
		info.Task = KeyValue{strconv.Itoa(i), strconv.Itoa(i)}
		c.ReduceTasks = append(c.ReduceTasks, info)
	}

	c.server()
	return &c
}
