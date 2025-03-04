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
)

const MAP_PHASE = 0
const REDUCE_PHASE = 1
const HANG_PHASE = 2
const TERMINATE_PHASE = 3

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	NWorker     int
	MMapWorker  map[int]bool
	ATask       []KeyValue
	NMapTask    int
	TaskPointer int
	NDone       int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *EmptyArgs, reply *IntReply) error {
	reply.I = c.NWorker
	c.NWorker++
	return nil
}

func (c *Coordinator) Assign(args *IntArgs, reply *TaskReply) error {
	id := args.I
	if c.TaskPointer == len(c.ATask) {
		reply.Phase = TERMINATE_PHASE
		return nil
	}
	if c.TaskPointer == c.NMapTask && c.NDone < c.NMapTask {
		reply.Phase = HANG_PHASE
		return nil
	}
	if c.TaskPointer < c.NMapTask {
		task := c.ATask[c.TaskPointer]
		reply.Phase = MAP_PHASE
		reply.Mkey = task.Key
		reply.Mvalue = task.Value
		reply.NReduce = c.NReduce
		c.MMapWorker[id] = true
	} else {
		reply.Phase = REDUCE_PHASE
		j := c.TaskPointer - c.NMapTask
		reply.Rindex = j
		files := []string{}
		for i := range c.MMapWorker {
			files = append(files, fmt.Sprintf("%v-%v-%v", INTERM, i, j))
		}
		reply.Rfilenames = files
	}
	c.TaskPointer++
	return nil
}

func (c *Coordinator) Signal(args *IntArgs, reply *EmptyReply) error {
	c.NDone++
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
	return c.NDone == len(c.ATask)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NReduce = nReduce
	c.NWorker = 0
	c.MMapWorker = map[int]bool{}
	c.ATask = nil
	c.TaskPointer = 0
	c.NDone = 0

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
		c.ATask = append(c.ATask, KeyValue{filename, content})
	}
	c.NMapTask = len(c.ATask)

	// prepare reduce tasks
	for i := range nReduce {
		c.ATask = append(c.ATask, KeyValue{strconv.Itoa(i), strconv.Itoa(i)})
	}

	fmt.Fprintf(os.Stdout, "Number of map tasks: %v, number of total tasks: %v\n", c.NMapTask, len(c.ATask))

	c.server()
	return &c
}
