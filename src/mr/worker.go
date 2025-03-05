package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
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

const INTERM = "intermediate-out"

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := EmptyArgs{}
		reply := TaskReply{}
		call("Coordinator.Assign", &args, &reply)
		id := reply.TaskID
		phase := reply.Phase
		if phase == MAP {
			key := reply.Mkey
			value := reply.Mvalue
			nReduce := reply.NReduce

			kva := mapf(key, value)

			// create M x R intermidiate output files
			files := []*os.File{}
			for i := range nReduce {
				file, err := os.Create(fmt.Sprintf("%v-%v-%v", INTERM, id, i))
				if err != nil {
					log.Fatalf("cannot create %v-%v-%v", INTERM, id, i)
				}
				files = append(files, file)
				defer file.Close()
			}

			for _, kv := range kva {
				i := ihash(kv.Key) % nReduce
				fmt.Fprintf(files[i], "%v %v\n", kv.Key, kv.Value)
			}

		} else if phase == REDUCE {
			filenames := reply.Rfilenames
			oidx := id
			intermediate := []KeyValue{}
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				defer file.Close()

				sc := bufio.NewScanner(file)
				for sc.Scan() {
					line := sc.Text()
					parts := strings.Fields(line)
					intermediate = append(intermediate, KeyValue{parts[0], parts[1]})
				}
			}

			sort.Sort(ByKey(intermediate))

			ofile, err := os.Create(fmt.Sprintf("mr-out-%v", oidx))
			if err != nil {
				log.Fatalf("cannot create mr-out-%v", oidx)
			}
			defer ofile.Close()

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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

		} else if phase == HANG {
			time.Sleep(time.Second)
			continue
		} else {
			break
		}

		nargs := SignalArgs{}
		nargs.Phase = phase
		nargs.TaskID = id
		nreply := EmptyReply{}
		call("Coordinator.Signal", &nargs, &nreply)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
