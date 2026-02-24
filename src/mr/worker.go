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
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MyWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func (w *MyWorker) run() {
	for {
		reply := CallRpc()
		switch reply.Task {
		case "map":
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := w.mapf(filename, string(content))
			tmpfiles := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				tmp, err := os.CreateTemp(".", "mr-temp-*")
				if err != nil {
					log.Fatal("cannot create temp file")
				}
				tmpfiles[i] = tmp
				encoders[i] = json.NewEncoder(tmp)
			}
			for _, kv := range kva {
				bucket := ihash(kv.Key) % reply.NReduce
				encoders[bucket].Encode(&kv)
			}
			for i, file := range tmpfiles {
				file.Close()
				name := fmt.Sprintf("mr-%d-%d", reply.Taskid, i)
				os.Rename(file.Name(), name)
			}
			CallDone(&reply)
		case "reduce":
			intermediate := []KeyValue{}
			for i := 0; i < reply.Nmap; i++ {
				fileName := fmt.Sprintf("mr-%d-%d", i, reply.Taskid)

				file, err := os.Open(fileName)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d", reply.Taskid)
			ofile, _ := os.Create(oname)
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
				output := w.reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()

			// 6. 报告完成
			CallDone(&reply)
		case "wait":
			time.Sleep(time.Second)
		case "done":
			return
		default:
			time.Sleep(time.Second)
		}
	}

}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := MyWorker{
		mapf:    mapf,
		reducef: reducef,
	}

	w.run()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallDone(reply *Reply) {
	// declare an argument structure.
	doneargs := DoneArgs{}

	// fill in the argument(s).
	// declare a reply structure.
	donereply := DoneReply{}
	doneargs.Task = reply.Task
	doneargs.Taskid = reply.Taskid

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.DoneArgs", &doneargs, &donereply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call ok\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}
func CallRpc() Reply {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.Workerid = 0

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskArg", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call ok\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
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
