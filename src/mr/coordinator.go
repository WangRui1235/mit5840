package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	maptask    []int    //存status 0 Idle 1 InProgress 2 finished
	reducetask []int    //存status
	filename   []string //用于计算nmap
	nReduce    int      //分桶
	maptime    []time.Time
	reducetime []time.Time
	mu         sync.Mutex
}

const (
	idle int = iota
	inprogress
	completed
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) DoneArgs(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Task {
	case "map":
		c.maptask[args.Taskid] = completed
	case "reduce":
		c.reducetask[args.Taskid] = completed
	}

	return nil
}

func (c *Coordinator) AskArg(arg *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Nmap = len(c.filename)
	reply.NReduce = c.nReduce

	for i, status := range c.maptask {
		if status == idle {
			reply.Task = "map"
			reply.Taskid = i
			c.maptask[i] = inprogress
			c.maptime[i] = time.Now()
			reply.Filename = c.filename[i]
			return nil
		}
	}
	//Map完成了，也就是说maptask里所有的status == 2,这时候要进行Reduce
	if slices.Min(c.maptask) == completed {
		for i, status := range c.reducetask {
			if status == idle {
				reply.Task = "reduce"
				reply.Taskid = i
				c.reducetask[i] = inprogress
				c.reducetime[i] = time.Now()
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}
	//不要调用c.Done()!!!会导致重复加锁问题
	if slices.Min(c.reducetask) == completed {
		reply.Task = "done"
		return nil
	}
	reply.Task = "wait"
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
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.
	if slices.Min(c.reducetask) == completed {
		ret = true
	}

	return ret
}

func (c *Coordinator) timeoutchecker() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		for i, t := range c.maptime {
			if time.Since(t) > 10*time.Second && c.maptask[i] == inprogress {
				//c.maptime[i] = time.Now()
				c.maptask[i] = idle
			}
		}
		for i, t := range c.reducetime {
			if time.Since(t) > 10*time.Second && c.reducetask[i] == inprogress {
				//c.reducetime[i] = time.Now()
				c.reducetask[i] = idle
			}
		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.filename = files
	c.nReduce = nReduce
	c.maptask = make([]int, len(files))
	c.reducetask = make([]int, nReduce)
	c.maptime = make([]time.Time, len(files))
	c.reducetime = make([]time.Time, nReduce)

	c.server()
	go c.timeoutchecker()
	return &c
}
