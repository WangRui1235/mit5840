package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type DoneArgs struct {
	Task   string
	Taskid int
}

type DoneReply struct {
}

// Add your RPC definitions here.
/*
 worker -> coordinator :
*/
type Args struct {
	//无状态，怎么定义一个ID用于调试？
	Workerid int
}

/*
coordinator -> worker
*/
type Reply struct {
	Task     string // Map,Reduce
	Filename string
	Nmap     int
	NReduce  int
	Taskid   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
