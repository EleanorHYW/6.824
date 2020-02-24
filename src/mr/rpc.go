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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetMapTaskArgs struct {

}

type GetMapTaskReply struct {
	Index int
	Filename string
}

type GetnReduceArgs struct {

}

type GetnReduceReply struct {
	NReduce int
}

type GetReduceTaskArgs struct {
	
}

type GetReduceTaskReply struct {
	Index int
}

type UpdateStatusArgs struct {
	Index int
}

type UpdateStatusReply struct {

}

type CheckJobStatusArgs struct {

}

type CheckJobStatusReply struct {
	IfFinished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
