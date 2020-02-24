package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	unallocated = iota
	running
	finished
)

const jobTimeOut = 10 * time.Second

type Master struct {
	// Your definitions here.
	fileNum int
	fileNames []string
	mapJobStatus []int
	reduceJobStatus []int

	mutex sync.Mutex
}

func mapJobWatcher(m *Master, index int) {
	time.Sleep(jobTimeOut)
	m.mutex.Lock()
	if m.mapJobStatus[index] == running {
		m.mapJobStatus[index] = unallocated
	}
	m.mutex.Unlock()
}

func reduceJobWatcher(m *Master, index int) {
	time.Sleep(jobTimeOut)
	m.mutex.Lock()
	if m.reduceJobStatus[index] == running {
		m.reduceJobStatus[index] = unallocated
	}
	m.mutex.Unlock()
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	m.mutex.Lock()
	for i, v := range m.mapJobStatus {
		if v == unallocated {
			reply.index = i
			reply.filename = m.fileNames[i]
			m.mapJobStatus[i] = running
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	m.mutex.Lock()
	for i, v := range m.reduceJobStatus {
		if v == unallocated {
			reply.index = i
			m.reduceJobStatus[i] = running
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) GetnReduce(args *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.nReduce = len(m.reduceJobStatus)
	return nil
}

func (m *Master) UpdateMapTaskStatus(args *UpdateStatusArgs, reply *UpdateStatusReply) error {
	m.mutex.Lock()
	m.mapJobStatus[args.index] = finished
	flag := true
	for _, v := range m.mapJobStatus {
		if v != finished {
			flag = false
			break
		}
	}
	m.mutex.Unlock()
	reply.ifFinished = flag
	return nil
}

func (m *Master) UpdateReduceTaskStatus(args *UpdateStatusArgs, reply *UpdateStatusReply) error {
	m.mutex.Lock()
	m.reduceJobStatus[args.index] = finished
	flag := true
	for _, v := range m.reduceJobStatus {
		if v != finished {
			flag = false
			break
		}
	}
	m.mutex.Unlock()
	reply.ifFinished = flag
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		fileNum:          len(files),
		fileNames:        files,
		mapJobStatus:     make([]int, len(files)),
		reduceJobStatus:  make([]int, nReduce),
	}

	// Your code here.
	fmt.Println(m.fileNames)
	fmt.Println(m.mapJobStatus)
	fmt.Println(m.reduceJobStatus)

	m.server()
	return &m
}
