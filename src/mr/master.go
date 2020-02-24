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
			reply.Index = i
			reply.Filename = m.fileNames[i]
			m.mapJobStatus[i] = running
			go mapJobWatcher(m, i)
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
			reply.Index = i
			m.reduceJobStatus[i] = running
			go reduceJobWatcher(m, i)
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) GetnReduce(args *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.NReduce = len(m.reduceJobStatus)
	return nil
}

func (m *Master) CheckMapJobStatus(args *CheckJobStatusArgs, reply *CheckJobStatusReply) error {
	m.mutex.Lock()
	flag := true
	for _, v := range m.mapJobStatus {
		if v != finished {
			flag = false
			break
		}
	}
	m.mutex.Unlock()
	reply.IfFinished = flag
	return nil
}

func (m *Master) CheckReduceJobStatus(args *CheckJobStatusArgs, reply *CheckJobStatusReply) error {
	m.mutex.Lock()
	flag := true
	for _, v := range m.reduceJobStatus {
		if v != finished {
			flag = false
			break
		}
	}
	m.mutex.Unlock()
	reply.IfFinished = flag
	return nil
}

func (m *Master) UpdateMapTaskStatus(args *UpdateStatusArgs, reply *UpdateStatusReply) error {
	m.mutex.Lock()
	m.mapJobStatus[args.Index] = finished
	m.mutex.Unlock()
	return nil
}

func (m *Master) UpdateReduceTaskStatus(args *UpdateStatusArgs, reply *UpdateStatusReply) error {
	m.mutex.Lock()
	m.reduceJobStatus[args.Index] = finished
	m.mutex.Unlock()
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
	//ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, v := range m.reduceJobStatus {
		if v != finished {
			return false
		}
	}
	fmt.Println("finished!")
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		fileNum:          len(files),
		fileNames:        files,
		mapJobStatus:     make([]int, len(files)),
		reduceJobStatus:  make([]int, nReduce),
	}

	m.server()
	return &m
}
