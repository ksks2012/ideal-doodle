package mr

import (
	"container/list"
	"fmt"
	"log"
	"mapreduce/util"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/google/uuid"
)

type Master struct {
	// Your definitions here.
	WorkerData map[uuid.UUID]util.WorkerData // info of worker
	JobQueue   list.List
	// working queue
}

// TODO: check worker state 10s (timeout)
// Job state: Doing -> Waiting
// Worker state: Running -> Error

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Regist(args *RegistArgs, reply *RegistReply) error {
	_, isExist := m.WorkerData[args.WorkerID]
	if !isExist {
		m.WorkerData[args.WorkerID] = util.WorkerData{
			State: util.Init,
			Job:   util.Job{},
		}
		reply.Success = true
		m.Print()
		return nil
	}
	reply.Success = false
	m.Print()
	return nil
}

func (m *Master) GetJob(args *RegistArgs, reply *GetJobReply) error {
	// Get worker info storage in master
	value, isExist := m.WorkerData[args.WorkerID]

	// None regist worker
	if !isExist {
		reply.setExitReply()
		m.Print()
		return nil
	}

	// Get job from queue
	e := m.JobQueue.Front()

	// Worker is not in initial state || No job
	// TODO: If no 'map job', return 'reduce job'
	// TODO: Number of reduce Job is base on keys
	if value.State != util.Init || e == nil {
		value.SetExit()
		reply.setExitReply()
		m.Print()
		return nil
	}

	// Assignment Job to Worker
	job := e.Value.(util.Job)
	value.SetRunning(job)
	reply.setJobReply(job)
	m.JobQueue.Remove(e)
	m.Print()
	return nil
}

func (m *Master) Print() error {
	fmt.Printf("\n")
	fmt.Printf("WorkerData:\n")
	for k, v := range m.WorkerData {
		fmt.Print(k, v)
	}

	fmt.Printf("\n")
	fmt.Printf("JobQueue:\n")
	for e := m.JobQueue.Front(); e != nil; e = e.Next() {
		j := e.Value.(util.Job)
		fmt.Print(j, "\n")
	}
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
	// l, e := net.Listen("tcp", ":1234")
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

func (m *Master) Report(args *ReportArgs, reply *ReportReply) error {
	reply.Success = false

	// Your code here.
	if args.Job.Action == util.Map {
		// Add reduce job to queue
		args.Job.Action = util.Reduce
		args.Job.State = util.Waiting
		m.JobQueue.PushBack(args.Job)
		reply.Success = true
	}
	switch args.Job.Action {
	case util.Map:
		// Add reduce job to queue
		args.Job.Action = util.Reduce
		args.Job.State = util.Waiting
		m.JobQueue.PushBack(args.Job)
		reply.Success = true
	case util.Reduce:
		// TODO:
		args.Job.Action = util.Exit
		args.Job.State = util.Done
	case util.Exit:
		return nil
	default:
		log.Printf("Error Action: %v", args.Job.Action)
	}

	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{WorkerData: make(map[uuid.UUID]util.WorkerData)}
	m.Print()
	// Your code here.
	count := 0
	for _, file := range files {
		job := util.Job{
			Action:   util.Map,
			State:    util.Waiting,
			FileName: file,
			NReduce:  nReduce,
			JobId:    count,
		}
		count++
		m.JobQueue.PushBack(job)
	}
	m.Print()
	m.server()
	return &m
}
