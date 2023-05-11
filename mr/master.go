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
	WorkerData     map[uuid.UUID]util.WorkerData // info of worker
	MapJobQueue    list.List
	ReduceJobQueue list.List
	DoneJobQueue   list.List
	WorkerCount    int
	NReduce        int
	// working queue
	Commit bool
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
	if m.MapJobQueue.Len() != 0 {
		top := m.MapJobQueue.Front()
		if top != nil {
			// Assignment Map Job to Worker
			job := top.Value.(util.Job)
			value.SetRunning(job)
			reply.setJobReply(job)
			m.MapJobQueue.Remove(top)
			m.Print()
			return nil
		}
	}
	if m.ReduceJobQueue.Len() != 0 {
		top := m.ReduceJobQueue.Front()
		if top != nil {
			// Assignment Reduce Job to Worker
			job := top.Value.(util.Job)
			value.SetRunning(job)
			reply.setJobReply(job)
			m.ReduceJobQueue.Remove(top)
			m.Print()
			return nil
		}
	}

	if m.DoneJobQueue.Len() == (m.WorkerCount + m.NReduce) {
		reply.setExitReply()
		m.Print()
		return nil
	}

	// Worker is not in initial state || No job
	// TODO: Number of reduce Job is base on keys
	if value.State != util.Init {
		value.SetExit()
		reply.setExitReply()
		m.Print()
		return nil
	}

	return nil
}

func (m *Master) Print() error {
	fmt.Printf("\n")
	fmt.Printf("************************************************\n")
	fmt.Printf("WorkerData:\n")
	for k, v := range m.WorkerData {
		fmt.Print(k, v)
	}

	fmt.Printf("\n")
	fmt.Printf("MapJobQueue:\n")
	for e := m.MapJobQueue.Front(); e != nil; e = e.Next() {
		j := e.Value.(util.Job)
		fmt.Print(j, "\n")
	}

	fmt.Printf("\n")
	fmt.Printf("ReduceJobQueue:\n")
	for e := m.ReduceJobQueue.Front(); e != nil; e = e.Next() {
		j := e.Value.(util.Job)
		fmt.Print(j, "\n")
	}

	fmt.Printf("\n")
	fmt.Printf("DoneJobQueue:\n")
	for e := m.DoneJobQueue.Front(); e != nil; e = e.Next() {
		j := e.Value.(util.Job)
		fmt.Print(j, "\n")
	}
	fmt.Printf("************************************************\n")
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
	return m.MapJobQueue.Len() == 0 && m.ReduceJobQueue.Len() == 0
}

func (m *Master) Report(args *ReportArgs, reply *ReportReply) error {
	reply.Success = false

	// Your code here.
	switch args.Job.Action {
	case util.Map:
		// Add reduce job to queue
		args.Job.State = util.Done
		m.DoneJobQueue.PushBack(args.Job)
		reply.Success = true
	case util.Reduce:
		// TODO:
		args.Job.State = util.Done
		m.DoneJobQueue.PushBack(args.Job)
	case util.Exit:
		if m.MapJobQueue.Len() == 0 && m.ReduceJobQueue.Len() == 0 {
			m.Commit = true
		}
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
		m.MapJobQueue.PushBack(job)
	}

	m.WorkerCount = count
	m.NReduce = nReduce

	for i := 0; i < nReduce; i++ {
		job := util.Job{
			Action:   util.Reduce,
			State:    util.Waiting,
			FileName: "mr-tmp-",
			NReduce:  m.WorkerCount,
			JobId:    i,
		}
		m.ReduceJobQueue.PushBack(job)
	}

	m.Print()
	m.server()
	return &m
}
