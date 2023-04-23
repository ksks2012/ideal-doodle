package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"mapreduce/util"
	"os"
	"strconv"

	"github.com/google/uuid"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RegistArgs struct {
	WorkerID uuid.UUID
}

type RegistReply struct {
	Success bool
}

type ReportArgs struct {
	Job util.Job
}

type ReportReply struct {
	Success bool
}

type GetJobReply struct {
	Job util.Job
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func (r *GetJobReply) setExitReply() {
	job := util.Job{Action: util.Exit}
	r.Job = job
}

func (r *GetJobReply) setJobReply(job util.Job) {
	r.Job = job
}
