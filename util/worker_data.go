package util

type WokerState int

const (
	Init WokerState = iota
	Running
	Stop
	Error
)

func (ws WokerState) JobState() string {
	return [...]string{"Init", "Running", "Stop", "Error"}[ws]
}

type WorkerData struct {
	State WokerState
	Job   Job // processing job
}

func (wd WorkerData) SetRunning(job Job) {
	wd.Job = job
	wd.State = Running
}

func (wd WorkerData) SetExit() {
	wd.State = Stop
}
