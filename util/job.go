package util

type JobAction int

const (
	Map JobAction = iota
	Reduce
	Wait
	Exit
)

func (jn JobAction) JobNum() string {
	return [...]string{"Map", "Reduce", "Exit"}[jn]
}

// TODO: Maybe change to different queue
type JobState int

const (
	Waiting JobState = iota
	Doing
	Done
)

func (js JobState) JobState() string {
	return [...]string{"Waiting", "Doing", "Done"}[js]
}

type Job struct {
	Action   JobAction
	State    JobState
	FileName string
	JobId    int
	// TODO: config value
	NReduce int
}
