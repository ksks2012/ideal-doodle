package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"mapreduce/util"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func writeTmpFile(keyValues []KeyValue, job util.Job) error {
	// TODO: config value
	tmpFolder := "mr-tmp/"

	// Write in each file of bucket
	prefixFileName := fmt.Sprintf("m-tmp-%d", job.JobId)
	var fileBucket = make(map[int]*json.Encoder)
	for i := 0; i < job.NReduce; i++ {
		outputFileName := fmt.Sprintf("%s-%d", prefixFileName, i)
		outFile, err := os.Create(tmpFolder + outputFileName)
		check(err)
		fileBucket[i] = json.NewEncoder(outFile)
		defer outFile.Close()
	}

	// Write each KeyValue pair to the file.
	for _, kv := range keyValues {
		reduce_idx := ihash(kv.Key) % job.NReduce
		err := fileBucket[reduce_idx].Encode(&kv)
		check(err)
	}

	return nil
}

func MapWorker(job util.Job, mapf func(string, string) []KeyValue) {
	// Read file
	data, err := ioutil.ReadFile(job.FileName)
	check(err)
	mapKeyValue := mapf(job.FileName, string(data))
	sort.Sort(ByKey(mapKeyValue))
	// Save work result
	writeTmpFile(mapKeyValue, job)
	args := ReportArgs{job}
	doneReply := ReportReply{}
	call("Master.Report", &args, &doneReply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	// TODO: worker rpc master function
	id := uuid.New()
	args := RegistArgs{}
	args.WorkerID = id
	registReply := RegistReply{}

	// Regist
	// TODO: Retry
	call("Master.Regist", &args, &registReply)
	if registReply.Success == false {
		// TODO: Exit current worker
	}

	// TODO: config value
	retry := 3
	for {
		log.Print("call Master.GetJob")
		getJobReply := GetJobReply{}
		call("Master.GetJob", &args, &getJobReply)
		log.Print(getJobReply.Job)
		switch getJobReply.Job.Action {
		case util.Map:
			MapWorker(getJobReply.Job, mapf)
			retry = 3
		case util.Reduce:
			// Read file
			empty := []string{}
			reducef("", empty)
			retry = 3
		case util.Exit:
			return
		default:
			log.Printf("Error Action: %v would retry times: %d", getJobReply.Job.Action, retry)
			if retry < 0 {
				return
			}
			retry--
		}
		// TODO: Apply the work

		// TODO: config value
		time.Sleep(500 * time.Millisecond)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
