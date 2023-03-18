package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"mapreduce/util"
	"net/rpc"
	"os"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	print(keyValues)
	// return nil
	tmpFolder := "mr-tmp/"

	outputFileName := fmt.Sprintf("m-tmp-%d-%d", job.JobId, ihash(keyValues[0].Key))

	file, err := os.Create(tmpFolder + outputFileName)
	check(err)
	defer file.Close() // ensure file is closed after writing

	// Write each KeyValue pair to the file, one per line.
	for _, kv := range keyValues {
		line := fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
		_, err := file.WriteString(line)
		check(err)
	}

	return nil
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

	fmt.Print("call Master.GetJob")
	getJobReply := GetJobReply{}
	call("Master.GetJob", &args, &getJobReply)
	fmt.Print(getJobReply.Job)

	if getJobReply.Job.Action == util.Map {
		// Read file
		data, err := ioutil.ReadFile(getJobReply.Job.FileName)
		check(err)
		mapKeyValue := mapf(getJobReply.Job.FileName, string(data))
		// TODO: Save work result
		writeTmpFile(mapKeyValue, getJobReply.Job)
	} else if getJobReply.Job.Action == util.Reduce {
		// Read file
		empty := []string{}
		reducef("", empty)
	} else if getJobReply.Job.Action == util.Exit {
		// TODO: Exit current worker
	}

	// TODO: Apply the work

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
