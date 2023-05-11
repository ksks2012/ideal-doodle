package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"mapreduce/util"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func writeTmpFile(keyValues []KeyValue, job util.Job) error {
	// Write in each file of bucket
	prefixFileName := fmt.Sprintf("mr-tmp-%d", job.JobId)
	var fileBucket = make(map[int]*json.Encoder)
	for i := 0; i < job.NReduce; i++ {
		outputFileName := fmt.Sprintf("%s-%d", prefixFileName, i)
		outFile, err := os.Create(outputFileName)
		checkErr(err)
		fileBucket[i] = json.NewEncoder(outFile)
		defer outFile.Close()
	}

	// Write each KeyValue pair to the file.
	for _, kv := range keyValues {
		reduce_idx := ihash(kv.Key) % job.NReduce
		err := fileBucket[reduce_idx].Encode(&kv)
		checkErr(err)
	}

	return nil
}

func MapWorker(job util.Job, mapf func(string, string) []KeyValue) {
	// Read file
	data, err := ioutil.ReadFile(job.FileName)
	checkErr(err)
	mapKeyValue := mapf(job.FileName, string(data))
	sort.Sort(ByKey(mapKeyValue))
	// Save work result
	writeTmpFile(mapKeyValue, job)
	args := ReportArgs{job}
	reportReply := ReportReply{}
	call("Master.Report", &args, &reportReply)
}

func readTmpFile(job util.Job) ([]KeyValue, error) {
	inputPrefixFileName := "mr-tmp"

	// define return value
	var fileBucket = make(map[int]*json.Decoder)
	for i := 0; i < job.NReduce; i++ {
		inputFileName := fmt.Sprintf("%s-%d-%d", inputPrefixFileName, i, job.JobId)
		log.Printf("[Worker] input file: %s", inputFileName)
		fullPath := inputFileName
		_, err := os.Stat(fullPath)
		if os.IsExist(err) {
			fmt.Printf("file %s not exist\n", fullPath)
			return []KeyValue{}, os.ErrNotExist
		}
		inputFile, err := os.Open(fullPath)
		checkErr(err)
		defer inputFile.Close()
		fileBucket[i] = json.NewDecoder(inputFile)
	}

	// Read each KeyValue pair from the file.
	keyValues := []KeyValue{}
	for {
		var kv KeyValue
		err := fileBucket[0].Decode(&kv)
		if err == io.EOF {
			break
		}
		checkErr(err)
		keyValues = append(keyValues, kv)
	}
	return keyValues, nil
}

func writeOutputFile(keyValues []KeyValue, job util.Job, reducef func(string, []string) string) error {
	outputPrefixFileName := "mr-out-"

	outputFileName := outputPrefixFileName + strconv.Itoa(job.JobId)
	outFile, err := os.Create(outputFileName)
	log.Println("complete to ", job.JobId, "start to write in to ", outputFileName)
	checkErr(err)
	defer outFile.Close()

	i := 0
	for i < len(keyValues) {
		j := i + 1
		for j < len(keyValues) && keyValues[j].Key == keyValues[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyValues[k].Value)
		}
		output := reducef(keyValues[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", keyValues[i].Key, output)

		i = j
	}

	return nil
}

func ReduceWorker(job util.Job, reducef func(string, []string) string) error {
	// Read file by NReduce
	mapStr, err := readTmpFile(job)
	checkErr(err)
	// save result
	writeOutputFile(mapStr, job, reducef)

	args := ReportArgs{job}
	reportReply := ReportReply{}
	call("Master.Report", &args, &reportReply)
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

	// TODO: config value
	retry := 3
	for {
		log.Print("[Worker] call Master.GetJob")
		getJobReply := GetJobReply{}
		call("Master.GetJob", &args, &getJobReply)
		log.Print(getJobReply.Job)
		switch getJobReply.Job.Action {
		case util.Map:
			MapWorker(getJobReply.Job, mapf)
			retry = 3
		case util.Reduce:
			// Read file
			ReduceWorker(getJobReply.Job, reducef)
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
