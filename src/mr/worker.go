package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

var current_job = new(Job)
var current_job_lock sync.Mutex

func SetCurrentJob(j *Job) {
	current_job_lock.Lock()
	defer current_job_lock.Unlock()

	if j == nil {
		current_job.JOB_TYPE = JOB_NONE
	} else if j.JOB_TYPE == JOB_NONE {
		current_job.JOB_TYPE = JOB_NONE
	} else {
		current_job = j
	}
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func doMap(new_job *Job,
	mapf func(string, string) []KeyValue) {
	file, err := os.Open(new_job.FILENAME)
	if err != nil {
		log.Printf("cannot open %v\n", new_job.FILENAME)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v\n", new_job.FILENAME)
	}
	file.Close()
	kva := mapf(new_job.FILENAME, string(content))

	tmp_file, err := ioutil.TempFile(".", "*")
	output_file_name := "mr-" + fmt.Sprint(new_job.JOB_ID)
	file, err = os.OpenFile(tmp_file.Name(), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Printf("cannot open tmp file %v\n", tmp_file.Name())
	}
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err = enc.Encode(&kv)
		if err != nil {
			log.Printf("encode wrong\n")
		}
	}
	file.Close()
	//TODO: handle rename failure
	os.Rename(tmp_file.Name(), output_file_name)

	// told coordinator map job done
	job_done_args := JobDone{
		JOB_ID:   new_job.JOB_ID,
		FILENAME: output_file_name,
		JOB_TYPE: new_job.JOB_TYPE,
	}
	FinishJob(&job_done_args)
}

func doReduce(new_job *Job,
	reducef func(string, []string) string) {
	filenames := new_job.FILENAMES
	reduce_part := new_job.REDUCE_PART
	var kva []KeyValue
	for _, f := range filenames {
		file, err := os.Open(f)
		if err != nil {
			log.Printf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()

	}
	output_file_name := "reduce-" + fmt.Sprint(new_job.JOB_ID)
	output_file, err := os.Create(output_file_name)
	if err != nil {
		log.Print("create reduce intermediate file failed\n")
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		if ihash(kva[i].Key)%new_job.REDUCE_JOBS_NUMBER == reduce_part {
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			fmt.Fprintf(output_file, "%v %v\n", kva[i].Key, output)
		}
		i = j
	}

	args := JobDone{
		JOB_ID:   new_job.JOB_ID,
		JOB_TYPE: new_job.JOB_TYPE,
		FILENAME: output_file_name,
	}

	FinishJob(&args)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// go func to heart beat
	go func() {
		SetCurrentJob(nil)
		for {
			current_job_lock.Lock()
			reply := Job{}
			if current_job.JOB_TYPE != JOB_NONE {
				ok := call("Coordinator.JobHeartBeat", current_job, &reply)
				if ok {
					log.Printf("heart beat %d\n", current_job.JOB_ID)
				} else {
					log.Printf("heart beat failed %d\n", current_job.JOB_ID)
				}
			}
			current_job_lock.Unlock()

			time.Sleep(time.Duration(HEART_BEAT_DURATION))
		}
	}()

	file, err := os.OpenFile("worker.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	new_job := CallForJob()
	for {
		time.Sleep(time.Duration(1 * SEC_IN_NANOSECONDS))
		if new_job == nil {
			new_job = CallForJob()
			continue
		}
		job_type := new_job.JOB_TYPE
		log.Printf("new job type is %d\n", job_type)
		if job_type == JOB_MAP {
			doMap(new_job, mapf)
		}
		if job_type == JOB_REDUCE {
			doReduce(new_job, reducef)
		}
		new_job = nil
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func FinishJob(args *JobDone) {
	reply := JobDone{}

	ok := call("Coordinator.JobDone", args, &reply)
	if ok {
		log.Printf("job %d done\n", args.JOB_ID)
	} else {
		log.Printf("job failed\n")
	}
	SetCurrentJob(nil)
}

func CallForJob() *Job {
	args := Job{}
	reply := Job{}
	log.Printf("call once alloc job\n")
	ok := call("Coordinator.AllocJob", &args, &reply)

	if !ok {
		log.Printf("call failed!\n")
		return nil
	}

	log.Printf("call success\n")
	log.Printf("reply in CallForJob is \n")
	log.Print(reply)
	log.Printf("\n")

	SetCurrentJob(&reply)

	return &reply
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
