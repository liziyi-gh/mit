package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const HEART_BEAT_TIMES int = 5

type MapJob struct {
	status int
	// 0 allocated and doing
	// 1 done
	job_id                         int
	last_heart_beat_passby_seconds int
	input_filename                 string
}

type ReduceJob struct {
	status int
	// 0 allocated and doing
	// 1 done
	job_id                         int
	last_heart_beat_passby_seconds int
	output_filename                string
	reduce_part                    int
}

type Coordinator struct {
	// Your definitions here.
	mutex                  sync.Mutex
	map_jobs_number        int
	reduce_jobs_number     int
	reduce_job_part        chan (int)
	new_id                 chan (int)
	input_files            []string
	map_jobs               []MapJob
	reduce_jobs            []ReduceJob
	alloc_file             chan (string)
	all_map_job_done       bool
	all_job_done           bool
	map_intermediate_files []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AllocJob(args *Job, reply *Job) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.JOB_TYPE = JOB_NONE
	if c.all_job_done {
		return nil
	}
	// if no map job left but not all map job done, return nil
	if (len(c.map_jobs) == c.map_jobs_number) && !c.all_map_job_done {
		log.Print("all map job allocted but not all done")
		return nil
	}
	// if still have map job to do
	if len(c.map_jobs) < c.map_jobs_number {
		log.Print("alloc new map job")
		new_map_job := MapJob{
			job_id:                         <-c.new_id,
			status:                         0,
			input_filename:                 <-c.alloc_file,
			last_heart_beat_passby_seconds: HEART_BEAT_TIMES,
		}
		c.map_jobs = append(c.map_jobs, new_map_job)

		reply.JOB_TYPE = JOB_MAP
		reply.FILENAME = new_map_job.input_filename
		reply.JOB_ID = new_map_job.job_id

		return nil
	}

	if len(c.reduce_jobs) < c.reduce_jobs_number {
		new_reduce_job := ReduceJob{
			job_id:                         <-c.new_id,
			status:                         0,
			last_heart_beat_passby_seconds: 0,
			reduce_part:                    <-c.reduce_job_part,
		}

		log.Printf("new reduce job alloc")

		c.reduce_jobs = append(c.reduce_jobs, new_reduce_job)

		reply.JOB_TYPE = JOB_REDUCE
		reply.FILENAMES = c.map_intermediate_files
		reply.JOB_ID = new_reduce_job.job_id
		reply.REDUCE_PART = new_reduce_job.reduce_part
		reply.REDUCE_JOBS_NUMBER = c.reduce_jobs_number

		return nil
	}

	if len(c.reduce_jobs) == c.reduce_jobs_number {
		return nil
	}

	return nil
}

func (c *Coordinator) JobHeartBeat(args *Job, reply *Job) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.JOB_TYPE {
	case JOB_NONE:
		return nil

	case JOB_MAP:
		log.Printf("heart beat from map job: %d", args.JOB_ID)
		for i, mj := range c.map_jobs {
			if mj.job_id == args.JOB_ID {
				c.map_jobs[i].last_heart_beat_passby_seconds = HEART_BEAT_TIMES
				return nil
			}
		}
		log.Printf("heart beat from map job: %d, not in c.map_jobs", args.JOB_ID)
		return nil

	case JOB_REDUCE:
		log.Printf("heart beat from reduce job: %d", args.JOB_ID)
		for i, rj := range c.reduce_jobs {
			if rj.job_id == args.JOB_ID {
				c.reduce_jobs[i].last_heart_beat_passby_seconds = HEART_BEAT_TIMES
				return nil
			}
		}
		log.Printf("heart beat from map job: %d, not in c.reduce_jobs", args.JOB_ID)
		return nil
	}

	return nil
}

func (c *Coordinator) JobDone(args *JobDone, reply *JobDone) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	result_file_name := "mr-out-0"

	log.Printf("jod %d done\n", args.JOB_ID)

	if args.JOB_TYPE == JOB_MAP {
		log.Printf("add file %s to intermediate\n", args.FILENAME)
		c.map_intermediate_files = append(c.map_intermediate_files,
			args.FILENAME)

		for i, map_job := range c.map_jobs {
			if map_job.job_id == args.JOB_ID {
				c.map_jobs[i].status = 1
				break
			}
		}
		if len(c.map_jobs) == c.map_jobs_number {
			all_job_done := true
			for _, map_job := range c.map_jobs {
				if map_job.status == 0 {
					all_job_done = false
					break
				}
			}

			if all_job_done {
				c.all_map_job_done = true
				log.Printf("all map job done\n")
			}
		}

		return nil
	}

	if args.JOB_TYPE == JOB_REDUCE {
		reduce_output_file_name := args.FILENAME

		result_file, err := os.OpenFile(result_file_name,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer result_file.Close()
		if err != nil {
			log.Print("open result file failed\n")
		}

		data, err := ioutil.ReadFile(reduce_output_file_name)
		if err != nil {
			log.Print("read reduce intermediate file failed\n")
		}

		result_file.Write(data)

		for i, reduce_job := range c.reduce_jobs {
			if reduce_job.job_id == args.JOB_ID {
				c.reduce_jobs[i].status = 1
				break
			}
		}

		if len(c.reduce_jobs) == c.reduce_jobs_number {
			all_job_done := true
			for _, reduce_job := range c.reduce_jobs {
				if reduce_job.status == 0 {
					all_job_done = false
					break
				}
			}
			if all_job_done {
				c.all_job_done = true
				log.Printf("all map job done\n")
			}
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	ret = c.all_job_done
	c.mutex.Unlock()

	return ret
}

func (c *Coordinator) Beat() {
	for {
		log.Printf("beating\n")
		c.mutex.Lock()

		for i, job := range c.map_jobs {
			if job.status == 1 {
				continue
			}
			c.map_jobs[i].last_heart_beat_passby_seconds -= 1
		}

		for i, job := range c.reduce_jobs {
			if job.status == 1 {
				continue
			}
			c.reduce_jobs[i].last_heart_beat_passby_seconds -= 1
		}

		// this approach cause more garbage
		tmp_map_jobs := []MapJob{}
		tmp_reduce_jobs := []ReduceJob{}

		for i, _ := range c.map_jobs {
			if c.map_jobs[i].last_heart_beat_passby_seconds == 0 {
				c.alloc_file <- c.map_jobs[i].input_filename
				log.Printf("map job %d died\n", c.map_jobs[i].job_id)
			} else {
				tmp_map_jobs = append(tmp_map_jobs, c.map_jobs[i])
			}
		}
		c.map_jobs = tmp_map_jobs

		for i, _ := range c.reduce_jobs {
			if c.reduce_jobs[i].last_heart_beat_passby_seconds == 0 {
				c.reduce_job_part <- c.reduce_jobs[i].reduce_part
				log.Printf("reduce job %d died\n", c.reduce_jobs[i].job_id)
			} else {
				tmp_reduce_jobs = append(tmp_reduce_jobs, c.reduce_jobs[i])
			}
		}
		c.reduce_jobs = tmp_reduce_jobs

		c.mutex.Unlock()
		// do reduce heart beat
		time.Sleep(time.Duration(HEART_BEAT_DURATION))
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	file, err := os.OpenFile("coor.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	c := Coordinator{}
	c.new_id = make(chan int)
	c.alloc_file = make(chan string, len(files))
	c.input_files = files
	c.all_map_job_done = false
	c.map_jobs_number = len(files)
	c.reduce_jobs_number = 2
	c.reduce_job_part = make(chan int, c.reduce_jobs_number)
	c.map_intermediate_files = []string{}

	go func() {
		j := 0
		for {
			c.new_id <- j
			j = j + 1
		}
	}()

	go func() {
		for _, filename := range files {
			c.alloc_file <- filename
		}
	}()

	go func() {
		for i := 0; i < c.reduce_jobs_number; i++ {
			c.reduce_job_part <- i
		}
	}()

	go c.Beat()

	c.server()
	return &c
}
