package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []string

	taskUpdateTime map[int]int64 // 0 if finished, most recent update timestamp if running
	taskFinished   map[int]struct{}

	*sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapReduce(args *MapReduceArgs, reply *MapReduceReply) error {
	log.Printf("MapReduce request: %v", *args)
	switch s := args.TaskState; s {
	case New:
		if !c.Done() {
			c.Lock()
			defer c.Unlock()
			mapFinished := len(c.taskFinished) >= len(c.files)
			// start reduce when map finished
			for i := 0; (!mapFinished && i < len(c.files)) || (mapFinished && i < len(c.files)+c.nReduce); i++ {

				updateTime, started := c.taskUpdateTime[i]
				timeout := time.Since(time.Unix(updateTime, 0)) > 10*time.Second
				_, finished := c.taskFinished[i]

				if !finished && (!started || timeout) { // reissue timeout task
					c.taskUpdateTime[i] = time.Now().Unix()
					if i < len(c.files) {
						reply.TaskType = Map
						reply.MapFile = c.files[i]
						reply.TaskID = i
					} else {
						reply.TaskType = Reduce
						reply.TaskID = i - len(c.files)
					}
					reply.NReduce = c.nReduce
					return nil
				}

			}
			reply.TaskType = None
		} else {
			reply.TaskType = Done
		}
	case Running:
		return nil
	case Failed:
		c.Lock()
		delete(c.taskUpdateTime, args.TaskID)
		c.Unlock()
	case Succeeded:
		c.Lock()
		if args.TaskType == Reduce {
			args.TaskID += len(c.files)
		}
		delete(c.taskUpdateTime, args.TaskID)
		c.taskFinished[args.TaskID] = struct{}{}
		log.Printf("task id:%d, task finished: %v", args.TaskID, c.taskFinished)
		c.Unlock()
	default:
		return fmt.Errorf("invalid task state: %v", s)
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
	c.RLock()
	defer c.RUnlock()
	ret = len(c.taskFinished) == len(c.files)+c.nReduce

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.taskUpdateTime = make(map[int]int64)
	c.taskFinished = make(map[int]struct{})
	c.RWMutex = &sync.RWMutex{}

	c.server()
	return &c
}
