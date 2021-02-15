package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Request() (*MapReduceReply, error) {
	reply := &MapReduceReply{}
	err := call("Coordinator.MapReduce",
		&MapReduceArgs{TaskState: New},
		reply)
	return reply, err
}

func Report(taskID int, tp TaskType, state TaskState) (*MapReduceReply, error) {
	reply := &MapReduceReply{}
	err := call("Coordinator.MapReduce",
		&MapReduceArgs{TaskID: taskID, TaskState: state, TaskType: tp},
		reply)
	return reply, err
}

func ReportFailed(taskID int, taskType TaskType) error {
	_, err := Report(taskID, taskType, Failed)
	return err
}

func ReportSucceeded(taskID int, taskType TaskType) error {
	_, err := Report(taskID, taskType, Succeeded)
	return err
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		reply, err := Request()
		if err != nil {
			log.Printf("request task error: %s", err)
			continue
		}

		log.Printf("request task reply: %v", *reply)

		switch t := reply.TaskType; t {
		case None:
			continue
		case Done:
			return
		case Map:
			if err := DoMap(reply, mapf); err != nil {
				log.Printf("map func: %s", err)
				continue
			}
		case Reduce:
			if err := DoReduce(reply, reducef); err != nil {
				log.Printf("reduce func: %s", err)
				continue
			}
		default:
			log.Printf("invalid task type: %v", t)
			continue
		}

		reply, err = Report(reply.TaskID, reply.TaskType, Succeeded)
		if err != nil {
			log.Printf("request task error: %s", err)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func DoMap(reply *MapReduceReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.MapFile)
	if err != nil {
		return fmt.Errorf("cannot open %v: %s", reply.MapFile, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read %v: %s", reply.MapFile, err)
	}
	file.Close()
	kva := mapf(reply.MapFile, string(content))
	// create key-value array by its hash value
	kvas := make([][]KeyValue, reply.NReduce)
	for i, _ := range kvas {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		kvas[ihash(kv.Key)%reply.NReduce] = append(kvas[ihash(kv.Key)%reply.NReduce], kv)
	}
	// serialize key-value array
	tempFiles := make([]*os.File, reply.NReduce)
	wd, _ := os.Getwd()
	for i, kva := range kvas {
		data, _ := json.Marshal(kva)
		// create temp file first so that when error occurred,
		// no intermediate files exposed to reduce stage
		tempFile, err := ioutil.TempFile(wd, "temp-*")
		if err != nil {
			return fmt.Errorf("create temp file: %s", err)
		}
		if _, err := tempFile.Write(data); err != nil {
			return fmt.Errorf("rename file %s: %s", tempFile.Name(), err)
		}
		tempFiles[i] = tempFile
		tempFiles[i].Close()
	}
	// rename temp files
	for i, f := range tempFiles {
		if err := os.Rename(f.Name(), path.Join(wd, fmt.Sprintf("mr-%d-%d", reply.TaskID, i))); err != nil {
			return fmt.Errorf("rename file %s: %s", f.Name(), err)
		}
	}
	return nil
}

func DoReduce(reply *MapReduceReply, reducef func(string, []string) string) error {
	kva := make([]KeyValue, 0)
	wd, _ := os.Getwd()
	// read the output from map stage
	err := filepath.Walk(wd, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		matched, err := regexp.Match(fmt.Sprintf("mr-[0-9]+-%d", reply.TaskID), []byte(info.Name()))
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		content, err := ioutil.ReadFile(info.Name())
		kvs := make([]KeyValue, 0)
		if err := json.Unmarshal(content, &kvs); err != nil {
			return err
		}
		kva = append(kva, kvs...)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk: %s", err)
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
