package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMap(nReduce int, job MapJob, mapf func(string, string) []KeyValue) {
	filename := job.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	ifilenames := []string{}
	iencs := []*json.Encoder{}
	get_ifilename := func(reduceId int) string {
		return strconv.Itoa(reduceId) + "-" + strconv.Itoa(job.Id)
	}
	for i := 0; i < nReduce; i++ {
		ifilename := get_ifilename(i)
		ifile, err := ioutil.TempFile(".", ifilename+"-")
		if err != nil {
			log.Fatalln("Fail to create temp file for " + ifilename + ": " + err.Error())
		}
		ifilenames = append(ifilenames, ifile.Name())
		iencs = append(iencs, json.NewEncoder(ifile))
	}
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		err := iencs[reduceId].Encode(&kv)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
	for i := 0; i < nReduce; i++ {
		ifilename := get_ifilename(i)
		os.Rename(ifilenames[i], ifilename)
	}
}

func doReduce(reduceId int, reducef func(string, []string) string) {
	matches, err := filepath.Glob(strconv.Itoa(reduceId) + "-*")
	if err != nil {
		log.Fatalln(err.Error())
	}
	kva := []KeyValue{}
	for _, ifilename := range matches {
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Fatalln(err.Error())
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	// External sort?
	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalln(err.Error())
	}

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
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	nReduce := 0
	ok := call("Coordinator.RegisterWorker", struct{}{}, &nReduce)
	if !ok {
		log.Fatalf("Register failed\n")
	}

	for {
		job := MapJob{}
		ok = call("Coordinator.AssignMapJob", struct{}{}, &job)
		if !ok {
			log.Fatalln("RPC AssignMapJob failed")
		}
		if job.Id == -1 {
			break
		}
		doMap(nReduce, job, mapf)
		ok = call("Coordinator.HandleMapJobDone", job.Id, &struct{}{})
		if !ok {
			log.Fatalln("RPC HandleMapJobDone failed")
		}
	}

	// fmt.Println("Switching to reduce phase")

	for {
		reduceId := 0
		ok = call("Coordinator.AssignReduceJob", struct{}{}, &reduceId)
		if !ok {
			// log.Fatalln("RPC AssignReduceJob failed")
			break
		}
		if reduceId == -1 {
			break
		}
		doReduce(reduceId, reducef)
		ok = call("Coordinator.HandleReduceJobDone", reduceId, &struct{}{})
		if !ok {
			log.Fatalf("RPC HandleReduceJobDone failed")
		}
	}

	// fmt.Println("Worker done")
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

	fmt.Println(err)
	return false
}
