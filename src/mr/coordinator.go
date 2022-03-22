package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const kTimeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	// workerIdTop   uint64
	nReduce        int
	mapJobsChan    chan MapJob // File names
	reduceJobsChan chan int    // Reduce job ID
	completeChan   chan struct{}
	done           uint32

	mutex            sync.Mutex
	jobCompleteChans map[int]chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(_ struct{}, reply *int) error {
	// reply.Id = atomic.AddUint64(&c.workerIdTop, 1)
	*reply = c.nReduce
	return nil
}

func (c *Coordinator) AssignMapJob(_ struct{}, reply *MapJob) error {
	job, ok := <-c.mapJobsChan
	if !ok {
		job.Id = -1
	}
	*reply = job
	if ok {
		complete := make(chan struct{})
		c.mutex.Lock()
		c.jobCompleteChans[job.Id] = complete
		c.mutex.Unlock()
		go c.mapJobTimer(job, complete)
	}
	return nil
}

// If the second parameter is struct{}, then:
// rpc: can't find method Coordinator.AssignReduceJob
func (c *Coordinator) HandleMapJobDone(id int, _ *struct{}) error {
	c.mutex.Lock()
	c.jobCompleteChans[id] <- struct{}{}
	c.mutex.Unlock()
	c.completeChan <- struct{}{}
	return nil
}

func (c *Coordinator) mapJobTimer(job MapJob, complete chan struct{}) {
	select {
	case <-time.After(kTimeout):
		c.mapJobsChan <- job
	case <-complete:
	}
}

func (c *Coordinator) AssignReduceJob(_ struct{}, reply *int) error {
	// fmt.Println("AssignReduceJob")
	reduceID, ok := <-c.reduceJobsChan
	if !ok {
		reduceID = -1
	}
	*reply = reduceID
	if ok {
		complete := make(chan struct{})
		c.mutex.Lock()
		c.jobCompleteChans[reduceID] = complete
		c.mutex.Unlock()
		go c.reduceJobTimer(reduceID, complete)
	}
	// fmt.Printf("AssignReduceJob reply %d\n", reduceID)
	return nil
}

func (c *Coordinator) HandleReduceJobDone(reduceId int, _ *struct{}) error {
	c.mutex.Lock()
	c.jobCompleteChans[reduceId] <- struct{}{}
	c.mutex.Unlock()
	c.completeChan <- struct{}{}
	return nil
}

func (c *Coordinator) reduceJobTimer(reduceID int, complete chan struct{}) {
	select {
	case <-time.After(kTimeout):
		c.reduceJobsChan <- reduceID
	case <-complete:
	}
}

func (c *Coordinator) Run(files []string, nReduce int) {
	go func() {
		for i, filename := range files {
			c.mapJobsChan <- MapJob{
				FileName: filename,
				Id:       i,
			}
		}
	}()

	unfinishedCnt := len(files)
	for unfinishedCnt != 0 {
		<-c.completeChan
		unfinishedCnt -= 1
	}

	close(c.mapJobsChan)

	go func() {
		for i := 0; i < nReduce; i++ {
			c.reduceJobsChan <- i
		}
	}()

	unfinishedCnt = nReduce
	for unfinishedCnt != 0 {
		<-c.completeChan
		unfinishedCnt -= 1
	}

	close(c.reduceJobsChan)

	atomic.StoreUint32(&c.done, 1)
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
	// Your code here.
	return atomic.LoadUint32(&c.done) != 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:        nReduce,
		mapJobsChan:    make(chan MapJob),
		reduceJobsChan: make(chan int),
		completeChan:   make(chan struct{}),
		done:           0,

		mutex:            sync.Mutex{},
		jobCompleteChans: make(map[int]chan struct{}),
	}

	// Your code here.

	c.server()
	go c.Run(files, nReduce)

	return &c
}
