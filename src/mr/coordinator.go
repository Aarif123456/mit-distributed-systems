package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	mapTm    *taskManager[MapReply]
	reduceTm *taskManager[ReduceReply]
}

var errJobCancelled = errors.New("job cancelled")

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapJobs := make([]MapReply, 0, len(files))
	for i, file := range files {
		mapJobs = append(mapJobs, MapReply{
			InFile:        file,
			MapTaskID:     i,
			NumReduceTask: nReduce,
		})
	}
	mapTm := newTaskManager(mapJobs)

	nMapTask := len(files)
	reduceJobs := make([]ReduceReply, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceJobs = append(reduceJobs, ReduceReply{
			ReduceTaskID: i,
			NumMapTask:   nMapTask,
		})
	}
	reduceTm := newTaskManager(reduceJobs)

	c := &Coordinator{
		mapTm:    mapTm,
		reduceTm: reduceTm,
	}
	c.server()

	return c
}

func (c *Coordinator) Idle(_ *IdleArgs, reply *IdleReply) error {
	switch {
	case !c.mapTm.IsDone():
		reply.Op = RunMap
	case !c.reduceTm.IsDone():
		reply.Op = RunReduce
	default:
		reply.Op = Die
	}

	return nil
}

func (c *Coordinator) Map(_ *MapArgs, reply *MapReply) error {
	val, ok := c.mapTm.Run()
	if !ok {
		return errJobCancelled
	}

	*reply = val
	return nil
}

func (c *Coordinator) DoneMap(args *DoneMapArgs, _ *DoneMapReply) error {
	c.mapTm.MarkDone(args.MapTaskID)
	return nil
}

func (c *Coordinator) Reduce(_ *ReduceArgs, reply *ReduceReply) error {
	val, ok := c.reduceTm.Run()
	if !ok {
		return errJobCancelled
	}

	*reply = val
	return nil
}

func (c *Coordinator) DoneReduce(args *DoneReduceArgs, _ *DoneReduceReply) error {
	c.reduceTm.MarkDone(args.ReduceTaskID)
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapTm.IsDone() && c.reduceTm.IsDone()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Fatal("could not register server:", err)
	}

	rpc.HandleHTTP()
	// lis, err := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)

	lis, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go func() {
		if err := http.Serve(lis, nil); err != nil {
			log.Fatal("cannot serve traffic:", err)
		}
	}()
}
