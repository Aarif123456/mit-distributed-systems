package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type (
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
	// Map functions return a slice of KeyValue.
	KeyValue struct {
		Key   string
		Value string
	}
)

// main/mrworker.go calls this function.
func Worker(mapf MapFunc, reducef ReduceFunc) {
	for {
		switch idleResp := IdleCall(); idleResp.Op {
		case Die:
			return
		case RunMap:
			reply, err := MapCall()
			if err != nil {
				log.Printf("Map Err %s\n", err)
				continue
			}
			runMap(reply, mapf)
		case RunReduce:
			reply, err := ReduceCall()
			if err != nil {
				log.Printf("Reduce Err %s\n", err)
				continue
			}
			runReduce(reply, reducef)
		}
	}
}

// IdleCall let's the coordinator know the worker is ready for more work
func IdleCall() *IdleReply {
	reply := &IdleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Idle", &IdleArgs{}, reply)

	return reply
}

func runMap(reply *MapReply, mapf MapFunc) {
	content := readFile(reply.InFile)

	// run map function
	kva := mapf(reply.InFile, string(content))

	outWriter := getFileWriters(reply.MapTaskID, reply.NumReduceTask)

	saveToDisk(kva, outWriter)

	DoneMapCall(&DoneMapArgs{reply.MapTaskID})
}

func readFile(filename string) []byte {
	// open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	// read from file
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return content
}

func getFileWriters(mapTaskID, numReduceTask int) []*json.Encoder {
	outWriter := make([]*json.Encoder, 0, numReduceTask)
	for nReduceTask := 0; nReduceTask < numReduceTask; nReduceTask++ {
		outName := getMapOutFile(mapTaskID, nReduceTask)
		outFile, _ := os.Create(outName)
		outWriter = append(outWriter, json.NewEncoder(outFile))
	}

	return outWriter
}

func saveToDisk(kva []KeyValue, outWriter []*json.Encoder) {
	n := len(outWriter)
	for _, kv := range kva {
		i := ihash(kv.Key) % n
		if err := outWriter[i].Encode(&kv); err != nil {
			log.Println(err)
		}
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32() & 0x7fffffff)
}

func MapCall() (*MapReply, error) {
	reply := &MapReply{}

	// send the RPC request, wait for the reply.
	if err := call("Coordinator.Map", MapArgs{}, reply); err != nil {
		return nil, err
	}

	return reply, nil
}

func DoneMapCall(args *DoneMapArgs) {
	reply := &DoneMapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.DoneMap", args, reply)
}

func runReduce(reply *ReduceReply, reducef ReduceFunc) {
	kva := readReduceData(reply.ReduceTaskID, reply.NumMapTask)

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	outName := getReduceOutFile(reply.ReduceTaskID)
	outFile, _ := os.Create(outName)

	// call Reduce on each distinct key in kva[],
	// and print the result to the output file.
	slow := 0
	for slow < len(kva) {
		fast := slow + 1
		for fast < len(kva) && kva[fast].Key == kva[slow].Key {
			fast++
		}

		var values []string
		for ; slow < fast; slow++ {
			values = append(values, kva[slow].Value)
		}
		if slow == fast {
			panic("slow was not supposed to catch up")
		}
		output := reducef(kva[slow].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[slow].Key, output)

		slow = fast
	}

	DoneReduceCall(&DoneReduceArgs{reply.ReduceTaskID})
}

func readReduceData(reduceTaskID, numMapTask int) []KeyValue {
	var kva []KeyValue

	for nMapTask := 0; nMapTask < numMapTask; nMapTask++ {
		filename := getMapOutFile(nMapTask, reduceTaskID)
		// open file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return kva
}

func ReduceCall() (*ReduceReply, error) {
	reply := &ReduceReply{}

	// send the RPC request, wait for the reply.
	if err := call("Coordinator.Reduce", &ReduceArgs{}, reply); err != nil {
		return nil, err
	}

	return reply, nil
}

func DoneReduceCall(args *DoneReduceArgs) *DoneReduceReply {
	reply := &DoneReduceReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.DoneReduce", args, reply)

	return reply
}

func getMapOutFile(mapTaskNum, reduceTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNum, reduceTaskNum)
}

func getReduceOutFile(reduceTaskNum int) string {
	return "mr-out-" + strconv.Itoa(reduceTaskNum)
}

// call sends an RPC request to the coordinator, waits for the response.
// and returns true if nothing went wrong.
func call(rpcName string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return fmt.Errorf("dialing %w", err)
	}
	defer c.Close()

	if err := c.Call(rpcName, args, reply); err != nil {
		return fmt.Errorf("calling %w", err)
	}

	return nil
}
