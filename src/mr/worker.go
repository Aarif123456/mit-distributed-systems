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
	MapFunc    func(fileName, content string) []KeyValue
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
				log.Printf("Map call error: %s\n", err)
				continue
			}
			if err := runMap(reply, mapf); err != nil {
				log.Printf("Map run error: %s\n", err)
				continue
			}
		case RunReduce:
			reply, err := ReduceCall()
			if err != nil {
				log.Printf("Reduce call error: %s\n", err)
				continue
			}

			if err := runReduce(reply, reducef); err != nil {
				log.Printf("Reduce run error: %s\n", err)
				continue
			}
		}
	}
}

// IdleCall let's the coordinator know the worker is ready for more work
func IdleCall() *IdleReply {
	reply := &IdleReply{}

	// send the RPC request, wait for the reply.
	_ = call("Coordinator.Idle", &IdleArgs{}, reply)

	return reply
}

func runMap(reply *MapReply, mapf MapFunc) error {
	content, err := readFile(reply.InFile)
	if err != nil {
		return err
	}

	kva := mapf(reply.InFile, content)

	outWriter, err := getFileWriters(reply.MapTaskID, reply.NumReduceTask)
	if err != nil {
		return err
	}

	if err := saveToDisk(kva, outWriter); err != nil {
		return err
	}

	DoneMapCall(&DoneMapArgs{reply.MapTaskID})
	return nil
}

func readFile(fileName string) (string, error) {
	// open file
	file, err := os.Open(fileName)
	if err != nil {
		return "", fmt.Errorf("cannot open %s", fileName)
	}
	defer file.Close()

	// read from file
	content, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read %v", fileName)
	}

	return string(content), nil
}

func getFileWriters(mapTaskID, numReduceTask int) ([]*json.Encoder, error) {
	outWriter := make([]*json.Encoder, 0, numReduceTask)
	for rt := 0; rt < numReduceTask; rt++ {
		outName := getMapOutFile(mapTaskID, rt)
		outFile, err := os.Create(outName)
		if err != nil {
			return nil, fmt.Errorf("cannot get file writer: %w", err)
		}
		outWriter = append(outWriter, json.NewEncoder(outFile))
	}

	return outWriter, nil
}

func saveToDisk(kva []KeyValue, outWriter []*json.Encoder) error {
	n := len(outWriter)
	for _, kv := range kva {
		kv := kv
		i := ihash(kv.Key) % n
		if err := outWriter[i].Encode(&kv); err != nil {
			return fmt.Errorf("cannot save: %w", err)
		}
	}

	return nil
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
	_ = call("Coordinator.DoneMap", args, reply)
}

func runReduce(reply *ReduceReply, reducef ReduceFunc) error {
	kva, err := readReduceData(reply.ReduceTaskID, reply.NumMapTask)
	if err != nil {
		return err
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// write to temp file first, in case something breaks
	tmpOutFile, err := os.CreateTemp("", "*")
	if err != nil {
		return err
	}
	tmpFileName := tmpOutFile.Name()
	// clean up in case something goes wrong
	defer os.Remove(tmpFileName)

	// call Reduce on each distinct key in kva[],
	// and write the result to the output file.
	if err := applyAndSaveReduce(kva, tmpOutFile, reducef); err != nil {
		return err
	}

	if err := tmpOutFile.Close(); err != nil {
		return err
	}

	outName := getReduceOutFile(reply.ReduceTaskID)
	if err := os.Rename(tmpFileName, outName); err != nil {
		return err
	}

	DoneReduceCall(&DoneReduceArgs{reply.ReduceTaskID})
	return nil
}

func readReduceData(reduceTaskID, numMapTask int) ([]KeyValue, error) {
	var kva []KeyValue

	for mt := 0; mt < numMapTask; mt++ {
		fileName := getMapOutFile(mt, reduceTaskID)
		if err := readInKeyValue(kva, fileName); err != nil {
			return nil, err
		}
	}

	return kva, nil
}

func readInKeyValue(kva []KeyValue, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("cannot open %s", fileName)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for dec.More() {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			return fmt.Errorf("cannot decode %w", err)
		}
		kva = append(kva, kv)
	}

	return nil
}

func applyAndSaveReduce(kva []KeyValue, w io.Writer, reducef ReduceFunc) error {
	for slow := 0; slow < len(kva); {
		fast := slow + 1
		key := kva[slow].Key
		for fast < len(kva) && kva[fast].Key == key {
			fast++
		}

		var vals []string
		for k := slow; k < fast; k++ {
			vals = append(vals, kva[k].Value)
		}

		output := reducef(key, vals)
		if _, err := fmt.Fprintf(w, "%v %v\n", key, output); err != nil {
			return err
		}

		slow = fast
	}

	return nil
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
	_ = call("Coordinator.DoneReduce", args, reply)

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
func call(rpcName string, args, reply any) error {
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
