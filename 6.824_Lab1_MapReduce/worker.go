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
	"sync"
	"time"
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

func doMap(mapf func(string, string) []KeyValue, filename string, r int, x int) int {
	intermediate := make([][]KeyValue, r)
	for i := 0; i < r; i++ {
		intermediate[i] = []KeyValue{}
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return 1
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return 1
	}
	file.Close()
	kva := mapf(filename, string(content))

	for _, kv := range kva {
		rhash := ihash(kv.Key) % r
		intermediate[rhash] = append(intermediate[rhash], kv)
	}

	var writing_wg sync.WaitGroup
	writing_wg.Add(r)
	for i := 0; i < r; i++ {
		go func(i int) {
			// defer writing_wg.Done()
			// mappedName := fmt.Sprintf("mr-%v-%v.json", x, i)
			// file, _ := os.Create(mappedName)
			// defer file.Close()
			// enc := json.NewEncoder(file)
			// for _, kv := range intermediate[i] {
			// 	enc.Encode(&kv)
			// }

			defer writing_wg.Done()
			tempName := "tmp-map.json"
			currentDir, _ := os.Getwd()
			tempFile, _ := os.CreateTemp(currentDir, tempName)
			enc := json.NewEncoder(tempFile)
			for _, kv := range intermediate[i] {
				enc.Encode(&kv)
			}
			tempFile.Close()
			mappedName := fmt.Sprintf("mr-%v-%v.json", x, i)
			os.Rename(tempFile.Name(), mappedName)
		}(i)
	}
	writing_wg.Wait()
	return 0
}

func doReduce(reducef func(string, []string) string, m int, y int) int {

	kva := []KeyValue{}
	for i := 0; i < m; i++ {
		fileName := fmt.Sprintf("mr-%d-%d.json", i, y)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return 1
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	tempName := "tmp-mr-out"
	// tempName := fmt.Sprintf("tmp-mr-out-%v", y)
	currentDir, _ := os.Getwd()
	tempFile, _ := os.CreateTemp(currentDir, tempName)
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
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", y)
	os.Rename(tempFile.Name(), oname)
	return 0
}

func heartBeat(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	msg *Message,
	reply *Reply) int {
	responsed := call("Coordinator.Response", msg, reply)
	if !responsed {
		return 1
	} else if msg.I >= 0 {
		msg.I = -1
	}
	switch reply.TaskType {
	case "wait":
		time.Sleep(3 * time.Second)
	case "map":
		doMap(mapf, reply.Filename, reply.R, reply.X)
		msg.I = reply.X + reply.Y
	case "reduce":
		doReduce(reducef, reply.M, reply.Y)
		msg.I = reply.X + reply.Y
	}
	return 0
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	msg := Message{-1}
	reply := Reply{}

	for {
		i := heartBeat(mapf, reducef, &msg, &reply)
		if i == 1 {
			break
		}
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
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
