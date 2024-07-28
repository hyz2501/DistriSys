package mr

import (
	"container/heap"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Coordinator struct {
	// Your definitions here.
	master_channels []chan Reply
	worker_channels []chan int
	m               int
	r               int
	mu              sync.Mutex
	task_pq         IntHeap
	completed       bool
}

func (c *Coordinator) Response(msg *Message, reply *Reply) error {
	if msg.I >= 0 {
		select {
		case c.worker_channels[msg.I] <- 1:
		case <-time.After(time.Second):
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.task_pq) == 0 {
		reply.TaskType = "wait"
	} else {
		i := heap.Pop(&c.task_pq)
		select {
		case <-time.After(2 * time.Second):
			select {
			case _, ok := <-c.master_channels[i.(int)]:
				if !ok {
					return nil
				}
			case <-time.After(1 * time.Second):
				heap.Push(&c.task_pq, i)
			}
			reply.TaskType = "wait"
		case c.worker_channels[i.(int)] <- 0:
			reply_val := <-c.master_channels[i.(int)]
			*reply = reply_val
		}
	}
	return nil

}

func (c *Coordinator) monitorMap(task_i int, filename string, master_ch chan<- Reply, worker_ch <-chan int) {
	rpl := Reply{"map", filename, c.m, c.r, task_i, 0}
	for {
		i := <-worker_ch
		if i == 1 {
			close(master_ch)
			break
		}
		master_ch <- rpl
		select {
		case i = <-worker_ch:
		case <-time.After(10 * time.Second):
			c.mu.Lock()
			heap.Push(&c.task_pq, task_i)
			c.mu.Unlock()
		} 
		// 这里等待10秒时并不会通知对应的worker停止job，相反在下一次循环里，如果monitor线程监测到job完成，
		// 并不会考虑是否是由“失踪”的那个worker完成的，而是一旦监测到完成，就会停止。这也就导致有可能有多
		// 个worker同时执行该job，即主线程把疑似卡住的job发给新worker，而失踪的worker其实还在继续执行。
		// 可能的解决方案：传递reply时加入时间戳信息，worker从而知道当前任务的deadline，如果未完成，自动abort
		if i == 1 {
			close(master_ch)
			break
		}
	}
}

func (c *Coordinator) monitorReduce(task_i int, master_ch chan<- Reply, worker_ch <-chan int) {
	rpl := Reply{"reduce", "", c.m, c.r, c.m, task_i - c.m}
	for {
		i := <-worker_ch
		if i == 1 {
			close(master_ch)
			break
		}
		master_ch <- rpl
		select {
		case i = <-worker_ch:
		case <-time.After(10 * time.Second):
			c.mu.Lock()
			heap.Push(&c.task_pq, task_i)
			c.mu.Unlock()
		}
		if i == 1 {
			close(master_ch)
			break
		}
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.m = len(files)
	c.r = nReduce
	c.completed = false
	arr := make([]int, c.m+c.r)
	for i := 0; i < c.m+c.r; i++ {
		arr[i] = i
	}
	c.task_pq = arr
	heap.Init(&c.task_pq)
	// 开M+R个channel
	c.worker_channels = make([]chan int, len(files)+nReduce)
	c.master_channels = make([]chan Reply, len(files)+nReduce)
	// 初始化这些 channel
	for i := range c.worker_channels {
		c.worker_channels[i] = make(chan int)
		c.master_channels[i] = make(chan Reply)
	}
	c.server()
	var map_wg sync.WaitGroup
	map_wg.Add(len(files))
	// 开n个线程，每一个线程通过channel监控一个worker，监控该task是否完成
	for i := 0; i < len(files); i++ {
		go func(id int) {
			defer map_wg.Done()
			c.monitorMap(id, files[id], c.master_channels[id], c.worker_channels[id])
		}(i)
	}
	map_wg.Wait()

	var reduce_wg sync.WaitGroup
	reduce_wg.Add(nReduce)
	for i := 0; i < nReduce; i++ {
		go func(id int) {
			defer reduce_wg.Done()
			c.monitorReduce(id, c.master_channels[id], c.worker_channels[id])
		}(i + len(files))
	}
	reduce_wg.Wait()
	c.completed = true
	return &c
}
