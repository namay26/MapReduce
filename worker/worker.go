package worker

import (
	"MapReduce/types"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"sync"
)

type Master struct {
	NumMap         int
	NumReduce      int
	Inputs         []string
	MapStatus      []types.TaskState
	ReduceStatus   []types.TaskState
	Intermediate   map[int]map[string][]int
	Outputs        map[string]int
	Wg             *sync.WaitGroup
	taskCh         chan types.Task
	resultCh       chan MapTaskResult
	reduceResultCh chan ReduceTaskResult
	workers        []*Worker
	client         *rpc.Client
}

type Worker struct {
	ID       int
	Master   *Master
	mapFn    types.MapFunc
	reduceFn types.ReduceFunc
	quit     chan string
}

type MapTaskResult struct {
	TaskID int
	Data   []types.MapResults
	Err    error
}

type ReduceTaskResult struct {
	TaskID int
	Output map[string]int
	Err    error
}

func ihash(s string) int { // Returns an int for hasing (Picked up the function for now)
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func NewMaster(wg *sync.WaitGroup, inputs []string, numReduce int) *Master {
	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error connecting:", err)
	}
	m := &Master{
		NumMap:         len(inputs),
		NumReduce:      numReduce,
		Inputs:         inputs,
		MapStatus:      make([]types.TaskState, len(inputs)),
		ReduceStatus:   make([]types.TaskState, numReduce),
		Intermediate:   make(map[int]map[string][]int),
		Outputs:        make(map[string]int),
		Wg:             wg,
		workers:        make([]*Worker, 0),
		client:         client,
		taskCh:         make(chan types.Task, 100),
		resultCh:       make(chan MapTaskResult, 100),
		reduceResultCh: make(chan ReduceTaskResult, 100),
	}

	for i := range m.MapStatus {
		m.MapStatus[i].State = "idle"
	}
	for i := range m.ReduceStatus {
		m.ReduceStatus[i].State = "idle"
	}

	for i := 0; i < numReduce; i++ {
		m.Intermediate[i] = make(map[string][]int)
	}
	return m
}

func NewWorker(ID int, m *Master) *Worker {
	w := &Worker{
		ID:     ID,
		Master: m,
		quit:   make(chan string),
	}

	return w
}

func (m *Master) HireWorkers(amt int) {
	for i := 0; i < amt; i++ {
		m.Wg.Add(1)
		worker := NewWorker(i, m)
		worker.Start()

		m.workers = append(m.workers, worker)
	}
}

func (m *Master) Start() {
	m.HireWorkers(m.NumReduce)

	for n, i := range m.Inputs {
		single_task := &types.Task{
			Type:       types.MapTask,
			TaskID:     n,
			ChunkIndex: n,
			Data:       i,
		}
		m.taskCh <- *single_task
	}

	completed := 0
	for completed < m.NumMap {
		res := <-m.resultCh

		if res.Err != nil {
			m.taskCh <- types.Task{
				Type:   types.MapTask,
				TaskID: res.TaskID,
				Data:   m.Inputs[res.TaskID],
			}
			continue
		}

		m.partitionMapOutput(res)
		m.MapStatus[res.TaskID].State = "done"
		completed++
	}

	for r := 0; r < m.NumReduce; r++ {
		m.taskCh <- types.Task{
			Type:   types.ReduceTask,
			TaskID: r,
		}
		m.ReduceStatus[r].State = "in-progress"
	}

	completed = 0
	for completed < m.NumReduce {
		res := <-m.reduceResultCh

		if res.Err != nil {
			m.taskCh <- types.Task{
				Type:   types.ReduceTask,
				TaskID: res.TaskID,
			}
			continue
		}

		for key, value := range res.Output {
			m.Outputs[key] = value
		}

		m.ReduceStatus[res.TaskID].State = "done"
		completed++
	}

	for _, worker := range m.workers {
		worker.quit <- "stop"
	}

	close(m.taskCh)
	m.Wg.Wait()
}

func (m *Master) Cleanup() {
	if m.client != nil {
		m.client.Close()
	}
}

func (w *Worker) Start() {
	go func() {
		defer w.Master.Wg.Done()
		for {
			select {
			case <-w.quit:
				return
			case task, ok := <-w.Master.taskCh:
				if !ok {
					return
				}
				if task.Type == types.MapTask {
					MapArgs := types.MapArgs{
						Key:     fmt.Sprintf("doc-%d", task.TaskID),
						Content: task.Data,
					}

					var fmpr []types.MapResults
					err := w.Master.client.Call("Tasks.FMap", MapArgs, &fmpr)
					if err != nil {
						w.Master.resultCh <- MapTaskResult{TaskID: task.TaskID, Err: err}
						continue
					}

					res := MapTaskResult{
						TaskID: task.TaskID,
						Data:   fmpr,
						Err:    err,
					}

					w.Master.resultCh <- res
				} else if task.Type == types.ReduceTask {
					taskID := task.TaskID
					bucket := w.Master.Intermediate[taskID]

					RedArgs := &types.ReduceArgs{
						TaskID: taskID,
						List:   bucket,
					}

					var reply types.ReduceResults
					err := w.Master.client.Call("Tasks.FReduce", RedArgs, &reply)
					if err != nil {
						w.Master.reduceResultCh <- ReduceTaskResult{TaskID: task.TaskID, Err: err}
						continue
					}
					w.Master.reduceResultCh <- ReduceTaskResult{
						TaskID: taskID,
						Output: reply.Value,
						Err:    nil,
					}
				}
			}
		}
	}()
}

func (m *Master) partitionMapOutput(res MapTaskResult) {
	for _, kv := range res.Data {
		word := kv.Word
		amt := kv.Amt

		reduceID := ihash(word) % m.NumReduce
		m.Intermediate[reduceID][word] = append(m.Intermediate[reduceID][word], amt)
	}
}
