package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	taskChannel := make(chan int, ntasks)
	go func() {
		for i:=0; i<ntasks; i++ {
			taskChannel <- i
		}
	}()
	var mutex = &sync.Mutex{}
	for {
		taskNum, more := <-taskChannel
		if !more {
			break
		}
		worker := <- mr.registerChannel
		go func() {
			args := new(DoTaskArgs)
			args.File = mr.files[taskNum]
			args.JobName = mr.jobName
			args.Phase = phase
			args.TaskNumber = taskNum
			args.NumOtherPhase = nios
			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if ok == false {
				taskChannel <- taskNum
			} else {
				mutex.Lock()
				ntasks--
				if ntasks == 0 {
					close(taskChannel)
				}
				mutex.Unlock()
			}
			mr.registerChannel <- worker
		} ()
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
