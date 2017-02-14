package mapreduce

import "fmt"

const (
	willDo = iota
	doing
	done
)

const (
	waitDoing = -1
	allDone   = -2
)

type chanMessage struct {
	worker     string
	taskNumber int
	status     int
}

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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	workerDoneChannel := make(chan chanMessage)
	tasksDoneStatus := make([]int, ntasks)

	args := new(DoTaskArgs)
	args.JobName = mr.jobName
	args.Phase = phase
	args.NumOtherPhase = nios

	var worker string

	// schedule 的主体不并发，控制复杂度，只有在调用 schedule_worker 才使用 go
	// 给现存的 worker 分配任务
	for _, worker = range mr.workers {
		args.TaskNumber = getTaskNumber(tasksDoneStatus)
		if args.TaskNumber == waitDoing || args.TaskNumber == allDone {
			break
		}
		args.File = mr.files[args.TaskNumber]
		go schedule_worker(worker, *args, workerDoneChannel)
	}

	for {
		// get idle worker
		select {
		case worker = <-mr.registerChannel:
		case result := <-workerDoneChannel:
			worker = result.worker
			tasksDoneStatus[result.taskNumber] = result.status
			if result.status == willDo {
				// TODO: one failed, never use worker ?
				// should allow somtimes failed
				// 或者失败后应该调用 shutdown 去杀死 worker
				continue
			}
		}

		args.TaskNumber = getTaskNumber(tasksDoneStatus)
		if args.TaskNumber == allDone {
			break
		} else if args.TaskNumber == waitDoing {
			continue
		}

		// 分配新的任务给完成上一任务的worker
		args.File = mr.files[args.TaskNumber]
		go schedule_worker(worker, *args, workerDoneChannel)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

// TODO: 每次都从 0 遍历到 len(s)，性能太差。可以考虑添加 doneNumber
// 每次从 doneNumber 遍历到 len(s), 如果没有 failures，每次只需遍历几个
func getTaskNumber(s []int) int {
	doingFlag := false
	for i := 0; i < len(s); i++ {
		if s[i] == willDo {
			s[i] = doing
			return i
		} else if s[i] == doing {
			doingFlag = true
		}
	}
	if doingFlag == true {
		return waitDoing
	} else {
		return allDone
	}
}

// args 必须使用引用而不是指针，防止并发时相互影响
// add comment 20170214 为什么要使用指针？
func schedule_worker(worker string, args DoTaskArgs, c chan chanMessage) {
	ok := call(worker, "Worker.DoTask", args, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", worker)
		c <- chanMessage{worker, args.TaskNumber, willDo}
	} else {
		c <- chanMessage{worker, args.TaskNumber, done}
	}
}
