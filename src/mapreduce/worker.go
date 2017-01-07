package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name   string
	Map    func(string, string) []KeyValue
	Reduce func(string, []string) string
	nRPC   int // protected by mutex
	nTasks int // protected by mutex
	l      net.Listener
}

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)
	}

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	// TODO: 正常逻辑下，shutdown 被调用，除非再调用一次 RPC
	// 否则 wk.listen 还是一直 open 的. 所以我在下面添加了 close 的逻辑
	wk.nRPC = 1
	wk.nTasks--  // Don't count the shutdown RPC
	wk.l.Close() // causes the Accept to fail // add by gjt
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) error {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		return fmt.Errorf("Register: RPC %s register error\n", master)
	}
	return nil
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int,
) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	e = wk.register(MasterAddress)
	if e != nil {
		// 注册失败的话，也没有必要往下走了
		fmt.Println("RunWorker: worker ", me, " error: ", e)
		return
	}

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.l.Close() // add by gjt
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
			wk.Lock()
			wk.nTasks++
			wk.Unlock()
		} else {
			break
		}
	}
	// TODO: 正常逻辑下，shutdown 被调用，除非再调用一次 RPC
	// 否则走不到这里，wk.listen 还是一直 open 的
	// wk.l.Close() // del by gjt
	debug("RunWorker %s exit\n", me)
}
