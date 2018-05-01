package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append" or "Get"

	// duplicate detection
	ClerkID int64
	ReqID   int64
}

type noticeChKey struct {
	index int
	term  int
}

type noticeChValue struct {
	succ  bool
	value string
	err   Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string
	noticeChs map[noticeChKey]chan noticeChValue // TODO release, nil
	dplDtc    map[int64]int64                    // duplicate detection
}

type persistKV struct {
	Store  map[string]string
	DplDtc map[int64]int64
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server Get function] %+v", args)
	index, term, isLeader := kv.rf.Start(Op{args.Key, "", "Get", args.ClerkID, args.ReqID})
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	rCh := make(chan noticeChValue, 1) // 非同步 chan
	kv.noticeChs[noticeChKey{index, term}] = rCh
	select {
	case r := <-rCh:
		DPrintf("[Server Get rCh] %+v", r)
		if r.succ == true {
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = r.value
		} else {
			reply.WrongLeader = false
			reply.Err = r.err
		}
	case <-time.After(time.Millisecond * 1000):
		reply.Err = "timeout" // TODO: timeout 时间多少比较合适
	}
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[Server PutAppend function] %+v", args)
	index, term, isLeader := kv.rf.Start(Op{args.Key, args.Value, args.Op, args.ClerkID, args.ReqID})
	if isLeader == false {
		reply.WrongLeader = true
		// reply.Err = nil
		return
	}
	rCh := make(chan noticeChValue, 1) // 非同步 chan
	kv.noticeChs[noticeChKey{index, term}] = rCh
	select {
	case r := <-rCh:
		DPrintf("[Server PutAppend rCh] %+v", r)
		if r.succ == true {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = false
			reply.Err = r.err
		}
	case <-time.After(time.Millisecond * 1000): // TODO 什么情况下会发生？start 的 log 没有 commit
		reply.Err = "timeout" // TODO: timeout 时间多少比较合适
	}
	return
}

func (kv *RaftKV) recvApplyMsg() {
	for applyMsg := range kv.applyCh {
		kv.mu.Lock() // need ? TODO
		DPrintf("[Server recvApplyMsg] %p %+v %+v", kv.rf, applyMsg, applyMsg.Command)
		commandValid := applyMsg.CommandValid
		if commandValid == true {
			command := applyMsg.Command
			index := applyMsg.CommandIndex

			term, _ := kv.rf.GetState()
			if op, ok := command.(Op); ok {
				reqID := kv.dplDtc[op.ClerkID]
				// 这样简单处理有问题吗？依赖顺序 TODO
				// && reqID cycle? 0
				dup := false
				if reqID >= op.ReqID {
					dup = true
				} else { // reqID < op.ReqID
					kv.dplDtc[op.ClerkID] = op.ReqID
				}

				switch op.Op {
				case "Put":
					if dup != true {
						kv.store[op.Key] = op.Value
					}
					key := noticeChKey{index, term}
					if kv.noticeChs[key] != nil {
						if dup != true {
							kv.noticeChs[key] <- noticeChValue{true, "", OK}
						} else {
							kv.noticeChs[key] <- noticeChValue{true, "", Dup}
						}
						close(kv.noticeChs[key])
					}
				case "Append":
					if dup != true {
						if original, ok := kv.store[op.Key]; ok {
							kv.store[op.Key] = original + op.Value
						} else {
							kv.store[op.Key] = op.Value
						}
					}
					key := noticeChKey{index, term}
					if kv.noticeChs[key] != nil {
						if dup != true {
							kv.noticeChs[key] <- noticeChValue{true, "", OK}
						} else {
							kv.noticeChs[key] <- noticeChValue{true, "", Dup}
						}
						close(kv.noticeChs[key])
					}
				case "Get":
					key := noticeChKey{index, term}
					if kv.noticeChs[key] != nil {
						if value, ok := kv.store[op.Key]; ok {
							kv.noticeChs[key] <- noticeChValue{true, value, OK}
						} else {
							kv.noticeChs[key] <- noticeChValue{true, value, ErrNoKey}
						}
						close(kv.noticeChs[key])
					}
				default:
					DPrintf("xxxxxxxxx err op %v", op)
					// error
				}
			} else {
				DPrintf("xxxxxxxxx err op %v", op)
			}

			// 判断是否需要 snapshot
			if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
				// fmt.Println(kv.rf.RaftStateSize(), "xxxxxxxxxx", index)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(persistKV{
					Store:  kv.store,
					DplDtc: kv.dplDtc,
				})
				data := w.Bytes()
				kv.rf.SaveSnapshot(data, index)
			}

		} else {
			// 接收 snapshot 消息，状态机更改
			data := applyMsg.Snapshot
			if data == nil || len(data) < 1 {
				kv.mu.Unlock()
				// TODO some error
				return
			}
			r := bytes.NewBuffer(data)
			d := labgob.NewDecoder(r)
			var snapshot persistKV
			if err := d.Decode(&snapshot); err != nil {
				panic(err) // panic is ok ? TODO
			} else {
				// DPrintf(persistLog, "[me]%d(%p) [readPersist]%+v\n", rf.me, rf, pRf)
				kv.store = snapshot.Store
				kv.dplDtc = snapshot.DplDtc
			}
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.noticeChs = make(map[noticeChKey]chan noticeChValue)
	kv.dplDtc = make(map[int64]int64)

	kv.readPersist(persister.ReadRaftState())

	go kv.recvApplyMsg()

	return kv
}

type persistRaft struct {
	Log         []raft.Entry
	CommitIndex int
}

// TODO 通过读取 snapshot 重启
func (kv *RaftKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pRf persistRaft
	if err := d.Decode(&pRf); err != nil {
		panic(err)
	} else {
		DPrintf("[kv raft readPersist] %+v\n", pRf)
		log := pRf.Log
		for i := 1; i <= pRf.CommitIndex; i++ {
			entry := log[i]
			command := entry.Command
			if op, ok := command.(Op); ok {
				reqID := kv.dplDtc[op.ClerkID]
				dup := false
				if reqID >= op.ReqID {
					dup = true
				} else { // reqID < op.ReqID
					kv.dplDtc[op.ClerkID] = op.ReqID
				}

				switch op.Op {
				case "Put":
					if dup != true {
						kv.store[op.Key] = op.Value
					}
				case "Append":
					if dup != true {
						if original, ok := kv.store[op.Key]; ok {
							kv.store[op.Key] = original + op.Value
						} else {
							kv.store[op.Key] = op.Value
						}
					}
				default:
					DPrintf("xxxxxxxxx err op %v", op)
					// error
				}
			} else {
				DPrintf("xxxxxxxxx err op %v", op)
			}
		}
	}
}

/*

append(key, value)

leader loss leadership

wait

									newleader

									get(key)
timeout

clerk retry



这种场景下，get 跟 append ,谁在 raft log 前面

*/
