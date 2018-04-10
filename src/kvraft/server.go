package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	noticeChs map[noticeChKey]chan noticeChValue // TODO release
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{args.Key, "", "Get"})
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	rCh := make(chan noticeChValue, 1) // 非同步 chan
	kv.noticeChs[noticeChKey{index, term}] = rCh
	r := <-rCh
	DPrintf("Get %+v", r)
	if r.succ == true {
		reply.WrongLeader = false
		reply.Err = OK
		reply.Value = r.value
	} else {
		reply.WrongLeader = false
		reply.Err = r.err
	}
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{args.Key, args.Value, args.Op})
	if isLeader == false {
		reply.WrongLeader = true
		// reply.Err = nil
		return
	}
	rCh := make(chan noticeChValue, 1) // 非同步 chan
	kv.noticeChs[noticeChKey{index, term}] = rCh
	r := <-rCh
	DPrintf("PutAppend %+v", r)
	if r.succ == true {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = false
		reply.Err = r.err
	}
	return
}

func (kv *RaftKV) recvApplyMsg() {
	for {
		applyMsg := <-kv.applyCh
		DPrintf("%+v", applyMsg)
		// applyMsg.CommandValid
		command := applyMsg.Command
		index := applyMsg.CommandIndex

		kv.mu.Lock()
		term, _ := kv.rf.GetState()
		if op, ok := command.(Op); ok {
			switch op.Op {
			case "Put":
				kv.store[op.Key] = op.Value
				key := noticeChKey{index, term}
				kv.noticeChs[key] <- noticeChValue{true, "", OK}
			case "Append":
				if original, ok := kv.store[op.Key]; ok {
					kv.store[op.Key] = original + op.Value
				} else {
					kv.store[op.Key] = op.Value
				}
				key := noticeChKey{index, term}
				kv.noticeChs[key] <- noticeChValue{true, "", OK}
			case "Get":
				key := noticeChKey{index, term}
				if value, ok := kv.store[op.Key]; ok {
					kv.noticeChs[key] <- noticeChValue{true, value, OK}
				} else {
					kv.noticeChs[key] <- noticeChValue{true, value, ErrNoKey}
				}
			default:
				// error
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

	go kv.recvApplyMsg()

	return kv
}
