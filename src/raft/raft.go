package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "fmt"
import "sync"
import "labrpc"
import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	follower = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []interface{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state int

	applyCh chan ApplyMsg

	// 用于超时，成为 candidate，发起选举
	// TODO: add Mutex
	r            *rand.Rand
	timeoutCount int
	timeoutMax   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
	// lastLogIndex int
	// lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// 收到对端的请求，进行回复
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm && rf.votedFor != -1 {
		reply.VoteGranted = false
	} else {
		// term 不可能等于 currentTerm ？
		// if args.lastLogIndex < rf.lastApplied {
		//	reply.voteGranted = false
		//} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.state = follower
		rf.applyCh <- ApplyMsg{}
		//}
	}

	fmt.Println("IM", rf.me, rf.votedFor, args, reply)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// PrevLogIndex int
	// PrevLogTerm  int
	Entries []interface{}
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimeout()
	return

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true // TODO: true ???

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

const timeoutMax = 100000
const heartbeat = 1000

func (rf *Raft) beCondidate() {
	fmt.Println("IM", rf.me, "timeout")
	rf.resetTimeout()

	// reset status and send vote request
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	// args.lastLogIndex = rf.lastApplied
	// args.lastLogTerm = rf.currentTerm // TODO

	reply := RequestVoteReply{}

	// TODO: 投票请求应该并发
	voteCount := 0
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.sendRequestVote(i, args, &reply)
			// e.Call("Raft.RequestVote", &args, &reply)
			if reply.VoteGranted == false {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				}
			} else {
				voteCount += 1
			}
		}
	}
	if voteCount*2 >= len(rf.peers) {
		rf.state = leader
		rf.sendHeartbeat()
	}
}

func (rf *Raft) sendHeartbeat() {

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me

	reply := AppendEntriesReply{}

	// TODO: 投票请求应该并发
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.peers[i].Call("Raft.AppendEntries", args, &reply)
		}
	}
}

func (rf *Raft) resetTimeout() {
	for rf.timeoutMax <= heartbeat {
		rf.timeoutMax = rf.r.Intn(timeoutMax)
	}
	rf.timeoutCount = 0
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.applyCh = applyCh

	// generate timeout channel
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.resetTimeout()

	// 超时检测
	go func() {
		for {
			time.Sleep(time.Microsecond) // 10 -6 second
			if rf.state == follower || rf.state == candidate {
				// follower 没有收到心跳包超时，candidate 发起一轮选举也会超时
				rf.timeoutCount++
				if rf.timeoutCount >= rf.timeoutMax {
					// 发起选举
					// 超时怎么退出？
					rf.beCondidate()
				}
			}
		}
	}()

	// leader 定时发心跳包
	go func() {
		for {
			time.Sleep(time.Microsecond * heartbeat)
			if rf.state == leader {
				// 向所有 peer 发送心跳包
				rf.sendHeartbeat()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
