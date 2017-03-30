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

const timeoutMax = 500 // 300
const timeoutMin = 250 // 150
const heartbeatInterval = 100

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

	// follow paper's Figure 2
	currentTerm int
	votedFor    int
	log         []interface{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// 自己添加的
	state int

	r       *rand.Rand
	timer   int
	timeout int
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
	// follow paper's Figure 2
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	// follow paper's Figure 2
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	fmt.Println("IM", rf.me, rf.votedFor, rf.currentTerm, args, reply)

	// args.Term == rf.currentTerm ??
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		go rf.tobeFollower()

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}

	fmt.Println("IM", rf.me, rf.votedFor, rf.currentTerm, args, reply)
	return
}

type AppendEntriesArgs struct {
	// follow paper's Figure 2
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	// follow paper's Figure 2
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
		go rf.tobeFollower()
	} else { // ==
		rf.resetTimeout()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

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
	fmt.Println(rf.me, "sendRequestVote to ", server, "begin")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Println(rf.me, "sendRequestVote to ", server, "end")
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

	if rf.state != leader {
		return -1, -1, false
	}

	index := -1
	term := -1
	isLeader := true

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

func (rf *Raft) tobeFollower() {
	rf.state = follower
	rf.resetTimeout()
}

func (rf *Raft) tobeCandidate() {
	fmt.Println("IM", rf.me, "tobe candidate")
	rf.resetTimeout()

	// reset status and send vote request
	rf.currentTerm++
	candidateTerm := rf.currentTerm
	rf.state = candidate
	rf.votedFor = rf.me

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	// args.lastLogIndex = rf.lastApplied
	// args.lastLogTerm = rf.currentTerm // TODO

	reply := RequestVoteReply{}

	results := make([]bool, len(rf.peers))
	for i := 0; i < len(results); i++ {
		if i == rf.me {
			results[i] = true
		} else {
			results[i] = false
		}
	}
	resultCh := make(chan struct{})

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				ok := rf.sendRequestVote(index, args, &reply)
				// e.Call("Raft.RequestVote", args, &reply)
				if ok {
					// TODO 还需要其他处理吗
					if reply.VoteGranted == false {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
						}
					} else {
						// TODO: 需要锁吗
						results[index] = true
					}
					// TODO 这个协程可能一直阻塞在这里，会被回收吗
					resultCh <- struct{}{}
				}
			}(i)
		}
	}

	// TODO: 这里的资源怎么回收，需要考虑吗？
	for {
		<-resultCh
		voteCount := 0
		for i := 0; i < len(results); i++ {
			if results[i] == true {
				voteCount += 1
			}
		}
		// TODO: 没有判断返回结果半数失败的情况，这种情况只能等超时重新选举
		if voteCount*2 > len(rf.peers) && rf.currentTerm == candidateTerm {
			go rf.tobeLeader()
			// 这里没接收信号，回复不了，怎么办
			// close(resultCh) // TODO: 还是要记录所有投票的结果的
			return
		}
	}
}

func (rf *Raft) tobeLeader() {
	fmt.Println("IM", rf.me, "tobe leader")
	rf.state = leader
	rf.sendHeartbeat()
}

func (rf *Raft) sendHeartbeat() {

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me

	reply := AppendEntriesReply{}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				// TODO: 需要记录结果吗？
				rf.peers[index].Call("Raft.AppendEntries", args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) resetTimeout() {
	rf.timeout = rf.r.Intn(timeoutMax)
	for rf.timeout <= timeoutMin {
		rf.timeout = rf.r.Intn(timeoutMax)
	}
	fmt.Println(rf.me, "reset timeout", rf.timeout)
	rf.timer = 0
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.state = follower

	// generate timeout message
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.resetTimeout()

	// 超时检测
	go func() {
		for {
			time.Sleep(time.Millisecond)
			if rf.state == follower || rf.state == candidate {
				// follower 没有收到心跳包超时，candidate 发起一轮选举也会超时
				// candidate 在选举过程超时，也会发起新一轮选举
				rf.timer++
				if rf.timer >= rf.timeout {
					// TODO: 发起的选举超时怎么退出？
					go rf.tobeCandidate()
				}
			}
		}
	}()

	// leader 定时发心跳包
	go func() {
		for {
			time.Sleep(time.Millisecond * heartbeatInterval)
			if rf.state == leader {
				// 向所有 peer 发送心跳包
				go rf.sendHeartbeat()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
