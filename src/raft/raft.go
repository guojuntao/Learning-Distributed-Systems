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

import "sync"
import "time"
import "labrpc"
import "math/rand"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Term  int
	Entry interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int // 我觉得可以去掉？

	state          State
	tobeFollowerCh chan struct{}
	// commandCh      chan interface{}
	getRandTime func() time.Duration

	apply func(int, int)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm &&
			args.CandidateId != rf.votedFor &&
			rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.tobeFollowerCh <- struct{}{} // TODO: 会被阻塞吗?

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// follow paper's Figure 2
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	// Entries      []interface{}
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	// follow paper's Figure 2
	Term       int
	Success    bool
	MatchIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term == rf.currentTerm && args.LeaderID != rf.votedFor
	// then 不改变 votedFor, 同时返回 success = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.tobeFollowerCh <- struct{}{}

	reply.MatchIndex = -1
	// index 索引从 1 还是 0 开始, 1
	// lastApplied == len(rf.log), len(rf.log) >= lastApplied
	if len(rf.log) > args.PrevLogIndex && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
		rf.lastApplied = args.PrevLogIndex
		for _, entry := range args.Entries {
			rf.lastApplied = rf.lastApplied + 1
			rf.log[rf.lastApplied] = entry // Log{args.Term, entry}
		}
		reply.MatchIndex = rf.lastApplied
		if rf.commitIndex < args.LeaderCommit { // TODO: 需要判断吗，如果小于会怎样
			oldIndex := rf.commitIndex
			rf.commitIndex = args.LeaderCommit
			go rf.apply(oldIndex, rf.commitIndex)
		}

	}

	reply.Term = rf.currentTerm
	reply.Success = true

	DPrintln("AppendEntries", rf.me, args, reply)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		isLeader = false
	} else {
		rf.lastApplied = rf.lastApplied + 1
		// or append
		rf.log[rf.lastApplied] = Log{rf.currentTerm, command}

		index = rf.lastApplied
		term = rf.currentTerm
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0 // TODO: or 1 ?
	rf.votedFor = -1
	rf.log = make([]Log, 1000) // should be init > 0
	rf.log = append(rf.log, Log{0, nil})
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = FOLLOWER
	rf.getRandTime = randTimeBuilder()
	rf.tobeFollowerCh = make(chan struct{})
	// rf.commandCh = make(chan interface{})

	rf.apply = func(oldIndex int, newIndex int) {
		for i := oldIndex + 1; i <= newIndex; i++ {
			applyCh <- ApplyMsg{true, rf.log[i].Entry, i}
		}
	}

	go func() {
		// state machine
		for {
			switch rf.state {
			case FOLLOWER:
				rf.enterFollowerState()
			case CANDIDATE:
				rf.enterCandidateState()
			case LEADER:
				rf.enterLeaderState()
			default:
				panic("error state")
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randTimeBuilder() func() time.Duration {

	const timeoutMax = 500 // 300
	const timeoutMin = 250 // 150

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return func() time.Duration {
		timeout := r.Intn(timeoutMax)
		for timeout <= timeoutMin {
			timeout = r.Intn(timeoutMax)
		}
		return time.Duration(timeout) * time.Millisecond
	}
}

func (rf *Raft) enterFollowerState() {
	timer := time.NewTimer(rf.getRandTime())
	for {
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.mu.Unlock()
			return
		case <-rf.tobeFollowerCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(rf.getRandTime())
		}
	}
}

func (rf *Raft) enterCandidateState() {

	voteCh := make(chan RequestVoteReply, len(rf.peers))

	votedCounter := 1
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			go func(index int) {
				// if not ok, retry, until enter other state
				if ok := rf.sendRequestVote(index, &args, &reply); ok {
					voteCh <- reply
				}
			}(i)
		}
	}

	timer := time.NewTimer(rf.getRandTime())
	for {
		select {
		case <-timer.C:
			// TODO: need close voteCh?
			return
		case <-rf.tobeFollowerCh:
			rf.mu.Lock()
			rf.state = FOLLOWER
			rf.mu.Unlock()
			// TODO: need stop timer/ close voteCh?
			return
		case reply := <-voteCh:
			if reply.VoteGranted == false {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					rf.mu.Unlock()
					// TODO: need stop timer/ close voteCh?
					return
				}
				rf.mu.Unlock()
			} else {
				votedCounter = votedCounter + 1
				if votedCounter >= len(rf.peers)/2+1 {
					rf.mu.Lock()
					rf.state = LEADER
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

func (rf *Raft) enterLeaderState() {
	const heartbeatInterval = 100 * time.Millisecond
	ticker := time.NewTicker(heartbeatInterval)

	// init rf.nextIndex[]
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastApplied + 1
		rf.matchIndex[i] = 0 // or -1 ?
	}

	for {
		select {
		case <-ticker.C:
			// TODO: 执行到一半的时候，收到更高 term 的请求，怎么处理!!!
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(index int) {
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term:     rf.currentTerm,
							LeaderID: rf.me,
						}
						if rf.lastApplied+1 > rf.nextIndex[index] {
							//for j= rf.nextIndex[index]; j < rf.lastApplied+1; j++ {
							args.Entries = make([]Log, rf.lastApplied+1-rf.nextIndex[index])
							args.Entries = rf.log[rf.nextIndex[index] : rf.lastApplied+1]
						}
						// if nextIndex[index] == 0
						args.PrevLogIndex = rf.nextIndex[index] - 1
						args.PrevLogTerm = rf.log[rf.nextIndex[index]-1].Term
						args.LeaderCommit = rf.commitIndex
						rf.mu.Unlock()

						reply := AppendEntriesReply{}

						// TODO: if return false value, retry?
						if ok := rf.sendAppendEntries(index, &args, &reply); ok {
							rf.mu.Lock()
							if rf.state == LEADER {
								if reply.Success == false {
									if reply.Term > rf.currentTerm {
										// TODO: tobeFollowerCh 有三个 <- , 可能同时触发吗？
										rf.currentTerm = reply.Term
										rf.votedFor = -1
										rf.tobeFollowerCh <- struct{}{}
									}
								} else {
									rf.nextIndex[index] = reply.MatchIndex + 1
									rf.matchIndex[index] = reply.MatchIndex
									var cnt int
									for _, idx := range rf.matchIndex {
										if idx >= reply.MatchIndex {
											cnt = cnt + 1
										}
									}
									// need reply.MatchIndex > rf.commitIndex ??
									if cnt == len(rf.peers)/2+1 && reply.MatchIndex > rf.commitIndex {
										oldIndex := rf.commitIndex
										rf.commitIndex = reply.MatchIndex
										go rf.apply(oldIndex, rf.commitIndex)
									}
								}
							}
							rf.mu.Unlock()
						}
					}(i)
				} else {
					rf.nextIndex[i] = rf.lastApplied + 1
					rf.matchIndex[i] = rf.lastApplied
				}
			}
		// case command := <- rf.commandCh:
		case <-rf.tobeFollowerCh:
			rf.state = FOLLOWER
			// TODO: need stop ticker?
			ticker.Stop()
			return
		}
	}
}
