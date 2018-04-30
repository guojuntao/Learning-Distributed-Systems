package raft

// TODO list
// 3. stop timer & close voteCh
// 4. 日志还需要进一步整理
// 5. TestFigure8Unreliable2C 还没通过
// 7. rf.lastApplied 理解错了，修正！！！

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
import "time"
import "labrpc"
import "math/rand"
import "bytes"
import "labgob"

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

type state int

const (
	FOLLOWER state = iota
	CANDIDATE
	LEADER
	KILLED // for the test to add
)

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// from paper
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int

	// add by myself
	state          state
	tobeFollowerCh chan struct{}
	killCh         chan struct{}
	getRandTime    func() time.Duration
	apply          func(int, int)

	// 防止 sendAppendEntriesWrapper 重入
	sendingAppendEntries []bool
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

type persistRaft struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
	CommitIndex int
	LastApplied int

	SnapshotLastIncludedIndex int
	SnapshotLastIncludedTerm  int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	// should be called after rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(persistRaft{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
	})
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pRf persistRaft
	if err := d.Decode(&pRf); err != nil {
		panic(err)
	} else {
		rf.mu.Lock()
		DPrintf(persistLog, "[me]%d(%p) [readPersist]%+v\n", rf.me, rf, pRf)
		rf.currentTerm = pRf.CurrentTerm
		rf.votedFor = pRf.VotedFor
		rf.log = pRf.Log
		rf.commitIndex = pRf.CommitIndex
		rf.lastApplied = pRf.LastApplied
		rf.mu.Unlock()
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
	defer rf.persist()

	var falseReason string
	defer func() {
		logStr := fmt.Sprintf("[RequestVote] [me]%d(%p) [currentTerm]%d [args]%+v [reply]%+v",
			rf.me, rf, rf.currentTerm, args, reply)
		if falseReason != "" {
			logStr = logStr + fmt.Sprintf(" [falseReason !!!] %v", falseReason)
		}
		DPrintln(rpcRecvLog, logStr)
	}()

	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.snapshotLastIncludedIndex
		lastLogTerm = rf.snapshotLastIncludedTerm
	} else {
		lastLogIndex = rf.sliceIndexToLogIndex(len(rf.log) - 1)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && args.CandidateId != rf.votedFor && rf.votedFor != -1) ||
		args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			go func() {
				rf.tobeFollowerCh <- struct{}{}
			}()
		}
		reply.VoteGranted = false
		falseReason = fmt.Sprintf("1. rf.votedFor[%v] lastLogTerm[%v] lastLogIndex[%v]",
			rf.votedFor, lastLogTerm, lastLogIndex)
		reply.Term = rf.currentTerm

	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		go func() {
			rf.tobeFollowerCh <- struct{}{}
		}()

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
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// follow paper's Figure 2
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	var falseReason string
	defer func() {
		logStr := fmt.Sprintf("[AppendEntries] [me]%d(%p) [currentTerm]%d [args]%+v [reply]%+v",
			rf.me, rf, rf.currentTerm, args, reply)
		if falseReason != "" {
			logStr = logStr + fmt.Sprintf(" [falseReason !!!] %v", falseReason)
		}
		DPrintln(rpcRecvLog, logStr)
	}()

	// if args.Term == rf.currentTerm && args.LeaderID != rf.votedFor
	// then 不改变 votedFor, 同时返回 success = true，这种场景要思考一下 TODO
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		falseReason = fmt.Sprintf("1. args.Term[%v] < rf.currentTerm[%v]",
			args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	go func() {
		rf.tobeFollowerCh <- struct{}{}
	}()
	reply.Term = rf.currentTerm

	prevLogSliceIndex := rf.logIndexToSliceIndex(args.PrevLogIndex)
	if (prevLogSliceIndex >= 0) && (len(rf.log) > prevLogSliceIndex) {
		if args.PrevLogTerm == rf.log[prevLogSliceIndex].Term {
			rf.log = rf.log[:prevLogSliceIndex+1]
			rf.log = append(rf.log, args.Entries...)
			if rf.commitIndex < args.LeaderCommit { // TODO: 需要判断吗，如果小于会怎样
				oldIndex := rf.commitIndex
				rf.commitIndex = args.LeaderCommit
				go rf.apply(oldIndex, rf.commitIndex)
			}
			reply.Success = true
		} else {
			reply.Success = false
			falseReason = fmt.Sprintf(
				"2. args.PrevLogTerm[%v] < rf.log[prevLogSliceIndex].Term[%v]",
				args.PrevLogTerm, rf.log[prevLogSliceIndex].Term)
		}
	} else {
		reply.Success = false
		falseReason = fmt.Sprintf("3. prevLogSliceIndex[%v] len(rf.log)[%v]",
			prevLogSliceIndex, len(rf.log))
	}

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
	defer rf.persist() // TODO need ? 只有被提交的信息才持久化，还是所有信息持久化

	if rf.state != LEADER {
		isLeader = false
	} else {
		DPrintf(startLog, "[Start] [me]%d(%p) [currentTerm]%d [command]%v\n", rf.me, rf, rf.currentTerm, command)
		rf.log = append(rf.log, Entry{rf.currentTerm, command})

		index = rf.sliceIndexToLogIndex(len(rf.log) - 1)
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
	go func() {
		rf.killCh <- struct{}{}
	}()
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
	rf.sendingAppendEntries = make([]bool, len(rf.peers))
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// From paper
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0, 128)
	rf.log = append(rf.log, Entry{0, nil}) // init rf.log, 初始阶段需要发送 PrevLogIndex & PrevLogTerm
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.snapshotLastIncludedIndex = -1

	rf.state = FOLLOWER
	rf.tobeFollowerCh = make(chan struct{})
	rf.killCh = make(chan struct{})
	rf.getRandTime = func() func() time.Duration {
		const timeoutMax = 500 // 300
		const timeoutMin = 250 // 150
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		return func() time.Duration {
			timeout := r.Intn(timeoutMax)
			for timeout <= timeoutMin {
				timeout = r.Intn(timeoutMax)
			}
			t := time.Duration(timeout) * time.Millisecond
			DPrintln(getRandTimeLog, "[getRandTime]", t)
			return t
		}
	}()
	rf.apply = func(oldIndex int, newIndex int) {
		rf.mu.Lock()

		oldIndex = rf.logIndexToSliceIndex(oldIndex)
		newIndex = rf.logIndexToSliceIndex(newIndex)

		log := make([]Entry, newIndex-oldIndex)
		copy(log, rf.log[oldIndex+1:newIndex+1])
		rf.mu.Unlock()

		for k, v := range log {
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      v.Command,
				CommandIndex: oldIndex + 1 + k,
			}
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// state machine
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				rf.enterFollowerState()
			case CANDIDATE:
				rf.enterCandidateState()
			case LEADER:
				rf.enterLeaderState()
			case KILLED:
				DPrintf(stateLog, "[enterKillState] [me]%d(%p)\n", rf.me, rf)
				return
			default:
				panic(fmt.Sprintln("Unknown state ", rf.state))
			}
		}
	}()

	return rf
}

func (rf *Raft) enterFollowerState() {
	rf.mu.Lock()
	DPrintf(stateLog, "[enterFollowerState] [me]%d(%p) [currentTerm]%d\n", rf.me, rf, rf.currentTerm)
	rf.mu.Unlock()

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
				// TODO: 好好看 timer 的 stop&reset
				<-timer.C
			}
			timer.Reset(rf.getRandTime())
		case <-rf.killCh:
			rf.mu.Lock()
			rf.state = KILLED
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) enterCandidateState() {
	rf.mu.Lock()
	DPrintf(stateLog, "[enterCandidateState] [me]%d(%p) [currentTerm]%d\n", rf.me, rf, rf.currentTerm+1)
	rf.mu.Unlock()

	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	votedCounter := 1
	rf.persist()
	rf.mu.Unlock()

	// TODO 可不可以不用 chan，用数组记录
	voteCh := make(chan RequestVoteReply, len(rf.peers))

	rf.mu.Lock()
	DPrintf(candidateSendLog, "[Candidate Send RequestVote] [me]%d(%p) [currentTerm]%d\n", rf.me, rf, rf.currentTerm)
	rf.mu.Unlock()

	rf.mu.Lock()
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.snapshotLastIncludedIndex
		lastLogTerm = rf.snapshotLastIncludedTerm
	} else {
		lastLogIndex = rf.sliceIndexToLogIndex(len(rf.log) - 1)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	length := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	for i := 0; i < length; i++ {
		if i != me {
			reply := RequestVoteReply{}
			go func(index int) {
				// TODO: if not ok, retry, until enter other state
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
					rf.persist()
					rf.mu.Unlock()
					// TODO: need stop timer/ close voteCh?
					return
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				votedCounter = votedCounter + 1
				if votedCounter >= len(rf.peers)/2+1 {
					rf.state = LEADER
					// TODO: need stop timer/ close voteCh?
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		case <-rf.killCh:
			rf.mu.Lock()
			rf.state = KILLED
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) enterLeaderState() {
	rf.mu.Lock()
	DPrintf(stateLog, "[enterLeaderState] [me]%d(%p) [currentTerm]%d\n", rf.me, rf, rf.currentTerm)

	// 在这个状态机里，这几个变量都不会改变
	length := len(rf.peers)
	me := rf.me
	currentTerm := rf.currentTerm

	for i := 0; i < length; i++ {
		rf.nextIndex[i] = rf.sliceIndexToLogIndex(len(rf.log))
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// TODO: why "the tester limits you to 10 heartbeats per second" ?
	const heartbeatInterval = 100 * time.Millisecond
	ticker := time.NewTicker(heartbeatInterval)

	for {
		select {
		case <-ticker.C:
			// TODO 这里有问题，为什么从 Start 到这里，会有 100 毫秒的延迟
			DPrintf(leaderSendLog, "[Leader Send AppendEntries] [me]%d(%p) [currentTerm]%d\n", me, rf, currentTerm)

			// TODO: 执行到一半的时候，收到更高 term 的请求，怎么处理!!!
			for i := 0; i < length; i++ {
				if i != me {
					go func(server int) {
						for {
							reply, _ := rf.sendAppendEntriesWrapper(server, currentTerm)
							if reply == false {
								break
							}
						}
					}(i)
				} else {
					rf.mu.Lock()
					rf.incMactchIndex(len(rf.log)-1, me)
					rf.mu.Unlock()
				}
			}

		case <-rf.tobeFollowerCh:
			rf.mu.Lock()
			rf.state = FOLLOWER
			rf.mu.Unlock()
			// TODO: need stop ticker?
			ticker.Stop()
			return

		case <-rf.killCh:
			rf.mu.Lock()
			rf.state = KILLED
			rf.mu.Unlock()
			// TODO: need stop ticker?
			ticker.Stop()
			return
		}
	}
}

func (rf *Raft) sendAppendEntriesWrapper(server int, currentTerm int) (retry bool, installSnapshot bool) {

	rf.mu.Lock()
	// 防止重入堆积
	if rf.sendingAppendEntries[server] == true {
		rf.mu.Unlock()
		return false, false
	}
	rf.sendingAppendEntries[server] = true

	var prevLogTerm int
	if len(rf.log) == 0 {
		prevLogTerm = rf.snapshotLastIncludedTerm
	} else {
		prevLogSliceIndex := rf.logIndexToSliceIndex(rf.nextIndex[server] - 1)
		if prevLogSliceIndex < 0 {
			// TODO
			rf.mu.Unlock()
			return false, true // TODO 什么情况会走到这里
		}
		prevLogTerm = rf.log[prevLogSliceIndex].Term
	}

	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	nextSliceIndex := rf.logIndexToSliceIndex(rf.nextIndex[server])
	if nextSliceIndex < 0 {
		rf.mu.Unlock()
		return false, true // TODO 什么情况会走到这里
	}
	lastSliceIndex := len(rf.log) - 1
	if lastSliceIndex >= nextSliceIndex {
		args.Entries = make([]Entry, lastSliceIndex+1-nextSliceIndex)
		copy(args.Entries, rf.log[nextSliceIndex:lastSliceIndex+1])
	}
	lastLogIndex := rf.sliceIndexToLogIndex(lastSliceIndex)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	if ok := rf.sendAppendEntries(server, &args, &reply); ok {
		retry, installSnapshot = rf.handleAppendEntriesReply(&reply, lastLogIndex, server)
	} else {
		retry = true
	}

	rf.mu.Lock()
	rf.sendingAppendEntries[server] = false
	rf.mu.Unlock()

	return
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply, lastLogIndex int,
	server int) (retry bool, installSnapshot bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		if reply.Success == true {
			rf.incMactchIndex(lastLogIndex, server)
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				go func() {
					rf.tobeFollowerCh <- struct{}{}
				}()
			} else {
				rf.nextIndex[server] = rf.nextIndex[server] - 1
				// if rf.nextIndex[server] > reply.LastApplied {
				// 	rf.nextIndex[server] = reply.LastApplied + 1
				// }
				nextIndex := rf.logIndexToSliceIndex(rf.nextIndex[server])
				if nextIndex < 0 {
					return false, true
				} else {
					return true, false
				}
				// if rf.nextIndex[server] < 1 { // add this for fix bug
				// 	rf.nextIndex[server] = 1
				// }
				// return true
			}
		}
	}
	return retry, installSnapshot
}

// should be called after rf.mu.Lock()
func (rf *Raft) incMactchIndex(matchIndex int, index int) {
	if rf.matchIndex[index] != matchIndex {
		rf.nextIndex[index] = matchIndex + 1
		rf.matchIndex[index] = matchIndex

		cnt := 0
		for _, idx := range rf.matchIndex {
			if idx >= matchIndex {
				cnt = cnt + 1
			}
		}

		matchSliceIndex := rf.logIndexToSliceIndex(matchIndex)
		if matchSliceIndex < 0 {
			// TODO:
		}
		if cnt == len(rf.peers)/2+1 && matchIndex > rf.commitIndex && rf.log[matchSliceIndex].Term == rf.currentTerm {
			oldIndex := rf.commitIndex
			rf.commitIndex = matchIndex
			rf.persist()
			go rf.apply(oldIndex, rf.commitIndex)
		}
	}
}

// TODO 边界
// call before rf.Lock
func (rf *Raft) logIndexToSliceIndex(logIndex int) (sliceIndex int) {
	return logIndex - rf.snapshotLastIncludedIndex - 1
}

// call before rf.Lock
func (rf *Raft) sliceIndexToLogIndex(sliceIndex int) (logIndex int) {
	return sliceIndex + rf.snapshotLastIncludedIndex + 1
}

func (rf *Raft) DiscardLog(index int) {
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) SaveSnapshot(snapshot []byte) {
	rf.persister.SaveSnapshot(snapshot)
	return
}

// TODO: 为什么 raft 死锁了，test 就会卡住
