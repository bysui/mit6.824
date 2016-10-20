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

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	"labrpc"
	//"log"
	"math/rand"
	//"os"
	//"strconv"
	"sync"
	"time"
)

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
	STOPPED      = "stopped"
	INITIALIZED  = "initialized"
	FOLLOWER     = "follower"
	CANDIDATE    = "candidate"
	LEADER       = "leader"
	SNAPSHOTTING = "snapshotting"
)

const (
	HeartbeatCycle  = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

//
// log entry contains command for state machine, and term when entry
// was received by leader
//
type LogEntry struct {
	Command interface{}
	Term    int
}

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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server

	// granted vote number
	granted_votes_count int

	// State and applyMsg chan
	state   string
	applyCh chan ApplyMsg

	// Log and Timer
	//logger *log.Logger
	timer *time.Timer
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == LEADER)

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
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	rf.persister.SaveRaftState(buf.Bytes())
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
	if data != nil {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		dec.Decode(&rf.currentTerm)
		dec.Decode(&rf.votedFor)
		dec.Decode(&rf.logs)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true

	// current server's log must newer than the candidate
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm &&
				len(rf.logs)-1 > args.LastLogIndex) {
			may_grant_vote = false
		}
	}

	// current server's current term bigger than the candidate
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// current server's current term same as the candidate
	if args.Term == rf.currentTerm {
		// no voted candidate
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		//if reply.VoteGranted {
		//	rf.logger.Printf("Vote for server[%v] at Term:%v\n", rf.me, args.CandidateId, rf.currentTerm)
		//}
		return
	}

	// current server's current term smaller than the candidate
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()

		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		//if reply.VoteGranted {
		//	rf.logger.Printf("Vote for server[%v] at Term:%v\n", rf.me, args.CandidateId, rf.currentTerm)
		//}
		return
	}
}

//
// majority func
//
func majority(n int) int {
	return n/2 + 1
}

//
// handle vote result
//
func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// old term ignore
	if reply.Term < rf.currentTerm {
		return
	}

	// newer reply item push peer to be follwer again
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.granted_votes_count += 1
		if rf.granted_votes_count >= majority(len(rf.peers)) {
			rf.state = LEADER
			// rf.logger.Printf("Leader at Term:%v log_len:%v\n", rf.me, rf.currentTerm, len(rf.logs))
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer()
		}
		return
	}
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
// example AppendEntry RPC arguments structure.
//
type AppendEntryArgs struct {
	Term         int
	Leader_id    int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntry RPC reply structure.
//
type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

//
// append entries
//
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// rf.logger.Printf("Args term:%v less than currentTerm:%v drop it", args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term
		// Since at first, leader communicates with followers,
		// nextIndex[server] value equal to len(leader.logs)
		// so system need to find the matching term and index
		if args.PrevLogIndex >= 0 &&
			(len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			// rf.logger.Printf("Match failed %v\n", args)
			reply.CommitIndex = len(rf.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}
			for reply.CommitIndex >= 0 {
				if rf.logs[reply.CommitIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIndex--
			}
			reply.Success = false
		} else if args.Entries != nil {
			// If an existing entry conflicts with a new one (Entry with same index but different terms)
			// delete the existing entry and all that follow it
			// reply.CommitIndex is the fucking guy stand for server's log size
			// rf.logger.Printf("Appending %v logs from %v\n", len(args.Entries), args.PrevLogIndex)
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs()
			}
			reply.CommitIndex = len(rf.logs) - 1
			reply.Success = true
		} else {
			// rf.logger.Printf("Heartbeat...\n")
			if len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs()
			}
			reply.CommitIndex = args.PrevLogIndex
			reply.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
}

//
// send appendetries to a follower
//
func (rf *Raft) SendAppendEntryToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// send appendetries to all follwer
//
func (rf *Raft) SendAppendEntriesToAllFollwer() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs
		args.Term = rf.currentTerm
		args.Leader_id = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1
		// rf.logger.Printf("prevLogIndx:%v logs_term:%v", args.PrevLogIndex, len(rf.logs))
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		if rf.nextIndex[i] < len(rf.logs) {
			args.Entries = rf.logs[rf.nextIndex[i]:]
		}
		args.LeaderCommit = rf.commitIndex

		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := rf.SendAppendEntryToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

//
// Handle AppendEntry result
//
func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf.logger.Printf("Got append entries result: %v\n", reply)

	if rf.state != LEADER {
		// rf.logger.Printf("Lose leader\n")
		return
	}

	// Leader should degenerate to Follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		reply_count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				reply_count += 1
			}
		}
		if reply_count >= majority(len(rf.peers)) &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
			// rf.logger.Printf("Update commit index to %v\n", rf.matchIndex[server])
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	} else {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.SendAppendEntriesToAllFollwer()
	}
}

//
// commit log is send ApplyMsg(a kind of redo log) to applyCh
//
func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.logs)-1 {
		rf.commitIndex = len(rf.logs) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		// rf.logger.Printf("Applying cmd %v\t%v\n", i, rf.logs[i].Command)
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false
	nlog := LogEntry{command, rf.currentTerm}

	if rf.state != LEADER {
		return index, term, isLeader
	}

	isLeader = (rf.state == LEADER)
	rf.logs = append(rf.logs, nlog)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()
	// rf.logger.Printf("New command:%v at term:%v\n", index, rf.currentTerm)

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
// when peer timeout, it changes to be a candidate and sendRequwstVote.
//
func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.granted_votes_count = 1
		rf.persist()
		// rf.logger.Printf("New election, Candidate:%v term:%v\n", rf.me, rf.currentTerm)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
		}

		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}

		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}

			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					rf.handleVoteResult(reply)
				}
			}(server, args)

		}
	} else {
		rf.SendAppendEntriesToAllFollwer()
	}
	rf.resetTimer()
}

//
// LEADER(HeartBeat):50ms
// FOLLOWER:150~300
//
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	new_timeout := HeartbeatCycle
	if rf.state != LEADER {
		new_timeout = time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	rf.timer.Reset(new_timeout)
	// rf.logger.Printf("Resetting timeout to %v\n", new_timeout)
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	/* file, err := os.Create("log" + strconv.Itoa(me) + ".txt")
	if err != nil {
		log.Fatal("failed to create log.txt")
	}
	rf.logger = log.New(file, fmt.Sprintf("[Server %v]", me), log.LstdFlags)
	*/
	rf.persist()
	rf.resetTimer()

	return rf
}
