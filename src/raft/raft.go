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
	"math"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex  int
	lastApplied  int
	state        int //0 follower; 1 leader; 2 candidate
	heartBeatNum int64

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.currentTerm, rf.state == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//////fmt.Println("????", len(args.Entries), args.Entries == nil, rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Println("appendEntries, leader:", args.LeaderId, "me:", rf.me, "term:", args.Term, "curTerm:", rf.currentTerm, "entries:", args.Entries)
	//1. leader's term < follower's term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//heartBeat
	rf.currentTerm = args.Term
	if args.Entries != nil {
		//fmt.Println("appendEntries, server: ", rf.me, "leader: ", args.LeaderId, "args.Term:", args.Term, "entries:", args.Entries)
	}
	rf.state = 0
	curTime := time.Now().UnixMilli()
	rf.heartBeatNum = curTime
	rf.votedFor = -1
	reply.Term = args.Term

	if (len(rf.log) < args.PrevLogIndex+1) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		//场景3，槽位有缺失
		if len(rf.log) < args.PrevLogIndex+1 {
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex + 1 - len(rf.log)
		} else {
			//场景1、2，槽位无缺失
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex = rf.searchStartIndexByTerm(reply.XTerm)
		}
		return
	}
	//5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.Entries != nil {
		//fmt.Printf("%v received append log, args.prev: %v, len log: %v, logs: %v, args.prevTerm:%v\n", rf.me, args.PrevLogIndex, len(rf.log), rf.log, args.PrevLogTerm)
		//2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = true
		//3. an existing entry conflicts with a new one
		if len(rf.log) > args.PrevLogIndex+1 {
			if rf.log[args.PrevLogIndex+1].Term != args.Term {
				rf.log = rf.log[0 : args.PrevLogIndex+1]
				for i := 0; i < len(args.Entries); i++ {
					rf.log = append(rf.log, args.Entries[i])
				}
			}
		} else {
			//4. append any new entries not already in the log
			for i := 0; i < len(args.Entries); i++ {
				curIndex := args.PrevLogIndex + i + 1
				if curIndex < len(rf.log) {
					rf.log[curIndex] = args.Entries[i]
				} else {
					rf.log = append(rf.log, args.Entries[i])
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("%v update commitIndex, args.LeaderCommit: %v, rf.commitIndex: %v %v\n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.log))
		indexToCommit := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		rf.commit(indexToCommit)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//for ok == false {
	//	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//}
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term > rf.currentTerm {
		rf.state = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// the candidate's is at least as up-to-date as receiver's log, grant vote !!
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)-1) {
			reply.VoteGranted = true
			rf.state = 0
			rf.votedFor = args.CandidateId
			rf.heartBeatNum = time.Now().UnixMilli()
			return
		}
		////fmt.Printf("Follower %d requested vote for term %d\n", rf.me, args.Term)
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	////fmt.Printf("candidate: %v, follower: %v follower's term: %v, candidate's term: %v,  votedFor: %v\n", args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.votedFor)
}

// 通过二分法查找某一Term在log中第一次出现的位置；不存在就返回-1
func (rf *Raft) searchStartIndexByTerm(target int) int {
	l := 0
	r := len(rf.log) - 1
	ans := len(rf.log)
	for l <= r {
		mid := (r-l)/2 + l
		if rf.log[mid].Term >= target {
			r = mid - 1
			ans = mid
		} else {
			l = mid + 1
		}
	}
	if rf.log[ans].Term == target {
		return ans
	} else {
		return -1
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != 1 {
		return 0, 0, false
	}
	rf.mu.Lock()
	length := len(rf.log)
	term := rf.currentTerm
	entry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me] = rf.nextIndex[rf.me] + 1
	defer rf.mu.Unlock()
	// Your code here (3B).
	return length, term, true
}

func (rf *Raft) commit(index int) {
	//fmt.Printf("%v commiting %v %v\n", rf.me, index, rf.commitIndex)
	for i := rf.commitIndex + 1; i <= index; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}
	rf.commitIndex = index
}

func (rf *Raft) broadcastCommit() {
	for i, _ := range rf.peers {
		if i != rf.me {
			actualIndex := i
			go func() {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.log[len(rf.log)-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				//fmt.Println("commiting... server:", actualIndex)
				rf.sendAppendEntries(actualIndex, &args, &reply)
			}()
		}
	}
}

// 定期检查是否有新的可commit的log
func (rf *Raft) commitProcess() {
	for rf.killed() == false {
		if rf.state != 1 {
			return
		}
		rf.mu.Lock()
		toBeCommitted := rf.commitIndex + 1
		count := 0
		for i, _ := range rf.peers {
			if rf.matchIndex[i] >= toBeCommitted {
				count++
			}
		}
		rf.mu.Unlock()
		if count > len(rf.peers)/2 {
			go func() {
				rf.commit(toBeCommitted)
				//rf.broadcastCommit()
			}()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) appendLog(serverId int) {
	for rf.killed() == false {
		if rf.state == 1 {
			rf.mu.Lock()
			if len(rf.log) > rf.nextIndex[serverId] {
				entries := make([]LogEntry, len(rf.log)-rf.nextIndex[serverId])
				copy(entries, rf.log[rf.nextIndex[serverId]:])
				term := rf.currentTerm
				prevIdx := rf.nextIndex[serverId] - 1
				//fmt.Println("prevIdx:", prevIdx)
				prevTerm := rf.log[prevIdx].Term
				ldCommit := rf.commitIndex
				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevIdx,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: ldCommit,
				}
				reply := AppendEntriesReply{}
				//fmt.Println("appending log... server:", serverId, "prevIdx", prevIdx)
				rf.sendAppendEntries(serverId, &args, &reply)
				rf.mu.Lock()
				if term != rf.currentTerm {
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()
				if reply.Success {
					rf.mu.Lock()
					rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
					//fmt.Println("success, serverId:", serverId, "nextIndex:", rf.nextIndex[serverId], "matchIndex:", rf.matchIndex[serverId])
					rf.mu.Unlock()
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						//fmt.Println("appendlog", rf.me)
						rf.state = 0
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					if reply.XTerm == -1 {
						rf.mu.Lock()
						rf.nextIndex[serverId] = rf.nextIndex[serverId] - reply.XLen
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						ldStartIndex := rf.searchStartIndexByTerm(reply.XTerm)
						if ldStartIndex == -1 {
							rf.nextIndex[serverId] = reply.XIndex
						} else {
							rf.nextIndex[serverId] = ldStartIndex + 1
						}
						rf.mu.Unlock()
					}
				}
			} else {
				rf.mu.Unlock()
			}
		} else {
			return
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// 定期检查是否有新的可append的log
func (rf *Raft) appendProcess() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.appendLog(i)
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		ms := 500 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//1. 状态是follower 2. 没有收到心跳
		t := time.Now().UnixMilli()
		if rf.state != 1 && t-rf.heartBeatNum > ms {
			//开始选举
			////fmt.Println("start election, id: ", rf.me, " ", t, " ", rf.heartBeatNum)
			rf.startElection()
		}
	}
}

func (rf *Raft) heartBeatProcess() {
	for rf.killed() == false {
		if rf.state == 1 {
			rf.broadcastHeartBeat()
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = 2
	rf.currentTerm++
	//fmt.Println("election, me:", rf.me, "term:", rf.currentTerm)
	rf.votedFor = rf.me
	ballotNum := 1
	for index, _ := range rf.peers {
		if index != rf.me {
			actualIndex := index
			go func() {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(actualIndex, &args, &reply)
				////fmt.Printf("sended RequestVote at term %d, to server %d, voted? %v\n", rf.currentTerm, actualIndex, reply.VoteGranted)
				voted := reply.VoteGranted
				if voted {
					ballotNum++
				}
				if ballotNum > len(rf.peers)/2 && rf.state == 2 {
					rf.state = 1
					rf.broadcastHeartBeat()
					//上位后初始化nextIndex
					for index, _ := range rf.peers {
						rf.nextIndex[index] = len(rf.log)
						if index == rf.me {
							rf.matchIndex[index] = len(rf.log) - 1
						} else {
							rf.matchIndex[index] = 0
						}
					}
					go rf.commitProcess()
					go rf.appendProcess()
					//fmt.Println(rf.me, " win")
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.heartBeatNum = time.Now().UnixMilli()
					rf.state = 0
				}
			}()
		}
	}
	//beginTime := time.Now().UnixMilli()
	//////fmt.Println(rf.me, " start: ", beginTime, " ", rf.state)
	//for rf.state == 2 {
	//	if ballotNum > len(rf.peers)/2 && rf.state != 1 {
	//		rf.state = 1
	//		rf.broadcastHeartBeat()
	//		////fmt.Println(rf.me, " win", " ", rf.state)
	//		return
	//	}
	//	////fmt.Println(rf.me, " cur: ", time.Now().UnixMilli(), " ", rf.state)
	//	if time.Now().UnixMilli()-beginTime > 150 {
	//		defer rf.startElection()
	//		////fmt.Println("选举超时，", rf.me, " ", time.Now().UnixMilli(), " ", rf.state)
	//		return
	//	}
	//	time.Sleep(20 * time.Millisecond)
	//}
}

func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Println(rf.me, " broadcasting.....")
	for index, _ := range rf.peers {
		////fmt.Println(rf.me, " broadcasting.....", time.Now().UnixMilli(), " ", index)
		if index != rf.me {
			actualIndex := index
			go func() {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.currentTerm,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(actualIndex, &args, &reply)
				if reply.Term > rf.currentTerm {
					rf.state = 0
					curTime := time.Now().UnixMilli()
					rf.heartBeatNum = curTime
				}
			}()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//需要一个dummy节点
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 0
	rf.heartBeatNum = 0
	rf.votedFor = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatProcess()

	return rf
}
