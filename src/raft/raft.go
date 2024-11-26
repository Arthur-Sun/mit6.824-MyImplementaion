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
	voteTerm    int

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
	////fmt.Println("????", len(args.Entries), args.Entries == nil, rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//1. leader's term < follower's term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//heartBeat
	rf.currentTerm = args.Term
	rf.state = 0
	curTime := time.Now().UnixMilli()
	rf.heartBeatNum = curTime
	rf.votedFor = -1
	//5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("%v update commitIndex, args.LeaderCommit: %v, rf.commitIndex: %v %v\n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.log))
		indexToCommit := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		rf.commit(indexToCommit)
	}

	if args.Entries != nil {
		//2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		if (len(rf.log) < args.PrevLogIndex+1) || (args.PrevLogIndex >= 0 && (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)) {
			reply.Success = false
			//todo backup
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
		} else {
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
					rf.log = append(rf.log, args.Entries[i])
				}
			}
			return
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for ok == false {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term >= rf.currentTerm && ((rf.votedFor == -1 || rf.votedFor == args.CandidateId) || args.Term > rf.voteTerm) {
		reply.VoteGranted = true
		rf.state = 0
		rf.voteTerm = args.Term
		rf.votedFor = args.CandidateId
		//fmt.Printf("Follower %d requested vote for term %d\n", rf.me, args.Term)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	//fmt.Printf("candidate: %v, follower: %v follower's term: %v, candidate's term: %v,  votedFor: %v\n", args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.votedFor)
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
	index := len(rf.log)
	term := rf.currentTerm
	// Your code here (3B).

	//如果是master，启动新协程来运行，保证立刻返回
	go func() {
		entry := LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, entry)
		count := 1
		for i, _ := range rf.peers {
			if i != rf.me {
				actualIndex := i
				go func() {
					entries := make([]LogEntry, 1)
					copy(entries, rf.log[index:])
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: index - 1,
						PrevLogTerm:  rf.log[index-1].Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					//fmt.Printf("leader %v sending to %v\n", rf.me, actualIndex)
					rf.sendAppendEntries(actualIndex, &args, &reply)
					//todo 有错误，重新发送
					for !reply.Success {
						//场景3，log槽位有缺失
						if reply.XTerm == -1 {
							startIndex := index - reply.XLen
							args.PrevLogIndex = startIndex - 1
							args.PrevLogTerm = rf.log[startIndex-1].Term
							entries = make([]LogEntry, reply.XLen+1)
							copy(entries, rf.log[startIndex:])
							args.Entries = entries
							rf.sendAppendEntries(actualIndex, &args, &reply)
						} else {
							ldStartIndex := rf.searchStartIndexByTerm(reply.XTerm)
							//场景1，leader没有XTerm
							if ldStartIndex == -1 {
								startIndex := reply.XIndex
								args.PrevLogIndex = startIndex - 1
								args.PrevLogTerm = rf.log[startIndex-1].Term
								entries = make([]LogEntry, index-startIndex+1)
								copy(entries, rf.log[startIndex:])
								args.Entries = entries
								rf.sendAppendEntries(actualIndex, &args, &reply)
							} else {
								//场景2，leader有XTerm
								startIndex := ldStartIndex + 1
								args.PrevLogIndex = startIndex - 1
								args.PrevLogTerm = rf.log[startIndex-1].Term
								entries = make([]LogEntry, index-startIndex+1)
								copy(entries, rf.log[startIndex:])
								args.Entries = entries
								rf.sendAppendEntries(actualIndex, &args, &reply)
							}
						}
					}
					//判断是否被大多数收到
					if reply.Success {
						count++
						rf.nextIndex[actualIndex] = len(rf.log)
					}
					if count > len(rf.peers)/2 {
						rf.commit(index)
						rf.broadcastCommit()
					}
				}()
			}
		}
	}()

	return index, term, true
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
				rf.sendAppendEntries(actualIndex, &args, &reply)
			}()
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
		ms := 1000 + (rand.Int63() % 1000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//1. 状态是follower 2. 没有收到心跳
		t := time.Now().UnixMilli()
		if rf.state == 0 && t-rf.heartBeatNum > ms {
			//开始选举
			//fmt.Println("start election, id: ", rf.me, " ", t, " ", rf.heartBeatNum)
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
	rf.votedFor = rf.me
	ballotNum := 1
	for index, _ := range rf.peers {
		if index != rf.me {
			actualIndex := index
			go func() {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastApplied,
					LastLogTerm:  rf.lastApplied,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(actualIndex, &args, &reply)
				for ok == false {
					ok = rf.sendRequestVote(actualIndex, &args, &reply)
				}
				//fmt.Printf("sended RequestVote at term %d, to server %d, voted? %v\n", rf.currentTerm, actualIndex, reply.VoteGranted)
				voted := reply.VoteGranted
				if voted {
					ballotNum++
				}
				if ballotNum > len(rf.peers)/2 && rf.state != 1 {
					rf.state = 1
					rf.broadcastHeartBeat()
					//上位后初始化nextIndex
					for index, _ := range rf.peers {
						if index != rf.me {
							rf.nextIndex[index] = len(rf.log)
						}
					}
					//fmt.Println(rf.me, " win", " ", rf.state)
					return
				}
				if reply.Term > rf.currentTerm {
					rf.state = 0
				}
			}()
		}
	}
	//beginTime := time.Now().UnixMilli()
	////fmt.Println(rf.me, " start: ", beginTime, " ", rf.state)
	//for rf.state == 2 {
	//	if ballotNum > len(rf.peers)/2 && rf.state != 1 {
	//		rf.state = 1
	//		rf.broadcastHeartBeat()
	//		//fmt.Println(rf.me, " win", " ", rf.state)
	//		return
	//	}
	//	//fmt.Println(rf.me, " cur: ", time.Now().UnixMilli(), " ", rf.state)
	//	if time.Now().UnixMilli()-beginTime > 150 {
	//		defer rf.startElection()
	//		//fmt.Println("选举超时，", rf.me, " ", time.Now().UnixMilli(), " ", rf.state)
	//		return
	//	}
	//	time.Sleep(20 * time.Millisecond)
	//}
}

func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me, " broadcasting.....")
	for index, _ := range rf.peers {
		//fmt.Println(rf.me, " broadcasting.....", time.Now().UnixMilli(), " ", index)
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
