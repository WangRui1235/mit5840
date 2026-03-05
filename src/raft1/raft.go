package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type State int

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role              State
	currentTerm       int
	votedFor          int
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	Log               []LogEntry
	lastHeartbeat     time.Time
	lastIncludedIndex int
	lastIncludedTerm  int

	applyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	// warn: Passing nil causes issues because persister.Save(raftstate, nil) atomically overwrites the snapshot with nil (source: ps.snapshot = clone(nil)).
	// Any persist call that does not intend to update the snapshot should pass through the existing one
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil {
		return
	}
	if len(data) < 1 {
		return
	}
	// Your code here (3C).
	// Example:
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
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	var currentTerm int
	var votedFor int
	var Log []LogEntry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// error...
		panic("Decode Error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.Log = Log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.Log[rf.logIndex(index)].Term
	newLog := make([]LogEntry, len(rf.Log[rf.logIndex(index):]))
	copy(newLog, rf.Log[rf.logIndex(index):])
	rf.Log = newLog

	rf.lastIncludedIndex = index
	rf.persist(snapshot)
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

func (rf *Raft) lastlogIndex() int {
	return len(rf.Log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) lastlogTerm() int {
	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) logIndex(LogicalIndex int) int {
	return LogicalIndex - rf.lastIncludedIndex
}

func (rf *Raft) logicalIndex(logIndex int) int {
	return logIndex + rf.lastIncludedIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(rf.persister.ReadSnapshot())
	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	// warn: up-to-date check should be careful of order of conditions, otherwise it may cause wrong behavior when the candidate's log
	if (rf.lastlogTerm() > args.LastLogTerm) || ((rf.lastlogTerm() == args.LastLogTerm) && (rf.lastlogIndex() > args.LastLogIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// Log up-to-date check
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	// in 3A we ignore log up-to-date check

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
// send parallel requests to all servers
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(rf.persister.ReadSnapshot())

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = args.Term
	rf.role = Follower

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	// the log is too short
	if args.PrevLogIndex >= rf.lastlogIndex()+1 {
		reply.Success = false
		// the conflict index is equal to nextIndex, which is the index of the first entry that is not in the log
		reply.ConflictIndex = rf.lastlogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.Log[rf.logIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.Log[rf.logIndex(args.PrevLogIndex)].Term
		i := rf.logIndex(args.PrevLogIndex)
		for i > 0 && rf.Log[i].Term == reply.ConflictTerm {
			i -= 1
		}
		reply.ConflictIndex = rf.logicalIndex(i + 1)
		return
	}
	// warn:empty Log as heartbeat should not overwrite existing log
	// if len(args.Entries) == 0 {
	// 	reply.Success = true
	// 	return
	// }
	// warn: args.Entries and rf.Log have no dangerous overlap
	// warn : RPC isolation guarantee: labrpc performs Gob serialization and deserialization
	// warn: args.Entries on the receiver side is a freshly allocated block of memory
	// warn: if there is a conflict, delete the existing entry and all that follow it
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+1+i <= rf.lastlogIndex() {
			// handle conflicts
			if rf.Log[rf.logIndex(args.PrevLogIndex+1+i)].Term != args.Entries[i].Term {
				rf.Log = rf.Log[:rf.logIndex(args.PrevLogIndex+1+i)]
				rf.Log = append(rf.Log, args.Entries[i:]...)
				break
			}
		} else {
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
	}
	reply.Success = true
	// commitIndex should be updated to min(leaderCommit, index of last new entry)
	// warn:commitIndex only increases
	// error: index of last new entry should be args.PrevLogIndex + len(args.Entries),
	// error: but they are equal to len(rf.Log) - 1
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, rf.lastlogIndex())
	}
}

func (rf *Raft) sendAppendEntrie(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex < rf.lastlogIndex() &&
		args.LastIncludedTerm == rf.Log[rf.logIndex(args.LastIncludedIndex)].Term {
		index := rf.logIndex(args.LastIncludedIndex)
		newLog := make([]LogEntry, len(rf.Log[index:]))
		copy(newLog, rf.Log[index:])
		rf.Log = newLog
	} else {
		// dummy entry to simplify the logic of log replication and commitment
		rf.Log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	// applier may try to apply entries that are already compacted, so we need to update lastApplied to avoid applying compacted entries
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	rf.persist(args.Data)

	rf.mu.Unlock()
	// let the state machine know that it should now use the snapshot
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- applyMsg
	rf.mu.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(rf.persister.ReadSnapshot())
	if rf.role == Leader {
		index = rf.lastlogIndex() + 1
		term = rf.currentTerm
		//
		rf.Log = append(rf.Log, LogEntry{Term: term, Command: command})
	} else {
		isLeader = false
	}

	return index, term, isLeader
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
	// Goroutine leak prevention: if rf.killed() returns true, this goroutine should exit.
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			// send heartbeats to all followers
			if time.Since(rf.lastHeartbeat) > time.Duration(50)*time.Millisecond {
				//warn : be careful when using rf.lastheartbeat, which has different meaning for leader and followers
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				go rf.sendAppendEntries()
				continue
			}
		} else {
			// if last heartbeat was more than 200-350ms ago, start an election
			if time.Since(rf.lastHeartbeat) > time.Duration(200+rand.Int63()%150)*time.Millisecond {
				rf.role = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.lastHeartbeat = time.Now()

				rf.persist(rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				go rf.sendRequestVotes()
				continue
			}
		}
		rf.mu.Unlock()
	}
}

// there is no long-running goroutine in this function, so we don't need to check rf.killed() here
func (rf *Raft) sendRequestVotes() {
	rf.mu.Lock()
	term := rf.currentTerm
	candidateid := rf.me
	lastLogIndex := rf.lastlogIndex()
	lastLogTerm := rf.lastlogTerm()
	votes := 1
	// warn: use channel to collect votes may cause goroutine leak,
	// warn: because if the candidate loses the election, the goroutines will be blocked on sending votes to the channel,
	// warn: and they will never exit. so we use a counter to count votes instead of a channel.
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateid,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			// vote for self
			// labrpc guarantees that the RPC handler will return, so there is no need to implement our own timeout here
			// 10% chance lose request,10% chance lose reply, instant return false
			// server may be killed.but at most 100 ms can be detected
			// real-world RPCs (net/rpc or gRPC) run over TCP, the exponential backoff
			// mechanism can cause a Call() to block for up to 15 minutes before failing
			// warn: use context to set timeout
			// ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			// defer cancel()
			// reply, err := client.AppendEntries(ctx, args)
			// if err != nil {
			//     return
			// }
			if rf.sendRequestVote(i, args, reply) {
				rf.mu.Lock()
				if reply.VoteGranted {
					// callback may cause term/role change, so we need to check term/role before counting votes
					votes += 1
					if votes > len(rf.peers)/2 {
						//warn: term/role may change
						if rf.currentTerm != term || rf.role != Candidate {
							// 	rf.persist()
							rf.mu.Unlock()
							return
						}
						rf.role = Leader
						rf.lastHeartbeat = time.Now()
						for i := range rf.peers {
							rf.nextIndex[i] = rf.lastlogIndex() + 1
							rf.matchIndex[i] = 0
						}
						//tests show that there is no need to persist here
						//rf.persist()
						rf.mu.Unlock()
						go rf.sendAppendEntries()
						return
					}
					rf.mu.Unlock()
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.persist(rf.persister.ReadSnapshot())
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

}

func (rf *Raft) sendAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		// the follower's log is too short, so we need to send InstallSnapshot RPC instead of AppendEntries RPC
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.sendInstallSnapshots(i)
			continue
		}
		term := rf.currentTerm
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.Log[rf.logIndex(prevLogIndex)].Term

		leaderCommit := rf.commitIndex
		// warn: be careful when slicing the log,deep copy is needed
		// warn: data pointer in interface{} is shallow copied, but Commmand is immutable, so we don't need to deep copy Command
		entries := make([]LogEntry, len(rf.Log[rf.logIndex(rf.nextIndex[i]):]))
		// The copy built-in function copies elements from a source slice into a
		// destination slice. (As a special case, it also will copy bytes from a
		// string to a slice of bytes.) The source and destination may overlap. Copy
		// returns the number of elements copied, which will be the minimum of
		// len(src) and len(dst).
		copy(entries, rf.Log[rf.logIndex(rf.nextIndex[i]):])

		rf.mu.Unlock()
		go func(i int) {
			// warn:1.Atomic Snapshot,2.Minimize Critical Section
			// rf.mu.Lock()
			// args := &AppendEntriesArgs{
			// 	Term:         term,
			// 	LeaderId:     rf.me,
			// 	PrevLogIndex: rf.nextIndex[i] - 1,
			// 	PrevLogTerm:  rf.Log[rf.nextIndex[i]-1].Term,
			// 	LeaderCommit: rf.commitIndex,
			// 	Entries:      rf.Log[rf.nextIndex[i]:],
			// }
			// rf.mu.Unlock()
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit,
				Entries:      entries,
			}
			// reply := &AppendEntriesReply{}
			for {
				// Goroutine leak prevention: if rf.killed() returns true, this goroutine should exit.
				// warn:any goroutine with a long-running loop should call killed()
				if rf.killed() {
					return
				}
				// warn: reply should be initialized in the loop, otherwise the old reply may cause wrong behavior
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntrie(i, args, reply) {
					rf.mu.Lock()
					// warn: callback may cause term/role change, so we need to check term/role before processing reply
					if rf.currentTerm != term || rf.role != Leader {
						// rf.persist()
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						nextIndex := args.PrevLogIndex + len(args.Entries) + 1
						// error: Rachet, matchIndex only increases, so nextIndex should be greater than current nextIndex
						if nextIndex > rf.nextIndex[i] {
							rf.nextIndex[i] = nextIndex
							rf.matchIndex[i] = rf.nextIndex[i] - 1
						}

						for N := rf.lastlogIndex(); N > rf.commitIndex; N-- {
							count := 1
							for j := range rf.peers {
								if j != rf.me && rf.matchIndex[j] >= N {
									count += 1
								}
							}
							// warn : no need to wait for all followers to respond, as long as a majority of followers have replicated the entry, the leader can commit the entry
							if count > len(rf.peers)/2 && rf.Log[rf.logIndex(N)].Term == term {
								rf.commitIndex = N
								break
							}
						}
						rf.mu.Unlock()
						return
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.role = Follower
							rf.votedFor = -1
							rf.persist(rf.persister.ReadSnapshot())
							rf.mu.Unlock()
							return
						}
						// rf.nextIndex[i] -= 1
						if reply.ConflictTerm == -1 {
							rf.nextIndex[i] = reply.ConflictIndex
						} else {
							idx := -1
							for j := len(rf.Log) - 1; j >= 0; j-- {
								if rf.Log[j].Term == reply.ConflictTerm {
									idx = j
									break
								}
							}
							if idx != -1 {
								// the last entry in the log with the conflicting term is at index idx, so we can skip all entries after idx
								rf.nextIndex[i] = rf.logicalIndex(idx + 1)
							} else {
								// the whole term is not in the log, so we can skip all entries in that term
								rf.nextIndex[i] = reply.ConflictIndex
							}
						}
						if rf.nextIndex[i] < 1 {
							rf.nextIndex[i] = 1
						}
						// args.PrevLogIndex = rf.nextIndex[i] - 1
						// // PrevLogTerm should be 0 if PrevLogIndex is 0, otherwise it should be the term of the log entry at PrevLogIndex
						// warn: one-flight RPC,be careful not continue in one rpc,retry in the next AppendEntries RPC!
						// args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
						// // data's life cycle != time to handle the mutex
						// args.Entries = make([]LogEntry, len(rf.Log[rf.nextIndex[i]:]))
						// copy(args.Entries, rf.Log[rf.nextIndex[i]:])
						// error: Run 93
						// === Run 93 ===
						// 2026/02/22 10:27:51 apply error: commit index=52 server=2 1252713630947491826 != server=4 287926712796257467
						// exit status 1
						// FAIL    6.5840/raft1    57.008s
						// error:safety violation!!!
						rf.mu.Unlock()
						return
					}
				} else {
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) sendInstallSnapshots(i int) {
	rf.mu.Lock()
	term := rf.currentTerm
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	snapshot := rf.persister.ReadSnapshot()
	rf.mu.Unlock()
	go func(i int) {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Data:              snapshot,
		}
		reply := &InstallSnapshotReply{}
		if rf.sendInstallSnapshot(i, args, reply) {
			rf.mu.Lock()
			if rf.currentTerm != term || rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.persist(rf.persister.ReadSnapshot())
			}
			if rf.nextIndex[i] < lastIncludedIndex+1 {
				rf.nextIndex[i] = lastIncludedIndex + 1
				rf.matchIndex[i] = lastIncludedIndex
			}
			rf.mu.Unlock()
		}
	}(i)
	// update nextIndex and matchIndex
	// rf.mu.Lock()
	// if rf.nextIndex[i] < lastIncludedIndex+1 {
	// 	rf.nextIndex[i] = lastIncludedIndex + 1
	// 	rf.matchIndex[i] = lastIncludedIndex
	// }
	// rf.mu.Unlock()
	//warn: Result-driven not Attempt-driven, we should update nextIndex and matchIndex in the callback of InstallSnapshot RPC, because only when the follower successfully receives and processes the InstallSnapshot RPC, we can be sure that the follower has the snapshot and can update nextIndex and matchIndex accordingly. If we update nextIndex and matchIndex here, there is a risk that the InstallSnapshot RPC fails to send or the follower fails to process it, which may cause nextIndex and matchIndex to be updated incorrectly, leading to log inconsistency between the leader and followers.
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.logIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.Log = []LogEntry{{Term: 0, Command: nil}} // log index starts at 1, so we add a dummy entry at index 0

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// cap >= len
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// recover last applied index and commit index after restart
	//warn: === RUN   TestSnapshotInstallCrash3D
	// Test (3D): install snapshots (crash) (reliable network)...
	// panic: runtime error: index out of range [-38]
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
