package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"reflect"
	"sort"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
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
	applyCh chan raftapi.ApplyMsg
    status int // 0 follower , 1 candidate, 2 leader
	lastHeartBeat time.Time //

	currentTerm int
	voteFor int

	lastIncludedIndex int 
    lastIncludedTerm  int

	log []LogContent

	commitIndex int 
	lastApplied int
	
	nextIndex []int
	matchIndex []int

	currentSnapShot LocalSnapshot

}

type PersistState struct {
	CurrentTerm int
    VoteFor     int      
    Log         []LogContent 
	LastIncludedIndex int
    LastIncludedTerm  int
}

type LogContent struct {
	Term int
	Content interface{}
}

func (rf *Raft) getLogSliceIndex(absoluteIndex int) int {
    return absoluteIndex - rf.lastIncludedIndex
}

// Helper: Get entry at absolute index
func (rf *Raft) getLogEntry(absoluteIndex int) LogContent {
    sliceIdx := rf.getLogSliceIndex(absoluteIndex)
    return rf.log[sliceIdx]
}

// Helper: Get the absolute index of the last log entry
func (rf *Raft) getLastLogIndex() int {
    return rf.lastIncludedIndex + len(rf.log) - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm , rf.status == 2
}

func (rf *Raft) compareInterfaceSlices(s1 interface{}, s2 interface{}) bool {
	if !reflect.DeepEqual(s1, s2) {
		return false
	}
	return true
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

func (rf *Raft) getPersistStateBytes() []byte {
    
    // 1. Create the struct containing all persistent fields
    state := PersistState{
        CurrentTerm:       rf.currentTerm,
        VoteFor:           rf.voteFor,
        Log:               rf.log, // Assuming rf.log includes the dummy entry
        LastIncludedIndex: rf.lastIncludedIndex,
        LastIncludedTerm:  rf.lastIncludedTerm,
    }

    // 2. Initialize the buffer and encoder
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    // 3. Encode the state struct
    err := e.Encode(state)
    
    // 4. Handle potential encoding error
    if err != nil {
        // You should log this error, as failure to persist is critical
        // DPrintf("Raft %d failed to encode persistent state: %v", rf.me, err)
        
        // IMPORTANT: Returning a nil or empty byte slice on error can hide bugs.
        // Depending on your error handling, you might panic or return nil.
        return nil 
    }   
    // 5. Return the encoded bytes from the buffer
    return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
    
    raftstate := rf.getPersistStateBytes()
	var snapshotBytes []byte
    if rf.currentSnapShot.Data != nil {
         snapshotBytes = rf.currentSnapShot.Data
    }

    rf.persister.Save(raftstate, snapshotBytes)
}



// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
	// Example:
	if data == nil || len(data) < 1 { 
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var state PersistState
    if d.Decode(&state) != nil {
        // Handle error
    } else {
        rf.currentTerm = state.CurrentTerm
        rf.voteFor = state.VoteFor
        rf.log = state.Log
		rf.lastIncludedIndex = state.LastIncludedIndex
        rf.lastIncludedTerm = state.LastIncludedTerm
		rf.lastApplied = state.LastIncludedIndex
		rf.commitIndex = state.LastIncludedIndex
    }

	snapshotData := rf.persister.ReadSnapshot()
    if len(snapshotData) > 0 {
        rf.currentSnapShot = LocalSnapshot{
             Data: snapshotData,
             MetaData: SnapshotMetadata{
                 LastIncludedIndex: rf.lastIncludedIndex,
                 LastIncludedTerm:  rf.lastIncludedTerm,
             },
        }
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
// need to hold lock when calling it
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D). 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
        return
    }
	lastLogIndex := rf.getLastLogIndex()
	if index > lastLogIndex {
		// Log an error or panic: The service tried to snapshot an index that doesn't exist.
		// For a stable implementation, just return if this happens.
		return 
	}

	snapshottedTerm := rf.getLogEntry(index).Term
	sliceIndexAfterIndex := rf.getLogSliceIndex(index) + 1 // Index of the first entry *after* 'index'
	
	// 2. Create the new log: Dummy entry + remaining log entries
	var newLog []LogContent
	
	// New dummy entry (index 0 of the slice) uses the term of the snapshotted entry
	newLog = make([]LogContent, 1)
	newLog[0] = LogContent{
		Term: snapshottedTerm,
		Content: nil, // Command content is discarded for the dummy entry
	}

	// 3. Append the remaining log entries (if any)
	if sliceIndexAfterIndex < len(rf.log) {
		// Append log entries starting from the entry *after* 'index'
		newLog = append(newLog, rf.log[sliceIndexAfterIndex:]...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = snapshottedTerm
	
	// Update snapshot data
	rf.currentSnapShot.MetaData.LastIncludedIndex = index
	rf.currentSnapShot.MetaData.LastIncludedTerm = snapshottedTerm
	rf.currentSnapShot.Data = snapshot

	rf.persist()
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
    Content []LogContent
	LeaderCommit int
}

type SnapshotMetadata struct {
    LastIncludedIndex int // The absolute index of the last entry included in the snapshot
    LastIncludedTerm  int // The term of the entry at LastIncludedIndex
}

type LocalSnapshot struct {
	Term int
    LeaderId int
    MetaData SnapshotMetadata
	Data []byte
}

type InstallSnapshotArgs struct {
	SnapshotData LocalSnapshot
    Offset int
    Done bool
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictTerm  int // Term of the conflicting entry (if index mismatch)
	ConflictIndex int
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

func (rf *Raft) InstallSnapShots(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock();
	reply.Term = rf.currentTerm
    snapshotIndex := args.SnapshotData.MetaData.LastIncludedIndex
    snapshotTerm := args.SnapshotData.MetaData.LastIncludedTerm

	if (args.SnapshotData.Term < rf.currentTerm) {
		DPrintf("inside InstallSnapShots args.SnapshotData.Term < rf.currentTerm")
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.lastHeartBeat = time.Now()
    
	change := false
	if args.SnapshotData.Term > rf.currentTerm {
        rf.currentTerm = args.SnapshotData.Term
        rf.status = 0 // Convert to Follower
        rf.voteFor = -1
		change = true
       
    }

	if args.SnapshotData.MetaData.LastIncludedIndex <= rf.lastIncludedIndex {
		DPrintf("inside InstallSnapShots args.SnapshotData.MetaData.LastIncludedIndex <= rf.lastIncludedIndex")
        if (change) {
			rf.persist()
		}
		rf.mu.Unlock()
		return
    }

	if snapshotIndex <= rf.getLastLogIndex() && rf.getLogEntry(snapshotIndex).Term == snapshotTerm {
        // Log is consistent: Trim log, retaining log entries that follow the snapshot index.
        
        // sliceIndexAfterIndex is the index into rf.log slice *after* the entry at snapshotIndex.
        sliceIndexAfterIndex := rf.getLogSliceIndex(snapshotIndex) + 1
        
        // 4. Create the new log structure
        var newLog []LogContent
        newLog = make([]LogContent, 1)
        
        // The first "entry" in the new log slice is the metadata for the snapshotted entry.
        newLog[0] = LogContent{Term: snapshotTerm} 
        
        // Append any remaining entries that came after the snapshot point
        if sliceIndexAfterIndex < len(rf.log) {
            newLog = append(newLog, rf.log[sliceIndexAfterIndex:]...)
        }
        rf.log = newLog

    } else {
        // Log is inconsistent or too short (Step 7): Discard the entire log, keeping only the dummy entry.
        rf.log = make([]LogContent, 1)
        rf.log[0] = LogContent{Term: snapshotTerm}
    }

	metadata := SnapshotMetadata{
    	LastIncludedIndex : snapshotIndex,
    	LastIncludedTerm : snapshotTerm,
	}
	data := args.SnapshotData.Data

	rf.lastIncludedIndex = snapshotIndex
    rf.lastIncludedTerm  = snapshotTerm

	if rf.lastApplied < snapshotIndex {
		rf.lastApplied = snapshotIndex
	}

	localSnapshot := LocalSnapshot{
		Term : rf.currentTerm,
    	LeaderId : args.SnapshotData.LeaderId,
    	MetaData : metadata,
		Data : data,
	}
	DPrintf("Raft install snapshot: %v, lastincludindex:%d, latincludeterm:%d, %s", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, string(data))
	rf.currentSnapShot = localSnapshot
	if rf.commitIndex < snapshotIndex {
        rf.commitIndex = snapshotIndex
    }
	rf.persist()
	rf.mu.Unlock()

	rf.applyCh <- raftapi.ApplyMsg{
        SnapshotValid: true,
        Snapshot: data,
        SnapshotIndex: snapshotIndex,
        SnapshotTerm: snapshotTerm,
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	if (args.Term < rf.currentTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} 
	if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.voteFor = -1 // Clear vote
        rf.status = 0 
		reply.Term = rf.currentTerm
		rf.persist()
    }
    
	// 1. Reply false if log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm

	prevLogIndex := args.PrevLogIndex
	lastLogIndex := rf.getLastLogIndex()
    if prevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	if prevLogIndex > lastLogIndex {
		// Log is too short. ConflictIndex should be the length of the follower's log + 1 (the next index to try).
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = lastLogIndex + 1
		return
	}

	// Case: Log index exists, but terms mismatch
	if rf.getLogEntry(prevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.getLogEntry(prevLogIndex).Term
		// Find the first index of the conflicting term
		// The leader should backtrack to the first entry of the conflicting term.
		conflictIndex := prevLogIndex
		for conflictIndex > rf.lastIncludedIndex && rf.getLogEntry(conflictIndex-1).Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	nextIdx := args.PrevLogIndex + 1 
    for i := 0; i < len(args.Content); i++ {
		logIndex := nextIdx + i
		newEntry := args.Content[i]
        
		if logIndex > rf.getLastLogIndex() {
			// Append all remaining new entries and stop the loop
			rf.log = append(rf.log, args.Content[i:]...)
			rf.persist()
			break
		}

		// Case 2: Conflict found (same index, different terms)
		if rf.getLogEntry(logIndex).Term != newEntry.Term {
			// Truncate the log from the conflict point (this index)
			sliceIdx := rf.getLogSliceIndex(logIndex)
			rf.log = rf.log[:sliceIdx]
			// Append the new entries from the current index i onwards
			rf.log = append(rf.log, args.Content[i:]...)
			rf.persist()
			break
		}
	}
	
	if (args.LeaderCommit > rf.commitIndex) {
		lastLogIndex := rf.getLastLogIndex()
        if args.LeaderCommit < lastLogIndex {
             rf.commitIndex = args.LeaderCommit
        } else {
             rf.commitIndex = lastLogIndex
        }
	}
	rf.status = 0
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

// the struct itself.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the struct itself.
func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("send install snapshot to server %d", server)
	ok := rf.peers[server].Call("Raft.InstallSnapShots", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return;
	}
    
	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.status = 0
	} 
	reply.Term = rf.currentTerm
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		myLastLogIndex := rf.getLastLogIndex()
		myLastLogTerm := rf.getLogEntry(myLastLogIndex).Term
		if (args.LastLogTerm > myLastLogTerm) || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
			DPrintf("%d grant vote for %d\n", rf.me, args.CandidateId)
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeartBeat = time.Now()
			return;
		} else {
			reply.VoteGranted = false
			return;
		}
	} else {
		reply.VoteGranted = false
		return;
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

func (rf *Raft) createInstallSnapShotArgs(i int) InstallSnapshotArgs {
	localSnapshot := rf.currentSnapShot
	localSnapshot.Term = rf.currentTerm
	args := InstallSnapshotArgs{
		SnapshotData: localSnapshot,
		Offset : 0,
		Done : true,
	}
	return args
}

// will need to hold a lock
func (rf *Raft) createAppendEntrieArgs(i int) AppendEntriesArgs {
	prevIndex := rf.nextIndex[i] - 1
	prevLogTerm := rf.log[0].Term
	if (prevIndex >= 0) {
		prevLogTerm = rf.getLogEntry(prevIndex).Term
	}
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm: prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	if (rf.nextIndex[i] <= rf.getLastLogIndex()) {
		//send entries
		args.Content = rf.log[rf.getLogSliceIndex(rf.nextIndex[i]) :]
	} else {
		args.Content = []LogContent{}
	}
	return args;
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
	 
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.status != 2) {
		return -1,-1, false
	}

	term := rf.currentTerm

	rf.log = append(rf.log, LogContent{
    	Term:    term,
    	Content:  command,
	})

	currentIdx := rf.getLastLogIndex()
	nextIdx := currentIdx + 1

	DPrintf("append to master %d, %d, %d, %d", rf.me, currentIdx, nextIdx, term)
	rf.matchIndex[rf.me] = currentIdx
	rf.nextIndex[rf.me] = nextIdx
	rf.persist()
	return currentIdx, term, true
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

func (rf *Raft) checkCommitIndex() {
    // Ensure lock is released upon function exit

    // 1. Gather all matchIndex values (including the Leader's own log end)
    
    // The Leader's log length is its own highest matched index.
    lastLogIndex := rf.getLastLogIndex()
    
    matchIndices := make([]int, len(rf.peers))
    matchIndices[rf.me] = lastLogIndex // Leader's log end
    
    // Collect all follower match indices
    for i := range rf.peers {
        if i != rf.me {
            matchIndices[i] = rf.matchIndex[i]
        }
    }

    // 2. Sort the Indices
    // Sorting allows us to find the median index, which represents the highest 
    // index replicated on a majority of servers.
    sort.Ints(matchIndices)

    // 3. Find the Majority Index
    N := len(rf.peers)
    majorityIndex := N / 2 
    
    // The value at the majorityIndex is the new candidate for commitIndex (M).
    newCommitIndex := matchIndices[majorityIndex]

    // 4. Raft Safety Check and Update
    
    // Condition 1: Must advance the commit index.
    // Condition 2 (CRITICAL): The log entry at the proposed index MUST belong to the 
    //                         current Leader's term. This prevents the commitment of 
    //                         stale entries from previous terms after network partitions.
	DPrintf("me%d, newCommitIndex %d, rf.commitIndex %d, len(rf.log) %d, rf.getLogEntry(newCommitIndex).Term %d, rf.currentTerm %d", rf.me, newCommitIndex, rf.commitIndex, rf.getLastLogIndex(), rf.getLogEntry(newCommitIndex).Term, rf.currentTerm)
    if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.getLastLogIndex() && rf.getLogEntry(newCommitIndex).Term == rf.currentTerm {
        rf.commitIndex = newCommitIndex // Safely update Raft state
        DPrintf("commit index is %d", rf.commitIndex)
    }
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		// Apply entries at a controlled rate
		time.Sleep(10 * time.Millisecond) 

		rf.mu.Lock()
		
		// Apply messages only if commitIndex has advanced past lastApplied
		if rf.commitIndex > rf.lastApplied {
			
			startIndex := rf.lastApplied + 1
			endIndex := rf.commitIndex

			var msgsToApply []raftapi.ApplyMsg
			
			// Collect entries to apply
			for i := startIndex; i <= endIndex; i++ {
				msgsToApply = append(msgsToApply, raftapi.ApplyMsg{
					CommandValid: true,
					Command: rf.getLogEntry(i).Content,
					CommandIndex: i,
				})
			}
			
			// Update lastApplied *before* unlocking to maintain a consistent view
			// This tells the applier where to start next time.
			rf.lastApplied = endIndex
			
			rf.mu.Unlock() // Unlock before sending to the potentially blocking channel
			
			// Send collected messages to the apply channel
			for _, msg := range msgsToApply {
				rf.applyCh <- msg
			}
			
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendInstallSnapshot(args InstallSnapshotArgs, reply InstallSnapshotReply , i int) {
	done := make(chan bool, 1)
    	go func() {
    		ok := rf.sendInstallSnapshotRPC(i, &args, &reply)
    		done <- ok
    	}()
		select {
    		case ok := <-done:
    			if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.status != 2 || rf.currentTerm != args.SnapshotData.Term{
        				return
    				}
					if reply.Term > rf.currentTerm {
                		rf.status = 0
                		rf.voteFor = -1
                		rf.currentTerm = reply.Term
						rf.persist() 
					} else {
						snapshotIndex := args.SnapshotData.MetaData.LastIncludedIndex
	 					rf.matchIndex[i] = snapshotIndex
						rf.nextIndex[i] = snapshotIndex + 1
					}
    			}
    		case <-time.After(150 * time.Millisecond): // timeout
    			// RPC timed out, do nothing		
    	}
}

func (rf *Raft) sendEntries(args AppendEntriesArgs, reply AppendEntriesReply, i int) {
    	// Wrap the blocking sendRequestVote call with a timeout
    	done := make(chan bool, 1)
    	go func() {
    		ok := rf.sendAppendEntries(i, &args, &reply)
    		done <- ok
    	}()
		select {
    		case ok := <-done:
    			if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.status != 2 || rf.currentTerm != args.Term {
        				return
    				}
					if (reply.Success == false) {
						if reply.Term > rf.currentTerm {
                			rf.status = 0
                			rf.voteFor = -1
                			rf.currentTerm = reply.Term
							rf.persist()
            			} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					} else {
						lastReplicatedIndex := args.PrevLogIndex + len(args.Content)
						if (lastReplicatedIndex > rf.matchIndex[i]) {
							rf.matchIndex[i] = lastReplicatedIndex
							rf.nextIndex[i] = lastReplicatedIndex + 1
							DPrintf("matchIndex is %d for %d", lastReplicatedIndex, i)
						}
						rf.checkCommitIndex()
					}
    			}
    		case <-time.After(150 * time.Millisecond): // timeout
    			// RPC timed out, do nothing		
    	}
}

func (rf *Raft) ticker() {
    for rf.killed() == false {
        // Calculate a new randomized timeout for the next election cycle
         electionTimeout := time.Duration(400 + rand.Int63()%300) * time.Millisecond
        time.Sleep(electionTimeout)  

        rf.mu.Lock()
        
        if rf.status != 2 && time.Since(rf.lastHeartBeat) > electionTimeout {
            
            // 2. CONVERT TO CANDIDATE (Start Election)
            rf.status = 1 // Assuming 1 is Candidate
            rf.currentTerm++
            rf.voteFor = rf.me
            rf.lastHeartBeat = time.Now() // Reset timer for this new election
            
            // Capture immutable variables for the RPCs
            term := rf.currentTerm
            lastLogIndex := rf.getLastLogIndex()
            lastLogTerm := rf.log[len(rf.log) - 1].Term
            rf.persist()
            // Unlock before sending RPCs (long operation)
            rf.mu.Unlock() 
            
            DPrintf("Server %d starting election in term %d\n", rf.me, term)

            var voteGranted int32 = 1 // Start with vote for self
            
            // 3. Send RequestVote RPCs to all peers concurrently
            for i := range rf.peers { // Use rf.peers directly
                if i == rf.me {
                    continue
                }
                
                // Launch RPC in a new goroutine
                go rf.sendRequestVoteWrapper(i, rf.me, term, lastLogIndex, lastLogTerm, &voteGranted)
            }
        } else {
            rf.mu.Unlock() // Unlock if no election was started
        }
    }
}

func (rf *Raft) leaderActivityLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if (rf.status != 2) {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
    		continue
		}
		peers := rf.peers
		appendEntriesArgs := make(map[int]AppendEntriesArgs, len(peers) - 1)
		installSnapshotArgs := make(map[int]InstallSnapshotArgs, len(peers) - 1)
		me := rf.me
		for i := range peers { 
			if (i == me) {
				continue
			}
			if (rf.nextIndex[i] > rf.lastIncludedIndex) {
				arg := rf.createAppendEntrieArgs(i)
				appendEntriesArgs[i] = arg
			} else {
				arg := rf.createInstallSnapShotArgs(i)
				installSnapshotArgs[i] = arg
			}
			
   		}
		rf.mu.Unlock()

		for i, arg := range appendEntriesArgs { 
			var reply AppendEntriesReply
			go rf.sendEntries(arg, reply, i)
   		}

		for i, arg := range installSnapshotArgs {
			// send snapshot
			var reply InstallSnapshotReply
			go rf.sendInstallSnapshot(arg, reply, i)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) sendRequestVoteWrapper(i int, me int, term int, lastLogIndex int, lastLogTerm int , voteGranted *int32) {
            args := RequestVoteArgs{
                Term:         term,
                CandidateId:  me,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }
            var reply RequestVoteReply
		
            // Wrap the blocking sendRequestVote call with a timeout
            done := make(chan bool, 1)
            go func() {
                ok := rf.sendRequestVote(i, &args, &reply)
                done <- ok
            }()
			
            select {
            case ok := <-done:
                if ok {
					rf.mu.Lock()
    				if (reply.Term > rf.currentTerm) {
    					    rf.currentTerm = reply.Term
    					    rf.status = 0
    					    rf.voteFor = -1
    					    rf.lastHeartBeat = time.Now()
							rf.persist()
							rf.mu.Unlock()
							return
    				}  
					if (reply.VoteGranted && rf.currentTerm == term) {
						    newVotes := atomic.AddInt32(voteGranted, 1)
							DPrintf("new votes : %d", newVotes)
    						if newVotes >= int32(len(rf.peers)/2 + 1) && rf.status == 1 {
    						    rf.status = 2
								rf.nextIndex = make([]int, len(rf.peers))
    							rf.matchIndex = make([]int, len(rf.peers))
								DPrintf("becomes leader : %d", rf.me)
    							for i := range rf.nextIndex {
    							    rf.nextIndex[i] = rf.getLastLogIndex() + 1
    							    rf.matchIndex[i] = rf.lastIncludedIndex
    							}
							} 
					} 
					rf.mu.Unlock()
				}
            case <-time.After(150 * time.Millisecond): // timeout
                // RPC timed out, do nothing
                return
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
    rf.lastHeartBeat = time.Now()

    rf.status = 0
	rf.currentTerm = 0
	rf.voteFor = -1
    rf.commitIndex = 0 
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.currentSnapShot = LocalSnapshot{}

	entry := LogContent{
        Term: rf.currentTerm,
    }
    //DPrintf("inside make append to rf.log")
    rf.log = append(rf.log, entry)
    
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
    //DPrintf("inside make append to rf.log length is %d", len(rf.log))
	// Your initialization code here (3A, 3B, 3C).
    
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    
	    if len(rf.currentSnapShot.Data) > 0 {
        go func() {
            applyCh <- raftapi.ApplyMsg{
                SnapshotValid: true,
                Snapshot: rf.currentSnapShot.Data,
                SnapshotIndex: rf.lastIncludedIndex,
                SnapshotTerm: rf.lastIncludedTerm,
            }
        }()
    }

	rand.Seed(time.Now().UnixNano() + int64(me))
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderActivityLoop()
	go rf.applier()
    
	return rf
}
