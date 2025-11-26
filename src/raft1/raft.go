package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
    status int // 0 follower , 1 candidate, 2 leader
	lastHeartBeat time.Time //

	currentTerm int
	voteFor int

	log []LogContent

	commitIndex int 
	lastApplied int
	
	nextIndex []int
	matchIndex []int

}

type LogContent struct {
	term int
	content interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm , rf.status == 2
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
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
    Content []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	if (args.Term < rf.currentTerm) {
		reply.Success = false;
		reply.Term = rf.currentTerm
		return
	} 

	if (args.Term > rf.currentTerm) {
		 rf.currentTerm = args.Term
    	rf.voteFor = -1   // clear last-term vote so we can vote in the new term
    	rf.status = 0     // step down to follower
	}

	reply.Success = true
    reply.Term = rf.currentTerm
}

// the struct itself.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
		DPrintf("%d grant voste for %d\n", rf.me, args.CandidateId)
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.status = 0
	} 
	reply.Term = rf.currentTerm
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		myLastLogIndex := len(rf.log) - 1
		myLastLogTerm := rf.log[myLastLogIndex].term
		if (args.LastLogTerm > myLastLogTerm) || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
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

	if (rf.status == 2) {

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

func (rf *Raft) sendEntries() {
	for rf.killed() == false {
		rf.mu.Lock()
        me := rf.me
        status := rf.status
		peers := rf.peers
        rf.mu.Unlock()
		if (status == 2) {
				for i := range peers {
    		    	if i == me {
    		        	continue
    		    	}
			
    		    	go func(server int) {
						rf.mu.Lock()
                    	args := AppendEntriesArgs{
                        	Term:     rf.currentTerm, // capture latest term
                        	LeaderId: rf.me,
                    	}
                    	rf.mu.Unlock()

    		        	var reply AppendEntriesReply
						
    		        	// Wrap the blocking sendRequestVote call with a timeout
    		        	done := make(chan bool, 1)
    		        	go func() {
    		            	ok := rf.sendAppendEntries(server, &args, &reply)
    		            	done <- ok
    		        	}()
					
    		        	select {
    		        	case ok := <-done:
    		            	if ok {
    		                	rf.mu.Lock()
                            	if reply.Term > rf.currentTerm {
									rf.lastHeartBeat = time.Now()
                                	rf.status = 0
                                	rf.voteFor = -1
                                	rf.currentTerm = reply.Term
                            	}
                            	rf.mu.Unlock()
    		            	}
    		        	case <-time.After(150 * time.Millisecond): // timeout
    		            	// RPC timed out, do nothing
    		            	return
    		        	}
    		    }(i)
    		}
    	}
		time.Sleep(150 * time.Millisecond) // small tick to prevent busy loop
		}
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		        // Small tick: check often (10ms)
		        // Randomized election timeout (300-900ms)

        electionTimeout := time.Duration(300 + rand.Int63()%600) * time.Millisecond

        time.Sleep(electionTimeout)

        rf.mu.Lock()
        lastHeartBeat := rf.lastHeartBeat
        status := rf.status
        me := rf.me
        peers := rf.peers
        rf.mu.Unlock()


        // Start election if follower and timeout exceeded
        if status != 2 && time.Since(lastHeartBeat) > electionTimeout {
            // Convert to candidate
			DPrintf("starting hear beat\n")
            rf.mu.Lock()
            rf.status = 1
            rf.currentTerm++
            rf.voteFor = me
            term := rf.currentTerm
            lastLogIndex := len(rf.log) - 1
            lastLogTerm := rf.log[lastLogIndex].term
			rf.lastHeartBeat = time.Now()
            rf.mu.Unlock()
            
			var voteGranted int32 = 1

            // Send RequestVote RPCs to all peers concurrently
    		for i := range peers {
    		    if i == me {
    		        continue
    		    }
    		    go func(server int) {
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
    		            ok := rf.sendRequestVote(server, &args, &reply)
    		            done <- ok
    		        }()
					
    		        select {
    		        case ok := <-done:
    		            if ok {
    		                if reply.VoteGranted {
								    newVotes := atomic.AddInt32(&voteGranted, 1)
    								rf.mu.Lock()
    								if newVotes >= int32(len(peers)/2 + 1) && rf.status == 1 {
    								    rf.status = 2
										rf.nextIndex = make([]int, len(rf.peers))
    									rf.matchIndex = make([]int, len(rf.peers))
    									for i := range rf.nextIndex {
    									    rf.nextIndex[i] = len(rf.log)
    									    rf.matchIndex[i] = 0
    									}
    									DPrintf("%d became leader in term %d\n", rf.me, rf.currentTerm)
    								}
    								rf.mu.Unlock()
							} else if reply.Term > term {
								rf.mu.Lock()
    							if reply.Term > rf.currentTerm {
    							    rf.currentTerm = reply.Term
    							    rf.status = 0
    							    rf.voteFor = -1
    							    rf.lastHeartBeat = time.Now()
    							}
    							rf.mu.Unlock()
							}
    		            }
    		        case <-time.After(150 * time.Millisecond): // timeout
    		            // RPC timed out, do nothing
    		            return
    		        }
    		    }(i)
    		}
    	}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
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

	entry := LogContent{
        term: rf.currentTerm,
    }
    
    rf.log = append(rf.log, entry)
    
	// Your initialization code here (3A, 3B, 3C).
    
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    
	go rf.sendEntries()

    
	return rf
}
