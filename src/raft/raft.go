// Abdullah Arif (Modified)

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command any) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

const (
	_minWaitTime = 800 * time.Millisecond
	_maxWaitTime = 1500 * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type (
	Raft struct {
		// TODO: mu        sync.Mutex // Lock to protect shared access to this peer's state
		persister *Persister // Object to hold this peer's persisted state
		me        int        // this peer's index into peers[]
		dead      int32      // set by Kill()
		peers     *Peers

		// Your data here (2A, 2B, 2C).
		// Look at the paper's Figure 2 for a description of what
		// state a Raft server must maintain.
		resetTimerCh chan empty
		term         *Term
		logs         *Logs
		CommitIndex  int
	}

	empty struct{}

	// as each Raft peer becomes aware that successive log entries are
	// committed, the peer should send an ApplyMsg to the service (or
	// tester) on the same server, via the applyCh passed to Make(). set
	// CommandValid to true to indicate that the ApplyMsg contains a newly
	// committed log entry.
	//
	// in part 2D you'll want to send other kinds of messages (e.g.,
	// snapshots) on the applyCh, but set CommandValid to false for these
	// other uses.
	//
	ApplyMsg struct {
		CommandValid bool
		Command      any
		CommandIndex int

		// For 2D:
		SnapshotValid bool
		Snapshot      []byte
		SnapshotTerm  int
		SnapshotIndex int
	}
)

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
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	// TODO: remove
	close(applyCh)

	rf := &Raft{
		peers: &Peers{
			clients: peers,
		},
		persister:    persister,
		me:           me,
		resetTimerCh: make(chan empty),
		term:         &Term{},
		logs:         &Logs{},
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	termNum, _, isLeader := rf.term.Info()
	return termNum, !rf.killed() && isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (*Raft) persist() {
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
func (*Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (*Raft) CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (*Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

type (
	// example RequestVote RPC arguments structure.
	// field names must start with capital letters!
	RequestVoteArgs struct {
		Term         int
		RequesterID  int
		LastLogIndex int
		LastLogTerm  int
	}

	// example RequestVote RPC reply structure.
	// field names must start with capital letters!
	RequestVoteReply struct {
		Term        int
		VoteGranted bool
	}

	AppendEntriesArgs struct {
		// leader’s term
		Term int
		// So follower can redirect clients
		LeaderID int
		// Index of log entry immediately preceding new ones
		PrevLogIndex int
		// Term of PrevLogIndex entry
		PrevLogTerm int
		// Log entries to store (empty for heartbeat; may send
		// more than one for efficiency)
		Entries []any
		// Leader’s CommitIndex
		LeaderCommit int
	}

	AppendEntriesReply struct {
		Term    int
		Success bool
	}
)

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("%d asked %d to vote for them!\n", args.RequesterID, rf.me)
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	curTermNum, curTermVotedFor, _ := rf.term.Info()

	reply.Term = curTermNum
	// Your code here (2B).
	if curTermNum > args.Term {
		//  We won't vote for anyone with a smaller term
		return
	}
	// Edge case if we already voted for the requesting candidate this term
	if curTermNum == args.Term && curTermVotedFor != nil && *curTermVotedFor == args.RequesterID {
		reply.VoteGranted = true
		rf.resetTimeout()
		return
	}

	// If our term is out of date we have to update regardless of our vote
	rf.term.Update(args.Term, nil)

	if !rf.logs.AreLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// Log must be up to date to receive our vote
		return
	}

	if voted := rf.term.Update(args.Term, &args.RequesterID); voted {
		reply.VoteGranted = true
		rf.resetTimeout()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	curTermNum := rf.term.Num()

	reply.Term = curTermNum
	// Your code here (2B).
	if curTermNum > args.Term {
		// If leader's term is outdated we don't listen
		return
	}

	if isOurLeader := rf.term.Update(args.Term, &args.LeaderID); !isOurLeader {
		log.Printf("%d is not the leader for %d, %d is\n", args.LeaderID, rf.me, rf.term.VotedFor())
		return
	}

	rf.resetTimeout()

	if !rf.logs.IsLogExactMatch(args.PrevLogIndex, args.PrevLogTerm) {
		log.Printf("%d tried to get heartbeat from %d, but logs were out of date\n", args.LeaderID, rf.me)
		// Log must be an exact match or we can't update
		return
	}

	// Most logs will empty s it's more efficient to leave this as nil
	var pendingLogs []Log
	for _, entry := range args.Entries {
		pendingLogs = append(pendingLogs, Log{
			term: curTermNum,
			cmd:  entry,
		})
	}
	rf.logs.AddEntries(args.PrevLogIndex, pendingLogs...)

	reply.Success = true

	// TODO: persist applied logs
	// TODO: handle committing the logs by sending message to channel
}

func (rf *Raft) resetTimeout() {
	select {
	case rf.resetTimerCh <- empty{}:
	default:
	}
}

//
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
//
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = rf.term.IsLeader()

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.term.Kill()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		randWait := rand.Int63n(int64(_maxWaitTime - _minWaitTime))
		electionTimeout := _minWaitTime + time.Duration(randWait)
		select {
		case <-time.After(electionTimeout):
			if rf.term.IsLeader() {
				continue
			}
			// Become candidate
			rf.becomeCandidate()
		case <-rf.resetTimerCh:
		}
	}
}

func (rf *Raft) becomeCandidate() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// TODO: make term store isCandidate - if we get an updated for votedFor - while we are a candidate then we update and stop being a candidate
	// We might two separate update- once for Request Vote and one for heart beat

	myTermNum, _, isLeader := rf.term.Info()

	myTermNum++

	if isLeader {
		return
	}
	rf.resetTimeout()

	curLogIndex, curLogTerm := rf.logs.LastAppliedInfo()

	log.Printf("%d is trying to become a leader for term %d\n", rf.me, myTermNum)

	var (
		wg            sync.WaitGroup
		gotVotes      int64 = 1
		requiredVotes       = (int64(rf.peers.Len()) / 2)
	)

	for i := 0; i < rf.peers.Len(); i++ {
		if rf.term.Num() >= myTermNum {
			// term is outdated we are done
			return
		}

		if atomic.LoadInt64(&gotVotes) > requiredVotes {
			// If we got enough to win finish early
			break
		}

		if i == rf.me {
			// we already voted for our selves
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			args := &RequestVoteArgs{
				Term:         myTermNum,
				RequesterID:  rf.me,
				LastLogIndex: curLogIndex,
				LastLogTerm:  curLogTerm,
			}
			reply := &RequestVoteReply{}

			if ok := rf.peers.SendRequestVote(i, args, reply); !ok {
				log.Printf("%d got no result from %d\n", rf.me, i)
				return
			}

			if reply.Term > myTermNum && rf.term.Update(reply.Term, nil) {
				return
			}

			atomic.AddInt64(&gotVotes, 1)
		}(i)
	}

	wg.Wait()

	if updated := rf.term.Update(myTermNum, &rf.me); !updated {
		// If we already voted for someone else we are done
		return
	}

	log.Printf("requiredVotes %d, gotVotes%d\n", requiredVotes, gotVotes)

	if gotVotes > requiredVotes {
		log.Printf("%d Became a leader for term %d!\n", rf.me, myTermNum)
		rf.term.BecomeLeader(rf.me, rf.peers, myTermNum, rf.logs)
	}
}
