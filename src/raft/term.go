// Abdullah Arif

package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	_minBroadcastTime = 500 * time.Nanosecond
	_maxBroadcastTime = 20 * time.Millisecond
)

type Term struct {
	mu          sync.Mutex
	num         int
	votedFor    *int
	termElected *int
}

func (t *Term) Update(termNum int, votedFor *int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	oldNum := t.num
	if oldNum > termNum {
		return false
	}

	// (curTerm <= newTerm) ==> we can update
	t.num = termNum

	// Our term got updated, so we get rid of the old vote
	if oldNum < termNum {
		t.votedFor = nil
		t.votedFor = votedFor
		return true
	}

	if votedFor == t.votedFor || (votedFor != nil && t.votedFor != nil && *votedFor == *t.votedFor) {
		// if we aren't trying to update our vote we are done
		return true
	}

	if t.votedFor != nil {
		// We can only update our vote once per term
		return false
	}

	t.votedFor = votedFor

	return true
}

func (t *Term) Num() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.num
}

func (t *Term) VotedFor() *int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.votedFor
}

func (t *Term) IsLeader() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	isLeader := t.termElected != nil && *t.termElected == t.num
	return isLeader
}

func (t *Term) Info() (num int, votedFor *int, isLeader bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	isLeader = t.termElected != nil && *t.termElected == t.num
	return t.num, t.votedFor, isLeader
}

func (t *Term) BecomeLeader(
	me int,
	peers *Peers,
	termNum int,
	logs *Logs,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.num != termNum {
		return
	}

	if t.votedFor == nil || *t.votedFor != me {
		return
	}

	t.termElected = &termNum

	go t.updateFollowers(me, peers, termNum, logs)
}

func (t *Term) updateFollowers(
	me int,
	peers *Peers,
	termNum int,
	logs *Logs,
) {
	n := peers.Len()
	// matchedIndex is the last known index we know is up to date
	// in the follower
	// TODO: matchedIndex := make([]int, n)
	// nextIndex is the index of the next log entry to send to
	// the follower
	nextIndex := make([]int, 0, n)
	for i := 0; i < n; i++ {
		nextIndex = append(nextIndex, logs.NumApplied())
	}

	randWait := rand.Int63n(int64(_maxBroadcastTime - _minBroadcastTime))
	heartBeatWait := _minBroadcastTime + time.Duration(randWait)
	ticker := time.NewTicker(heartBeatWait)
	// while we are in the term we got elected
	for t.Num() == termNum {
		for i := 0; i < n; i++ {
			logIndex := nextIndex[i]
			prevLog, _ := logs.LogAt(logIndex)
			args := &AppendEntriesArgs{
				Term:         termNum,
				LeaderID:     me,
				PrevLogIndex: logIndex,
				PrevLogTerm:  prevLog.term,
				// TODO: send actual commands not just heartbeats
				Entries:      nil,
				LeaderCommit: logs.CommitIndex(),
			}

			reply := &AppendEntriesReply{}
			go func(i int) {
				peers.AppendEntries(i, args, reply)
				t.Update(reply.Term, nil)
			}(i)
		}

		<-ticker.C
	}
}
