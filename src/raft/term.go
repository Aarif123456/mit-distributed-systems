// Abdullah Arif

package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// Tester doesn't allow us to send more than 10 beats/second
	_minBroadcastTime = 150 * time.Millisecond
	_maxBroadcastTime = 250 * time.Millisecond
)

type Term struct {
	mu          sync.RWMutex
	num         int
	votedFor    *int
	termElected *int
}

func (t *Term) Update(newNum int, votedFor *int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	oldNum := t.num
	if oldNum > newNum {
		return false
	}

	// (oldNum <= newNum) ==> we can update
	t.num = newNum

	// Our term got updated, so we get rid of the old vote
	if oldNum < newNum {
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
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.num
}

func (t *Term) VotedFor() *int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.votedFor
}

func (t *Term) IsLeader() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	isLeader := t.termElected != nil && *t.termElected == t.num
	return isLeader
}

func (t *Term) Info() (num int, votedFor *int, isLeader bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	isLeader = t.termElected != nil && *t.termElected == t.num
	return t.num, t.votedFor, isLeader
}

func (t *Term) BecomeLeader(
	me int,
	peers *Peers,
	termElected int,
	logs *Logs,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.num != termElected {
		return
	}

	if t.votedFor == nil || *t.votedFor != me {
		return
	}

	t.termElected = &termElected

	go t.updateFollowers(me, peers, termElected, logs)
}

func (t *Term) updateFollowers(
	me int,
	peers *Peers,
	termElected int,
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
	for t.Num() == termElected {
		for i := 0; i < n; i++ {
			logIndex := nextIndex[i]
			prevLog, _ := logs.LogAt(logIndex)
			args := &AppendEntriesArgs{
				Term:         termElected,
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
				if reply.Term > termElected {
					t.Update(reply.Term, nil)
				}
			}(i)
		}

		<-ticker.C
	}
}

func (t *Term) Kill() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.termElected = nil
	t.num = 0
}
