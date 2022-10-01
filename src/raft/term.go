// Abdullah Arif

package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Tester doesn't allow us to send more than 10 beats/second
	_heartBeatWait = 100 * time.Millisecond
)

// Term is responsible for managing a state of a term of the Raft node.
type Term struct {
	mu          sync.Mutex
	num         int
	votedFor    *int
	termElected *int

	isDead int32
}

func (t *Term) VoteFor(newNum int, toVoteFor *int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	curNum, votedFor := t.num, t.votedFor
	if curNum > newNum {
		return false
	}

	if curNum == newNum && votedFor != nil {
		// If we already voted for someone this term, don't reverse the vote.
		return toVoteFor != nil && *votedFor == *toVoteFor
	}

	// if term we want to voteFor was greater or we didn't vote for anyone this term, then we grant our vote.
	t.votedFor = toVoteFor
	t.num = newNum
	return true
}

func (t *Term) UpdatedNum(newNum int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.num < newNum {
		t.votedFor = nil
		t.num = newNum
	}

	return t.num
}

func (t *Term) Num() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.num
}

func (t *Term) IsDead() bool {
	return atomic.LoadInt32(&t.isDead) == 1
}

func (t *Term) Kill() {
	atomic.StoreInt32(&t.isDead, 1)
}

func (t *Term) VotedFor() *int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.votedFor
}

func (t *Term) IsLeader() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.isLeader()
}

func (t *Term) isLeader() bool {
	return t.termElected != nil && *t.termElected == t.num
}

func (t *Term) Info() (num int, votedFor *int, isLeader bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.num, t.votedFor, t.isLeader()
}

func (t *Term) BecomeLeader(
	me int,
	peers *Peers,
	electedTermNum int,
	logs *Logs,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.num != electedTermNum || t.isLeader() {
		return false
	}

	t.termElected = &electedTermNum

	go t.updateFollowers(me, peers, electedTermNum, logs)

	return true
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

	var wg sync.WaitGroup
	// while we are in the term we got elected - keep trying to take the heartbeat
	for t.Num() == termNum && !t.IsDead() {
		wg.Add(n - 1)

		for i := 0; i < n; i++ {
			if i == me {
				continue
			}

			go func(i int) {
				defer wg.Done()

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
				peers.AppendEntries(i, args, reply)
				t.UpdatedNum(reply.Term)

				time.Sleep(_heartBeatWait)
			}(i)
		}

		wg.Wait()
	}
}
