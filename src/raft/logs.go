// Abdullah Arif

package raft

import (
	"sync"
)

type (
	// Logs is a struct that represents the logs sent by a server. Keeping it separate helps,
	// make it easier to deal with the heartbeat logic separately.
	Logs struct {
		mu      sync.RWMutex
		applied []Log
		// lastApplied we can derive from applied
		commitIndex int
	}

	Log struct {
		term int
		cmd  any
	}
)

func (l *Logs) AddEntries(from int, logs ...Log) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i, log := range logs {
		if i+from >= len(l.applied) {
			l.applied = append(l.applied, log)
			continue
		}

		oldLog := l.applied[i+from]
		l.applied[i+from] = log
		if log.term != oldLog.term {
			// truncate any conflicting entries
			l.applied = l.applied[:i+from+1]
		}
	}
}

func (l *Logs) AreLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	curLogIndex, curLogTerm := l.LastAppliedInfo()

	return lastLogIndex >= curLogIndex && lastLogTerm >= curLogTerm
}

func (l *Logs) LastAppliedInfo() (curLogIndex, curLogTerm int) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	curLogIndex = l.commitIndex
	curLogTerm = -1
	if len(l.applied) > 0 {
		curLogTerm = l.applied[curLogIndex].term
	}

	return curLogIndex, curLogTerm
}

func (l *Logs) LogAt(i int) (Log, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.applied) == 0 {
		return Log{}, false
	}

	return l.applied[i], true
}

func (l *Logs) IsLogExactMatch(prevLogIndex, prevLogTerm int) bool {
	logTerm, exists := l.getLoggedInfoFor(prevLogIndex)

	// TODO: handle logs with `return exists && logTerm == prevLogTerm`
	return !exists || logTerm == prevLogTerm
}

func (l *Logs) getLoggedInfoFor(logIndex int) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if logIndex < 0 || logIndex >= len(l.applied) {
		return 0, false
	}

	return l.applied[logIndex].term, true
}

func (l *Logs) NumApplied() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.applied) - 1
}

func (l *Logs) CommitIndex() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.commitIndex
}
