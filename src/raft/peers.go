// Abdullah Arif

package raft

import (
	"6.824/labrpc"
)

// Peers is a helper class that helps manage making network calls to our peer.
type Peers struct {
	clients []*labrpc.ClientEnd // RPC end points of all peers
}

func (p *Peers) Call(endpoint string, i int, args, reply any) bool {
	return p.clients[i].Call(endpoint, args, reply)
}

func (p *Peers) Len() int {
	return len(p.clients)
}

// i is the index of the target server in p.clients[].
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
//
func (p *Peers) SendRequestVote(i int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return p.Call("Raft.RequestVote", i, args, reply)
}

func (p *Peers) AppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return p.Call("Raft.AppendEntries", i, args, reply)
}
