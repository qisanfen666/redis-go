package raft

import (
	"sync"
)

type VoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  int    `json:"candidateId"`
	Method       string `json:"method"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type VoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
	VoterID     int  `json:"voterId"`
}

// 发起投票请求
func (rn *RaftNode) startElection(term int, lastLogIndex int) {
	lastLogTerm := 0
	if lastLogIndex > 0 {
		rn.mu.RLock()
		if lastLogIndex-1 < len(rn.log) {
			lastLogTerm = rn.log[lastLogIndex-1].Term
		}
		rn.mu.RUnlock()
	}

	args := VoteArgs{
		Term:         term,
		CandidateID:  rn.id,
		Method:       "RequestVote",
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	votes := 1
	votesNeeded := (len(rn.peers)+1)/2 + 1
	voteChan := make(chan bool, len(rn.peers))

	var wg sync.WaitGroup
	for i := range rn.peers {
		wg.Add(1)
		go func(idx int) {
			reply := VoteReply{}
			if err := rn.callPeer(idx, "RequestVote", &args, &reply); err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()
				if reply.Term > rn.currentTerm {
					rn.currentTerm = reply.Term
					rn.role = RoleFollower
					rn.votedFor = -1
					return
				}
				if rn.role == RoleCandidate && reply.VoteGranted && reply.Term == term {
					voteChan <- true
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(voteChan)
	}()

	for range voteChan {
		votes++
		if votes >= votesNeeded {
			rn.convertToLeader()
			return
		}
	}

	rn.mu.Lock()
	if rn.role == RoleCandidate && rn.currentTerm == term {
		rn.role = RoleFollower
	}
	rn.mu.Unlock()
}

// 处理投票请求并发送回复
func (rn *RaftNode) handleRequestVote(args VoteArgs) VoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		return VoteReply{
			Term:        rn.currentTerm,
			VoteGranted: false,
			VoterID:     rn.id,
		}
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = RoleFollower
		rn.votedFor = -1
	}

	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}
	logUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	grant := false

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateID) && logUpToDate {
		rn.votedFor = args.CandidateID
		grant = true
		select {
		case rn.resetElectionChan <- struct{}{}:
		default:
		}
	}

	return VoteReply{
		Term:        rn.currentTerm,
		VoteGranted: grant,
		VoterID:     rn.id,
	}
}

// 处理投票回复
// func (rn *RaftNode) handleVoteReply(rep VoteReply) {
// 	rn.mu.RLock()
// 	myTerm := rn.currentTerm
// 	myRole := rn.role
// 	rn.mu.RUnlock()

// 	if rep.Term == myTerm && myRole == RoleCandidate && rep.VoteGranted {
// 		log.Printf("[VoteRecv] node %d got vote from node %d for term %d", rn.id, rep.VoterID, rep.Term)
// 		select {
// 		case rn.voteChan <- true:
// 		default:
// 		}
// 	}
// }
