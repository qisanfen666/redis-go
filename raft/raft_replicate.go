package raft

// import (
// 	"log"
// 	"redis-go/store"
// 	"strings"
// )

// type AppendArgs struct {
// 	Term         int        `json:"term"`
// 	LeaderID     int        `json:"leaderId"`
// 	Entries      []LogEntry `json:"entries,omitempty"` // 使用omitempty避免空数组
// 	PrevLogIndex int        `json:"prevLogIndex"`
// 	PrevLogTerm  int        `json:"prevLogTerm"`
// 	LeaderCommit int        `json:"leaderCommit"`
// 	Method       string     `json:"method"`
// }

// type AppendReply struct {
// 	Term    int  `json:"term"`
// 	Success bool `json:"success"`
// 	LastLog int  `json:"lastLog"`
// }

// // leader 广播日志
// func (rn *RaftNode) broadcastAppendEntries() {
// 	rn.mu.Lock()
// 	term := rn.currentTerm
// 	selfLog := rn.log
// 	commit := rn.commitIndex
// 	nextIndex := make([]int, len(rn.nextIndex))
// 	copy(nextIndex, rn.nextIndex)
// 	role := rn.role
// 	rn.mu.Unlock()

// 	if role != RoleLeader {
// 		return
// 	}

// 	for i := range rn.peers {
// 		go rn.sendAppendEntries(i, term, commit, selfLog, nextIndex[i])
// 	}
// }

// func (rn *RaftNode) sendAppendEntries(peerIdx int, term int, commit int, selfLog []LogEntry, nextIndex int) {
// 	if rn.getRole() != RoleLeader {
// 		rn.mu.RUnlock()
// 		return
// 	}

// 	if nextIndex < 0 {
// 		nextIndex = 0
// 	}

// 	if nextIndex >= len(selfLog) {
// 		nextIndex = len(selfLog)
// 	}

// 	prevLogIndex := nextIndex - 1
// 	prevLogTerm := 0
// 	if prevLogIndex >= 0 && prevLogIndex < len(selfLog) {
// 		prevLogTerm = selfLog[prevLogIndex].Term
// 	}

// 	var entries []LogEntry
// 	if nextIndex < len(selfLog) {
// 		entries = make([]LogEntry, len(selfLog)-nextIndex)
// 		copy(entries, selfLog[nextIndex:])
// 	}

// 	args := AppendArgs{
// 		Term:         term,
// 		LeaderID:     rn.id,
// 		Entries:      entries,
// 		PrevLogIndex: prevLogIndex,
// 		PrevLogTerm:  prevLogTerm,
// 		LeaderCommit: commit,
// 		Method:       "AppendEntries",
// 	}

// 	var reply AppendReply
// 	err := rn.callPeer(peerIdx, "AppendEntries", &args, &reply)
// 	if err != nil {
// 		return
// 	}

// 	rn.mu.Lock()
// 	defer rn.mu.Unlock()

// 	if reply.Term > rn.currentTerm {
// 		rn.currentTerm = reply.Term
// 		rn.role = RoleFollower
// 		rn.votedFor = -1
// 		return
// 	}

// 	if rn.role != RoleLeader || args.Term != term {
// 		return
// 	}

// 	if reply.Success {
// 		rn.matchIndex[peerIdx] = reply.LastLog
// 		if rn.matchIndex[peerIdx] < 0 {
// 			rn.matchIndex[peerIdx] = 0 // 防止负值
// 		}
// 		rn.nextIndex[peerIdx] = reply.LastLog + 1
// 		rn.tryCommit()
// 	} else {
// 		if rn.nextIndex[peerIdx] > 0 {
// 			rn.nextIndex[peerIdx]--
// 		}
// 	}
// }

// // follower 处理
// func (rn *RaftNode) handleAppendEntries(args AppendArgs) AppendReply {
// 	rn.mu.Lock()
// 	defer rn.mu.Unlock()

// 	//log.Printf("[TCP] node %d recv AppendEntries (term %d)", rn.id, args.Term)
// 	if args.Term < rn.currentTerm {
// 		//log.Printf("[TCP] node %d reject AppendEntries (term %d)111", rn.id, args.Term)
// 		return AppendReply{Term: rn.currentTerm, Success: false}
// 	}

// 	select {
// 	case rn.resetElectionChan <- struct{}{}:
// 	default:
// 	}

// 	if args.Term > rn.currentTerm {
// 		rn.currentTerm = args.Term
// 		rn.role = RoleFollower
// 		rn.votedFor = -1
// 		//log.Printf("[StepDown] node %d -> term %d, now Follower", rn.id, rn.currentTerm)
// 	}

// 	// 日志一致性检查
// 	if args.PrevLogIndex >= 0 {
// 		if args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
// 			//log.Printf("[Reject] node %d reject AppendEntries (term %d)222", rn.id, args.Term)
// 			return AppendReply{Term: rn.currentTerm, Success: false}
// 		}
// 	}

// 	//追加日志
// 	if len(args.Entries) > 0 {
// 		// 截断冲突的日志条目
// 		newLog := make([]LogEntry, 0, len(rn.log)+len(args.Entries))
// 		newLog = append(newLog, rn.log[:args.PrevLogIndex+1]...)
// 		newLog = append(newLog, args.Entries...)
// 		rn.log = newLog
// 	}

// 	// 提交日志
// 	if args.LeaderCommit > rn.commitIndex {
// 		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
// 		rn.applyCommitted()
// 	}

// 	return AppendReply{Term: rn.currentTerm, Success: true, LastLog: len(rn.log) - 1}
// }

// func (rn *RaftNode) tryCommit() {
// 	for idx := rn.commitIndex + 1; idx < len(rn.log); idx++ {
// 		if idx < 0 {
// 			//log.Println("111")
// 			continue
// 		}
// 		if rn.log[idx].Term != rn.currentTerm {
// 			//log.Println("222")
// 			continue
// 		}
// 		//log.Println("333")
// 		count := 1
// 		for i := 0; i < len(rn.peers); i++ {
// 			if rn.matchIndex[i] >= idx {
// 				count++
// 			}
// 		}
// 		if count > len(rn.peers)/2 {
// 			// log.Println("444")
// 			// for i, alog := range rn.log {
// 			// 	log.Printf("%d: %s", i, alog.Command)
// 			// }
// 			if idx >= rn.commitIndex {
// 				rn.commitIndex = idx
// 				//log.Printf("[Commit] node %d commit index -> %d (cmd=%s) commitIndex=%d`",
// 				//rn.id, idx, rn.log[idx].Command, rn.commitIndex)

// 				//提交后立即应用
// 				go rn.applyCommitted()
// 			}
// 		}
// 	}
// }

// func (rn *RaftNode) applyCommitted() {
// 	log.Printf("555 %d %d", rn.lastApplied, rn.commitIndex)
// 	for rn.lastApplied < rn.commitIndex {
// 		rn.lastApplied++
// 		log.Printf("[apply] node %d apply index -> %d (cmd=%s)]", rn.id, rn.lastApplied, rn.log[rn.lastApplied].Command)
// 		entry := rn.log[rn.lastApplied]
// 		if entry.Command != "NOOP" {
// 			applyCmd(entry.Command)
// 		}
// 	}
// }

// func applyCmd(cmd string) {
// 	fields := strings.Fields(cmd)
// 	log.Printf("[Apply] cmd=%v", fields)
// 	if len(fields) == 3 && fields[0] == "SET" {
// 		store.Store.Add(fields[1], fields[2])
// 	}
// }
