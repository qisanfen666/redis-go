package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	RoleFollower  = 0
	RoleCandidate = 1
	RoleLeader    = 2
	HeartbeatMS   = 150
	ElectionBase  = 3000
	ElectionRand  = 5000
)

type RaftNode struct {
	mu       sync.RWMutex
	id       int
	selfAddr string
	peers    []string
	role     int

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	pc []*peerClient

	//leader
	nextIndex  []int
	matchIndex []int

	//网络
	ln net.Listener

	//定时器
	resetElectionChan chan struct{}
	applyChan         chan ApplyMsg
	electionTimer     *time.Timer
	shutdownChan      chan struct{}
}

type LogEntry struct {
	Term    int    `json:"term"`
	Command string `json:"cmd"`
}

type ApplyMsg struct {
	Command string
}

// 创建新节点
func NewRaftNode(id int, addr string, peers []string) *RaftNode {
	rn := &RaftNode{
		id:       id,
		selfAddr: addr,
		peers:    peers,
		role:     RoleFollower,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 0, 100),

		commitIndex: -1,
		lastApplied: -1,

		pc: make([]*peerClient, len(peers)),

		nextIndex:  nil,
		matchIndex: nil,

		resetElectionChan: make(chan struct{}, 1),
		applyChan:         make(chan ApplyMsg, 1024),
		shutdownChan:      make(chan struct{}),
	}
	for i, p := range peers {
		rn.pc[i] = newPeerClient(p)
	}
	go rn.run()
	return rn
}

func (rn *RaftNode) Shutdown() {
	close(rn.shutdownChan)
	if rn.ln != nil {
		rn.ln.Close()
	}
}

func (rn *RaftNode) run() {
	rn.startListener()
	for {
		select {
		case <-rn.shutdownChan:
			return
		default:
			switch rn.getRole() {
			case RoleFollower:
				rn.followerLoop()
			case RoleCandidate:
				rn.candidateLoop()
			case RoleLeader:
				rn.leaderLoop()
			}
		}

	}
}

/*=========角色控制==========*/
func (rn *RaftNode) followerLoop() {
	log.Printf("[Follower] node %d enter followerLoop (term %d)", rn.id, rn.currentTerm)
	rn.electionTimer = time.NewTimer(randDuration(ElectionBase, ElectionRand))
	defer rn.electionTimer.Stop()

	for {
		select {
		case <-rn.shutdownChan:
			return
		case <-rn.electionTimer.C:
			log.Printf("[Follower] node %d election timer FIRED (term %d)", rn.id, rn.currentTerm)
			rn.convertToCandidate()
			return
		case <-rn.resetElectionChan:
			log.Printf("[Follower] node %d received heartbeat, reset election timer", rn.id)
			if !rn.electionTimer.Stop() {
				select {
				case <-rn.electionTimer.C:
				default:
				}
			}
			rn.electionTimer.Reset(randDuration(ElectionBase, ElectionRand))
		}
	}
}

func (rn *RaftNode) candidateLoop() {
	log.Printf("[Candidate] node %d start election", rn.id)
	rn.startElection(rn.currentTerm, len(rn.log))
}

func (rn *RaftNode) leaderLoop() {
	ticker := time.NewTicker(HeartbeatMS * time.Millisecond)
	defer ticker.Stop()

	// if len(rn.log) == 0 || rn.log[len(rn.log)-1].Command != "NOOP" {
	// 	rn.log = append(rn.log, LogEntry{
	// 		Term:    rn.currentTerm,
	// 		Command: "NOOP",
	// 	})
	// }

	for {
		select {
		case <-rn.shutdownChan:
			return
		case <-ticker.C:
			if rn.role != RoleLeader {
				return
			}
			rn.broadcastAppendEntries()
		}
	}
}

func (rn *RaftNode) convertToCandidate() {
	rn.mu.Lock()
	rn.role = RoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.id
	term := rn.currentTerm
	logLen := len(rn.log)
	rn.mu.Unlock()

	log.Printf("[Candidate] node %d >>>>>>>>>> BECOME CANDIDATE at term %d <<<<<<<<<", rn.id, rn.currentTerm)

	go rn.startElection(term, logLen)
}

func (rn *RaftNode) convertToLeader() {
	if rn.role != RoleCandidate || rn.votedFor != rn.id {
		log.Printf("[WARN] Node %d invalid state for leadership (role=%d, votedFor=%d)", rn.id, rn.role, rn.votedFor)
		return
	}

	rn.mu.Lock()
	rn.role = RoleLeader
	peersCount := len(rn.peers)
	rn.nextIndex = make([]int, len(rn.peers))
	rn.matchIndex = make([]int, len(rn.peers))

	for i := range peersCount {
		rn.nextIndex[i] = len(rn.log)
		rn.matchIndex[i] = 0
	}

	rn.mu.Unlock()

	rn.broadcastAppendEntries()

	log.Printf("[LEADER] node %d >>>>>>>>>> BECOME LEADER at term %d <<<<<<<<<", rn.id, rn.currentTerm)
}

/*=========提案接口=========*/
func (rn *RaftNode) Propose(cmd string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader {
		log.Printf("[Propose] reject, not leader")
		return
	}
	log.Printf("[Propose] leader accept cmd=%s", cmd)
	rn.log = append(rn.log, LogEntry{
		Term:    rn.currentTerm,
		Command: cmd,
	})

	rn.tryCommit()

	go rn.broadcastAppendEntries()
}

/*=========网络层=========*/
func (rn *RaftNode) startListener() {
	ln, err := net.Listen("tcp", rn.selfAddr)
	if err != nil {
		panic(err)
	}
	rn.ln = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-rn.shutdownChan:
					return
				default:
					log.Printf("[ServeConn] accept error: %v", err)
					continue
				}
			}
			go rn.serveConn(conn)
		}
	}()
}

func (rn *RaftNode) serveConn(conn net.Conn) {
	defer conn.Close()
	for {
		// 读长度头
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header)

		// 读 JSON 体
		body := make([]byte, length)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		//log.Printf("[ServeConn] recv frame len=%d body=%s", length, string(body))
		// 路由方法
		var reply interface{}
		switch {
		case bytes.Contains(body, []byte("RequestVote")):
			//log.Printf("[ServeConn] routing RequestVote")
			var args VoteArgs
			json.Unmarshal(body, &args)
			reply = rn.handleRequestVote(args)
		case bytes.Contains(body, []byte("AppendEntries")):
			var args AppendArgs
			json.Unmarshal(body, &args)
			reply = rn.handleAppendEntries(args)
		default:
			continue
		}
		writeFrame(conn, reply)
	}
}

// func (rn *RaftNode) appendNoop() {
// 	rn.Propose("NOOP")
// }

func (rn *RaftNode) callPeer(idx int, method string, args interface{}, reply interface{}) error {
	//log.Printf("[Dial] node %d dialing %s", rn.id, rn.pc[idx].addr)
	return rn.pc[idx].call(method, args, reply)
}

/*=========工具函数=========*/
func (rn *RaftNode) getRole() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.role
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.role == RoleLeader
}

func randDuration(base, randRange int) time.Duration {
	return time.Duration(base+rand.Intn(randRange)) * time.Millisecond
}
