package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
//import "//fmt"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const (
	LEADER = 1
	FOLLOWER = 2
	CANDIDATE = 3
	HEARTBEATINTERVAL = 50 * time.Millisecond

)
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct{
	LogIndex	int
	LogTerm		int
	LogCmd		interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//according raft paper figure2
	currentTerm		int
	votedFor		int
	//log entry
	log				[]LogEntry

	//all server
	commitIndex		int
	lastApplied		int

	//leader state
	//re-init after election
	//对每一个server，下一个需要发给它的日志的索引值
	nextIndex 		[]int
	//对每一个server，已经发给他的日志的索引值
	matchIndex		[]int

	//channel
	chanCommit		chan bool
	chanHeartbeat 	chan bool
	chanGrantVote 	chan bool
	chanLeader 		chan bool
	chanApply		chan ApplyMsg

	//other
	state 			int
	voteCount		int

	//定时器
	timer			*time.Timer
}

//获取日志信息，验证安全性
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].LogTerm
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
	//return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//通过RPC传输的信息
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//轮次 id
	Term			int
	CandidateId		int

	//日志信息，验证安全性
	LastLogTerm		int
	LastLogIndex	int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted	bool
}

//AppendEntries RPC
type AppendEntryArgs struct{
	//leader's term
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	//empty if it is a heartbeat
	//may more than one: batch
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntryReply struct{
	Term			int
	//true if follower cantained entry matching 
	//prevLogIndex and prevLogTerm
	Success			bool
	NextIndex		int
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	////fmt.Printf("%v recv the heartbeat from %v\n", rf.me, args.LeaderId)
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1
		return 
	}
	//心跳
	//rpc本身性质可验证心跳是否成功
	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	//主从日志一致性同步

	//1. 保证Leader已发送日志id >= Follower已有的日志id
	//args.PrevLogIndex 需要等于本机的LastLogIndex
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return 
	}
	//2. 保证index与任期都一致
	//id相同，任期不同的场景：
	//原文figure7
	//leader a: 	1114455666
	//follower b:	1114444
	//id = 5时，leader与follower任期不同，可能是由于当时b是leader，但是在突然挂掉了。
	//之后leader变成了a, b在一段时间之后恢复。


	//3. baseIndex != 0 是因为之后会加入快照
	//baseIndex := rf.log[0].LogIndex
	term := rf.log[args.PrevLogIndex/* - baseIndex*/].LogTerm

	//不断回退尝试
	//if args.PrevLogIndex > baseIndex {
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i > -1; i-- {
				//1. note: (args.PrevLogIndex, rf.getLastLogIndex] 
				// 这部分不必检查，一定会被丢弃。
				//2. 理论上这里不是最高效，因为是一步一步的回退
				//论文中指出，这样做的原因是实际生产中，不存在过多的日志不一致情况
				if rf.log[i].LogTerm != args.PrevLogTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			return 
		}
	//}

	//追加日志
	////fmt.Printf("before append len: %v\n", len(rf.log))
	// !!!!!!
	// 一定记住[left, right)....坑了一天
	rf.log = rf.log[: args.PrevLogIndex + 1 /*- baseIndex + 1*/]
	rf.log = append(rf.log, args.Entries...)
	////fmt.Printf("after append len: %v\n", len(rf.log))
	//更新reply
	reply.Success = true
	reply.NextIndex = rf.getLastLogIndex() + 1

	//提交
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastLogIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return 

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		currentTerm := rf.currentTerm
		replyTerm := reply.Term
		//已经发现自己不是最新轮次，已经转换为FOLLOWER
		if rf.state != LEADER {
			return ok
		}
		if currentTerm != args.Term {
			return ok
		}

		//状态转换
		if currentTerm < replyTerm {
			rf.currentTerm = replyTerm
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		//Follower成果append
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			//当前日志不同步，需要进一步同步
			////fmt.Println("!!!!!!!!!!!!!!!!!!")
			rf.nextIndex[server] = reply.NextIndex
			////fmt.Printf("update nextid by reply. nextid: %v\n", rf.nextIndex[server])
		}

	}
	return ok
}

func (rf *Raft) boardcastAppendEntries(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	couldCommited := rf.commitIndex
	lastIndex := rf.getLastLogIndex()

	//log commit逻辑
	//1.超过半数匹配的日志可以提交
	//2.日志n可提交，则n之前的日志都可以提交
	for i := couldCommited + 1; i <= lastIndex; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2 * num > len(rf.peers) {
			couldCommited = i
		} 
	}
	if couldCommited != rf.commitIndex {
		rf.commitIndex = couldCommited
		rf.chanCommit <- true
	}


	//boardcast
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER{
			var args AppendEntryArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			//need fix if add snapshot
			////fmt.Printf("prevLogIndex: %v\n", args.PrevLogIndex)
			args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
			args.LeaderCommit = rf.commitIndex
			args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1 :]))
			copy(args.Entries, rf.log[args.PrevLogIndex + 1 :])
			go func(i int, args AppendEntryArgs){
				var reply AppendEntryReply
				rf.sendAppendEntries(i, &args, &reply)
			}(i, args)
		}
	}
	////fmt.Println("----------------------")
}
//
// example RequestVote RPC handler.
//

//RPC实现
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//fmt.Printf("%v recv the vote request from %v\n", rf.me, args.CandidateId)
	reply.VoteGranted = false;

	//case1: candidate的任期小于本机
	//更新reply，返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	//case2：本机任期小于candidate任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//更新reply任期
	reply.Term = rf.currentTerm

	term := rf.getLastLogTerm()
	index := rf.getLastLogIndex()
	uptodate := false

	//以下两个逻辑来自论文中5.4节安全性
	//candadite的日志信息必须比本机的要新
	//防止新的Leader还要从Follower中同步日志
	//（这种情况常发生于Leader宕机，日志存在，但是没有与Followers一致）
	if args.LastLogTerm > term {
		uptodate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		uptodate = true
	}

	//更新本机状态，向chan发送信息，更新reply
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		rf.chanGrantVote <- true
		rf.state = FOLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != CANDIDATE {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		//获取选票失败，因为有更新的Term
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}

		//获取了选票，判断是否能够成为新的Leader
		if reply.VoteGranted {
			rf.voteCount++
			////fmt.Printf("%v recv one vote\n", rf.me)
			if rf.voteCount > len(rf.peers) / 2 && rf.state == CANDIDATE {
				rf.state = FOLLOWER
				rf.chanLeader <- true
			}
		}
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.GetState()
	index := -1
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogCmd: command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) boardcastRequestVote() {
	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.Term = rf.currentTerm

	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//广播，request votes
	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			//并发
			go func(i int) {
				var reply RequestVoteReply;
				////fmt.Printf("%v request vote to %v\n", rf.me, i) 
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//init
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool)
	rf.chanGrantVote = make(chan bool)
	rf.chanHeartbeat = make(chan bool)
	rf.chanLeader = make(chan bool)
	rf.chanApply = applyCh

	rf.timer = time.NewTimer(time.Duration(rand.Int63() % 330 + 550) * time.Millisecond) 

	go func() {
		for{
			switch rf.state {
			case LEADER:

				//发送心跳或者日志
				rf.boardcastAppendEntries()
				time.Sleep(HEARTBEATINTERVAL)
		
			case FOLLOWER: 
				select{
				case <-rf.chanHeartbeat:
					rf.timer.Reset(time.Duration(rand.Int63() % 330 + 550) * time.Millisecond)
				case <-rf.chanGrantVote:
					//rf.mu.Lock()
					////fmt.Printf("%v 's term is %v", rf.me, rf.currentTerm)
					//rf.mu.Unlock()
				//case <-rf.chanCommit:
				//case <-time.After(time.Duration(rand.Int63() % 330 + 550) * time.Millisecond):
				case <-rf.timer.C:
					//状态切换成CANDIDATE
					//fmt.Printf("%v turn to candidate state\n", rf.me)
					rf.state = CANDIDATE
				}
			case CANDIDATE:
				//初始化candidate状态
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()
				//广播request vote
				go rf.boardcastRequestVote()
				select {
				case <- rf.chanLeader:
					rf.mu.Lock()
					rf.state = LEADER
					//fmt.Printf("%v become new Leader\n", rf.me)
					////fmt.Printf("%v 's term is %v\n", rf.me, rf.currentTerm)
					//更新安全性索引数组
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						////fmt.Printf("rf[%v] = %v\n", i, rf.nextIndex[i])
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				case <- rf.chanHeartbeat:
				case <- time.After(time.Duration(rand.Int63() % 330 + 550) * time.Millisecond):
				}
			}
		}
	}()
	//并发写log
	go func() {
		for {
			select {
			case <- rf.chanCommit:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					////fmt.Printf("i: %v, log.len: %v, commitId: %v\n", i, len(rf.log), rf.commitIndex)
					msg := ApplyMsg{Index: i, Command: rf.log[i].LogCmd}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
