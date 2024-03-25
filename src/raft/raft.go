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

import (
	"math/rand"
	"sync"
)
import "sync/atomic"
import "../labrpc"
import "time"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	role        int           //服务器角色0：追随者 1：候选人 2：领导人
	heartBeat   chan struct{} //心跳信号，收到该信号则表示接受了心跳（也就是有领导人在）
	peerNum     int           //总的peer数量
	currentTerm int           //服务器知道的最新任期号
	votedFor    int           //在当前任期内收到选票的候选人id（即pees下标），也就是已经认谁为大哥了，且自己的任期也改为大哥的任期了

	overTime time.Duration
	timer    *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == 2
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 投票请求的请求参数
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	//2A
	Term        int //候选人的任期号
	CandidateId int //请求投票的候选人id

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票请求的回复
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //回复的任期，用于候选者更新自身
	VoteGranted bool //如果候选人收到选票(对方认可为leader),为true
}

//
// example RequestVote RPC handler.
// 处理别的服务器发来的投票请求,每来一个请求就会开一个新的协程运行该函数，需要先来后到处理，所以加互斥锁
// 一个任期内只能投一次票，当任期
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	// Your code here (2A, 2B).

	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	println(args.CandidateId, "向", rf.me, "请求", args.Term, rf.currentTerm)
	//如果任期比自己大，给他投票
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.Term
		//如果是领导者，则要停止发送心跳并转换为追随者
		//如果是候选者，则要停止候选，转为追随者
		//如果是追随者则停止选举超时倒计时
		//综上，其实就是一次心跳
		rf.heartBeat <- struct{}{}
	} else if args.Term == rf.currentTerm { //任期和自己相同
		if rf.votedFor == -1 { //自己该任期还没投过票
			//拒绝
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			//投过票了，没票了
			reply.VoteGranted = false
			reply.Term = -1
		}
	} else { //任期比自己还小
		//拒绝
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

//leader追加日志或心跳的请求参数
type AppendEntriesArgs struct {
	//2A
	Term     int //领导人任期
	LeaderId int //领导人的Id

}

//对心跳的回复(有日志的话，心跳就附加了传递日志的功能)
type AppendEntriesReply struct {
	Term    int  //当前的任期号，用于领导人更新自己的任期号
	Success bool //是否接受
}

//处理leader发来的心跳动，每来一个心跳就会开一个新的协程运行该函数，需要先来后到处理，所以加互斥锁
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm { //任期不小于自己
		println(rf.me, "接受来自", args.LeaderId, "的心跳")
		rf.currentTerm = args.Term      //更改自己的任期
		reply.Success = true            //接受该心跳
		if args.Term > rf.currentTerm { //严格大于
			rf.votedFor = -1 //重置投票信息
		}
		rf.heartBeat <- struct{}{} //通知主协程收到了心跳
	} else { //任期比自己小，拒绝
		println(rf.me, "拒绝来自", args.LeaderId, "的心跳")
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

// leader发送追加日志或心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.role == 2 {
		if !reply.Success {
			rf.currentTerm = reply.Term
			rf.role = 1
		}
	}

	return ok
}

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
// 候选人发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	println(rf.me, "向", server, "发送投票请求", ok)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.role == 1 {
		if reply.VoteGranted {
			*voteNum++
		} else {
			//有拒绝票，直接转变为追随者
			if !(reply.Term == -1) {
				rf.currentTerm = reply.Term
				rf.role = 0
				return ok
			}
		}
		if *voteNum >= rf.peerNum/2+1 {
			//有过半的票数
			rf.role = 2
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.role == 2

	// Your code here (2B).

	return index, term, isLeader
}

//追随者状态
func (rf *Raft) Follower() {
	if rf.killed() {
		return
	}
	rf.role = 0

	rf.overTime = time.Duration(200+rand.Intn(150)) * time.Millisecond //选举超时时间
	rf.timer = time.NewTimer(rf.overTime)
	//等待计时器信号或心跳信号
	for {
		select {
		case <-rf.timer.C:
			{
				//计时器结束，成为候选者
				println(rf.me, "计时器结束，成为候选者")
				rf.Candidate()
				return
			}
		case <-rf.heartBeat:
			{
				//心跳信号，重置计时器
				rf.overTime = time.Duration(200+rand.Intn(150)) * time.Millisecond
				rf.timer.Reset(rf.overTime)
			}
		}
	}
}

//候选人状态
func (rf *Raft) Candidate() {
	if rf.killed() {
		return
	}
	rf.role = 1
	//任期加1，准备要票
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	//给自己投票
	voteNum := 1
	rf.mu.Unlock()
	//计时
	rf.overTime = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)
	//并发发送请求
	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply, &voteNum)
		}
	}
	for {
		select {
		case <-rf.timer.C:
			{
				println(rf.me, "等待要票超时")
				rf.Candidate()
				return
			}
		case <-rf.heartBeat:
			{
				println(rf.me, "有比自己大的候选者，自己转为追随者")
				rf.Follower()
				rf.votedFor = -1
				return
			}
		default:
			{
				if rf.role == 2 {
					println(rf.me, "成功当选")
					rf.Leader()
					return
				}

			}
		}
	}
}

//领导人状态
func (rf *Raft) Leader() {
	if rf.killed() {
		println(rf.me, "已死")
		return
	}
	rf.role = 2

	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}

	rf.overTime = time.Duration(100) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)

	for {
		select {
		case <-rf.heartBeat: //接受心跳了，转追随者
			{
				rf.Follower()
				return
			}
		case <-rf.timer.C:
			{
				rf.Leader()
				return
			}
		default:
			{
				if rf.role == 1 {
					rf.Follower()
					return
				}
			}
		}
	}
}

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
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

// 测试服务会创建peers切片并填充其中的通信方式，然后调用多次该函数产生raft服务器（由协程持续运行），
// peers存储了所有raft服务器的通信方式？
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//2A
	rf.currentTerm = 0                 //初始化任期为0
	rf.peerNum = len(peers)            //peer总数量
	rf.heartBeat = make(chan struct{}) //心跳信号，收到该信号则表示接受了心跳（也就是有领导人在）
	rf.votedFor = -1                   //设置投票默认值，-1代表未给任何人投票

	rf.overTime = time.Duration(500) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)
	go rf.Follower() //另起协程初始按追随者状态运行
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
