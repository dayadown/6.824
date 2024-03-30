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

//当绝大部分raft服务器安全的复制了某一条命令日志，leader会通过往channel中传入ApplyMsg
//告知kv，曾经我答应你存在某一下标的某一指令成功或者失败地存入raft日志中了
//非leader节点在得知leader已经commit了某些自己已有的日志时，也会发送ApplyMsg到自己的kv层

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term    int
	Command interface{}
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

	//2B
	applyCh     chan ApplyMsg
	log         []Log //日志信息
	commitIndex int   //已知的提交的日志索引
	lastApplied int   //最大的应用于服务器的索引
	nextIndex   []int //leader为每个服务器维护的下一日志存放索引
	matchIndex  []int
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

	//2B
	LastLogIndex int
	LastLogTerm  int
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
	//println(args.CandidateId, "向", rf.me, "请求", args.Term, rf.currentTerm)

	//任期比自己小，拒绝
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.me
		return
	}

	//候选者任期比自己大，更新任期,该任期下肯定没有投票，所以更新voteFor=-1
	//是否投票给候选者还需要进一步的日志判断
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//候选者任期和自己相同，那就看自己本来有没有投票和日志的信息了，所以不用处理

	var meLastLogIndex int
	var meLastLogTerm int
	meLastLogIndex = len(rf.log) - 1
	meLastLogTerm = rf.log[meLastLogIndex].Term

	//满足条件，投票给他
	//1.当前任期未投票  or  投的就是候选者
	//2.候选者最后一条日志的任期大于自己的  or  等于但是候选者日志更长
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(meLastLogTerm < args.LastLogTerm ||
			(meLastLogTerm == args.LastLogTerm && args.LastLogIndex >= meLastLogIndex)) {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.heartBeat <- struct{}{}
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

//leader追加日志或心跳的请求参数

type AppendEntriesArgs struct {
	//2A
	Term     int //领导人任期
	LeaderId int //领导人的Id

	//2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
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
	if args.Term >= rf.currentTerm { //任期不小于自己,根据日志信息返回消息
		//println(rf.me, "接收到来自", args.LeaderId, "的心跳", "rf.commitIndex", rf.commitIndex, "args.LeaderCommit", args.LeaderCommit)
		rf.currentTerm = args.Term      //更改自己的任期
		if args.Term > rf.currentTerm { //严格大于
			rf.votedFor = -1 //重置投票信息
		}
		reply.Term = rf.currentTerm

		//心跳中日志信息为空也要检查preLogIndex和preLogTerm，
		//在成功返回的时候(即和leader的日志匹配的时候)才根据args的commitIndex来提交日志，并且更新commitIndex

		if args.PrevLogIndex >= len(rf.log) {
			//日志空缺，返回false
			//println("日志空缺，返回false", args.PrevLogIndex, len(rf.log))
			reply.Success = false
		} else {
			if args.PrevLogIndex == 0 {
				//已经到头了，不用查了，直接将args的日志复制过来即可,更新matchIndex到当前日志长度-1
				//表示到matchIndex这里均已经与leader同步了
				reply.Success = true
				if !(args.Entries == nil) {
					//println(rf.me, "到头了且日志不为空", len(args.Entries))
					rf.log = append(rf.log[:1], args.Entries...)
				}
			} else {
				if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					//前一日志的任期不匹配
					reply.Success = false
				} else {
					//前一日志匹配了，追加日志即可
					reply.Success = true
					if !(args.Entries == nil) {
						//println(rf.me, "日志不为空", len(args.Entries))
						rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
					}
				}
			}
		}
		rf.heartBeat <- struct{}{} //通知主协程收到了心跳
	} else { //任期比自己小，拒绝
		//println(rf.me, "拒绝来自", args.LeaderId, "的心跳")
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	//检查是否有可提交的日志
	if reply.Success {
		for i := rf.lastApplied + 1; i <= args.LeaderCommit; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			//println(rf.me, "ccccccccccccccccccccc")
			rf.lastApplied = i
			rf.commitIndex = i
		}
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
		if reply.Term > rf.currentTerm {
			//回复任期比自己大
			//println(rf.me, "作为leader有人的任期比我大")
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = 0
		} else {
			if reply.Success {
				if args.Entries != nil {
					//println("更新", server, "的match和next", args.PrevLogIndex+len(args.Entries), rf.nextIndex[server]+len(args.Entries))
					rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				}
			} else {
				//日志有冲突，该服务器的nextIndex递减，下一次心跳向前找第一个不冲突的日志
				rf.nextIndex[server]--
			}
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
// handler function on the server side does not return. Thus, there
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.role == 1 {
		if reply.Term > rf.currentTerm {
			//回应的任期有比自己大的
			rf.currentTerm = reply.Term
			rf.role = 0
			rf.votedFor = -1
			return ok
		}
		if reply.VoteGranted {
			*voteNum++
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

//Start，kv层操控raft层的接口，传递一个command,命令raft将其存入日志
//返回参数
//1.index表示raft领导告诉kv层条命令未来会被存在日志的哪个索引处
//2.term表示raft中这条日志创建时的任期
//3.当前start的是否为leader(不是leader，KV层会start下一个raft服务器，直到是leader)

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == 2
	if !isLeader {
		return index, term, isLeader
	} else {
		//println("给", rf.me, "发指令")
		rf.log = append(rf.log, Log{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		term = rf.currentTerm
	}

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
				//println(rf.me, "计时器结束，成为候选者")
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
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	rf.mu.Unlock()
	//给自己投票
	voteNum := 1
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
				//println(rf.me, "等待要票超时")
				rf.Candidate()
				return
			}
		case <-rf.heartBeat:
			{
				//println(rf.me, "有比自己大的候选者，自己转为追随者")
				rf.votedFor = -1
				rf.Follower()
				return
			}
		default:
			{
				if rf.role == 2 {
					//println(rf.me, "成功当选")
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
		//println(rf.me, "已死")
		return
	}
	rf.role = 2

	rf.mu.Lock()
	//重置nextIndex和matchIndex
	for i := 0; i < rf.peerNum; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	//先发送一次心跳，因为刚刚才初始化nextIndex,且上了锁，所以第一次心跳不可能有新日志，直接发心跳即可
	//假设要追加空日志
	for i := 0; i < rf.peerNum; i++ {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			args.PrevLogIndex = len(rf.log) - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
	rf.mu.Unlock()

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
				rf.mu.Lock()
				//检查是否有可提交的日志
				for i := rf.commitIndex + 1; i < len(rf.log); i++ {
					if rf.log[i].Term == rf.currentTerm {
						//统计该下标下的日志复制情况
						success := 1
						for j := 0; j < rf.peerNum; j++ {
							if rf.matchIndex[j] >= i {
								success++
							}
						}
						//超过半数，更改commitIndex
						if success >= rf.peerNum/2+1 {
							//println(rf.me, "ccccccccccccccccccccc")
							rf.commitIndex = i
						}
					}
				}
				//检查是否有可应用的日志
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					rf.lastApplied = i
				}
				//到了心跳的间隔，发送心跳
				//1.若日志的最大索引>=nextIndex[i],则需要向该服务器发送日志信息，否则发送心跳即可
				for i := 0; i < rf.peerNum; i++ {
					if i != rf.me {
						//println(rf.me, "给", i, "发心跳信息", len(rf.log), rf.nextIndex[i])
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							LeaderCommit: rf.commitIndex,
						}
						if len(rf.log)-1 >= rf.nextIndex[i] {
							//需要附加日志
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
							args.Entries = rf.log[rf.nextIndex[i]:]
						} else {
							//无须附加日志
							args.PrevLogIndex = len(rf.log) - 1
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						}
						reply := AppendEntriesReply{}
						go rf.sendAppendEntries(i, &args, &reply)
					}
				}
				rf.mu.Unlock()
				//重新计时
				rf.overTime = time.Duration(100) * time.Millisecond
				rf.timer = time.NewTimer(rf.overTime)

				break
			}
		default:
			{
				if rf.role == 0 {
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
	rf.currentTerm = 0                             //初始化任期为0
	rf.peerNum = len(peers)                        //peer总数量
	rf.heartBeat = make(chan struct{}, rf.peerNum) //心跳信号，收到该信号则表示接受了心跳（也就是有领导人在）
	rf.votedFor = -1                               //设置投票默认值，-1代表未给任何人投票

	rf.overTime = time.Duration(500) * time.Millisecond
	rf.timer = time.NewTimer(rf.overTime)

	//2B
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Log, 0)
	//填充一个无用的指令把索引为0的占住
	rf.log = append(rf.log, Log{
		Term:    -1,
		Command: nil,
	})
	rf.nextIndex = make([]int, rf.peerNum)
	rf.matchIndex = make([]int, rf.peerNum)

	go rf.Follower() //另起协程初始按追随者状态运行
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
