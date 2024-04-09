package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//raft日志中要存储的数据

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int
	Key       string
	Value     string
	ClientId  int64
	OpType    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientIdCommandId map[int64]int     //记录特定客户端在这台server上的命令位置	clientId / seqId
	indexOpChan       map[int]chan Op   //通过index匹配raft层传来的信息	index / chan(Op)
	kvMap             map[string]string // 存储持久化的KV键值对	K / V
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.indexOpChan[index] = make(chan Op, 1)
	ch := kv.indexOpChan[index]
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	//println(args.ClientId, "尝试给", kv.me, "发送Get操作,编号为", args.CommandId)

	op := Op{
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		OpType:    "Get",
	}

	//调用raft的start函数对日志进行共识
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//println(args.ClientId, "给", kv.me, "发送Get操作,编号为", args.CommandId)
	//根据index创建等待raft通道
	ch := kv.getWaitCh(index)

	//在函数返回时删除该等待通道
	defer func() {
		kv.mu.Lock()
		delete(kv.indexOpChan, index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(300 * time.Millisecond)
	defer timer.Stop()

	select {
	case appliedOp := <-ch:
		{
			if appliedOp.ClientId == args.ClientId && appliedOp.CommandId == args.CommandId {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				_, exist := kv.kvMap[args.Key]
				if !exist {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = kv.kvMap[args.Key]
				}
				return
			} else {
				//println("提交的不是期待的")
				reply.Err = ErrWrongLeader
				return
			}
		}
	case <-timer.C:
		{
			//println(args.ClientId, args.CommandId, "等待共识超时")
			reply.Err = ErrWrongLeader
			return
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		CommandId: args.CommandId,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		OpType:    args.Op,
	}

	//调用raft的start函数对日志进行共识
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//println(args.ClientId, "给", kv.me, "发送", args.Op, "操作,编号为", args.CommandId)
	//根据index创建等待raft通道
	ch := kv.getWaitCh(index)

	//在函数返回时删除该等待通道
	defer func() {
		kv.mu.Lock()
		delete(kv.indexOpChan, index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(300 * time.Millisecond)
	defer timer.Stop()

	select {
	case appliedOp := <-ch:
		{
			if appliedOp.ClientId == args.ClientId && appliedOp.CommandId == args.CommandId {
				reply.Err = OK
				return
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		}
	case <-timer.C:
		{
			//println(args.ClientId, args.CommandId, "等待共识超时")
			reply.Err = ErrWrongLeader
			return
		}
	}
}

func (kv *KVServer) applyChanHandler() {
	for {
		if kv.killed() {
			return
		}
		select {
		case applyMsg := <-kv.applyCh:
			{
				index := applyMsg.CommandIndex
				op := applyMsg.Command.(Op)
				//println("收到raft回复,", "op.OpType", op.OpType, "op.Key", op.Key)
				if !kv.isDuplicate(op.ClientId, op.CommandId) {
					//在KV中执行操作
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvMap[op.Key] = op.Value
					case "Append":
						kv.kvMap[op.Key] += op.Value
					}
					kv.clientIdCommandId[op.ClientId] = op.CommandId
					kv.mu.Unlock()
				}
				//通知操作已经执行
				kv.mu.Lock()
				ch, exist := kv.indexOpChan[index]
				if exist {
					ch <- op
				}
				kv.mu.Unlock()
			}
		}

	}
}

//判断某一客户端的某一条指令在本server中是否是重复的
func (kv *KVServer) isDuplicate(clientId int64, commandId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastCommandId, exist := kv.clientIdCommandId[clientId]
	if !exist {
		return false
	}
	return commandId <= lastCommandId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.clientIdCommandId = make(map[int64]int)
	kv.indexOpChan = make(map[int]chan Op)
	kv.kvMap = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg, len(servers))   //用于接收raft层传来的信息
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) //该server绑定的raft层

	// You may need initialization code here.
	go kv.applyChanHandler() //不断接受并处理raft传来的applyCh
	return kv
}
