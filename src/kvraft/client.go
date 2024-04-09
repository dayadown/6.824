package kvraft

import (
	"../labrpc"
	"crypto/rand"
	mrand "math/rand"
)
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64 //客户端唯一标识
	commandId int   //客户端命令的id,单调增
	leaderId  int   //所知道的leader的id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()                     //随机生成
	ck.leaderId = mrand.Intn(len(ck.servers)) //初始时随机一个
	ck.commandId = 0                          //从0开始递增
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.commandId++
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				{
					ck.leaderId = leaderId
					return reply.Value
				}
			case ErrNoKey:
				{
					ck.leaderId = leaderId
					return ""
				}
			case ErrWrongLeader:
				{
					leaderId = (leaderId + 1) % len(ck.servers)
					continue
				}
			}
		}
		//节点没有回应
		leaderId = (leaderId + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		//println(ck.clientId, "发送操作", op, key, value, "给", leaderId, "回复是", reply.Err)
		if ok {
			switch reply.Err {
			case OK:
				{
					ck.leaderId = leaderId
					return
				}
			case ErrWrongLeader:
				{
					leaderId = (leaderId + 1) % len(ck.servers)
					continue
				}
			}
		}
		leaderId = (leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
