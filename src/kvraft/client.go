package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader_id    int
	client_id    int64
	cur_op_count int64
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
	ck.client_id = nrand()
	ck.leader_id = 0
	ck.cur_op_count = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs

	args.Key = key
	args.Client = ck.client_id
	args.Id = atomic.AddInt64(&ck.cur_op_count, 1)
	for {
		var reply GetReply
		ck.servers[ck.leader_id].Call("RaftKV.Get", &args, &reply)
		if reply.Err == OK && reply.WrongLeader == false {
			return reply.Value
		} else {
			ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op
	args.Client = ck.client_id
	args.Id = atomic.AddInt64(&ck.cur_op_count, 1)
	for {
		var reply PutAppendReply
		ck.servers[ck.leader_id].Call("RaftKV.PutAppend", &args, &reply)
		if reply.Err == OK && reply.WrongLeader == false {
			DPrintf("Call success\n")
			break
		} else {
			ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
