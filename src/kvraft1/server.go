package kvraft

import (
	//"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServerValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu     sync.Mutex
	kv_map map[string]KVServerValue

	// Your definitions here.
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15

func (kv *KVServer) GetOp(args *rpc.GetArgs) (reply *rpc.GetReply) {
	// Your code here.
	reply = &rpc.GetReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.kv_map[args.Key]; ok {
		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) PutOp(args *rpc.PutArgs) (reply *rpc.PutReply) {
	// Your code here.
	reply = &rpc.PutReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查key是否存在
	existingVal, exists := kv.kv_map[args.Key]

	if !exists {
		// Key不存在
		if args.Version == 0 {
			kv.kv_map[args.Key] = KVServerValue{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		// Key存在
		if args.Version == existingVal.Version {
			//kv.kv_map[key].Version += 1  // ❌ Error
			kv.kv_map[args.Key] = KVServerValue{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
	return reply
}
func (kv *KVServer) DoOp(req any) any {
	// gob decode into value type not pointer type
	//fmt.Printf("DoOp req type: %T\n", req)
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.GetOp(&req)
	case rpc.PutArgs:
		return kv.PutOp(&req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	} else {
		if result == nil {
			reply.Err = rpc.ErrNoKey
			return
		}
		rep := result.(*rpc.GetReply)
		reply.Version = rep.Version
		reply.Err = rep.Err
		reply.Value = rep.Value
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	} else {
		if result == nil {
			reply.Err = rpc.ErrNoKey
			return
		}
		rep := result.(*rpc.PutReply)
		reply.Err = rep.Err
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.kv_map = make(map[string]KVServerValue)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
