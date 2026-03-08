package kvraft

import (
	//"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	chosen int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.servers[ck.chosen], "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.chosen = (ck.chosen + 1) % len(ck.servers)
				//log.Printf("Clerk Get wrong leader, switch to %v", ck.servers[ck.chosen])
			} else {
				return reply.Value, reply.Version, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// You will have to modify this function.
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	retried := false
	for {
		ok := ck.clnt.Call(ck.servers[ck.chosen], "KVServer.Put", &args, &reply)
		//log.Printf("Clerk Put key ok = %v,reply = %v", ok, reply.Err)

		if ok {
			switch reply.Err {
			case rpc.ErrWrongLeader:
				ck.chosen = (ck.chosen + 1) % len(ck.servers)
				// log.Printf("Clerk Put wrong leader, switch to %v", ck.servers[ck.chosen])
				continue
			case rpc.OK:
				return rpc.OK
			case rpc.ErrVersion:
				if retried {
					return rpc.ErrMaybe
				} else {
					return rpc.ErrVersion
				}
			default:
				// reply.Err is rpc.ErrNoKey
				return reply.Err
			}
		}
		retried = true
		// warn: kvraft_test.go:161: Operations completed too slowly 54.147334ms/op > 33.333333ms/op
		// have no relation with this line of code
		time.Sleep(100 * time.Millisecond)
	}
}
