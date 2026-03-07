package rsm

import (
	//"fmt"
	"sync"
	//"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

type WaiterInfo struct {
	waiterchan chan any
	id         int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waiter map[int]WaiterInfo
	nextId int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiter:       make(map[int]WaiterInfo),
		nextId:       0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.readApplyCh()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) readApplyCh() any {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			// warn:DoOp may block, so we should not hold the lock when calling it.
			// warn: op,msg is thread local variable
			// warn: map is not concurrency safe
			op := msg.Command.(Op)
			rep := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			if val, ok := rsm.waiter[msg.CommandIndex]; ok {
				if msg.Command.(Op).Id != val.id {
					for k, v := range rsm.waiter {
						if k >= msg.CommandIndex {
							close(v.waiterchan)
							delete(rsm.waiter, k)
						}
					}
					rsm.mu.Unlock()
					continue
				} else {
					//fmt.Printf("Applier: got index %d, op: %+v\n", msg.CommandIndex, op)
					select {
					case val.waiterchan <- rep:
						//fmt.Printf("%d send apply msg to waiter, index: %d, n = %d\n", rsm.me, msg.CommandIndex, op.Req.(Inc))
					default:
						//fmt.Printf("%d waiter channel is full, index: %d\n", rsm.me, msg.CommandIndex)
					}
				}
			}
			rsm.mu.Unlock()
		}
	}
	// warn:when applyCh is closed, we should close all waiter channels to avoid goroutine leak.
	rsm.mu.Lock()
	for k, v := range rsm.waiter {
		close(v.waiterchan)
		delete(rsm.waiter, k)
	}
	rsm.mu.Unlock()
	return nil
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	myid := rsm.nextId
	rsm.nextId++
	op := Op{Me: rsm.me, Id: myid, Req: req}
	index, _, isLeader := rsm.rf.Start(op)
	//fmt.Printf("%d submit op to raft, index: %d, isLeader: %v\n", rsm.me, index, isLeader)
	if isLeader {
		ch := WaiterInfo{
			waiterchan: make(chan any, 1),
			id:         myid,
		}
		rsm.waiter[index] = ch
		rsm.mu.Unlock()
		// error:stuck in this line:goroutine 1675 [chan receive]:
		// error:6.5840/kvraft1/rsm.(*RSM).Submit(0xc000138d70, {0x623500?, 0x896b20?})
		// error:6.5840/src/kvraft1/rsm/rsm.go:155 +0x18b
		// the reason is that ch.applyCh is not closed when the server is killed.
		op, ok := <-ch.waiterchan
		if !ok {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, op
	}
	// your code here
	rsm.mu.Unlock()
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
