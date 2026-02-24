package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	res     string
	version rpc.Tversion
	err     rpc.Err
	myID    string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, myID: kvtest.RandValue(8), res: l}
	lk.ck.Put(l, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	for { // 循环直到获得锁
		// 1. 读取锁状态
		value, version, _ := lk.ck.Get(lk.res)

		// 2. 如果锁空闲，尝试获取
		if value == "" {
			err := lk.ck.Put(lk.res, lk.myID, version)
			if err == rpc.OK {
				lk.version = version + 1
				return
			}
			// 如果是 ErrVersion，说明被别人抢了，继续循环
		}

		// 3. 锁被占用，等待一会再试
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.res, "", lk.version)

}
