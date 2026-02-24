package raft

//
// Raft tests.
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	// "log"
	"math/rand"
	"testing"
	"time"

	"6.5840/raftapi"
	"6.5840/tester1"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const MyRaftElectionTimeout = 1000 * time.Millisecond

func (ts *Test) myone(cmd any, expectedServers int, retry bool) int {
	var textretry string
	if retry {
		textretry = "with"
	} else {
		textretry = "without"
	}
	textcmd := fmt.Sprintf("%v", cmd)
	textb := fmt.Sprintf("checking agreement of %.8s by at least %v servers %v retry",
		textcmd, expectedServers, textretry)
	tester.AnnotateCheckerBegin(textb)
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 100 && ts.checkFinished() == false {
		// try all the servers, maybe one is the leader.
		index := -1
		for range ts.srvs {
			starts = (starts + 1) % len(ts.srvs)
			var rf raftapi.Raft
			if ts.g.IsConnected(starts) {
				ts.srvs[starts].mu.Lock()
				rf = ts.srvs[starts].raft
				ts.srvs[starts].mu.Unlock()
			}
			if rf != nil {
				//log.Printf("peer %d Start %v", starts, cmd)
				index1, _, ok := rf.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := ts.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						desp := fmt.Sprintf("agreement of %.8s reached", textcmd)
						tester.AnnotateCheckerSuccess(desp, "OK")
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
				tester.AnnotateCheckerFailure(desp, "failed after submitting command")
				ts.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if ts.checkFinished() == false {
		desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
		tester.AnnotateCheckerFailure(desp, "failed after 10-second timeout")
		ts.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

func TestMyFigure8Unreliable3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	tester.AnnotateTest("TestFigure8Unreliable3C", servers)
	ts.Begin("Test (3C): Figure 8 (unreliable)")

	ts.myone(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			ts.SetLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			cmd := rand.Int() % 10000
			_, _, ok := ts.srvs[i].Raft().Start(cmd)
			if ok {
				text := fmt.Sprintf("submitted command %v to server %v", cmd, i)
				tester.AnnotateInfo(text, text)
			}
			if ok && ts.g.IsConnected(i) {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(MyRaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(MyRaftElectionTimeout/time.Millisecond)/2 {
			ts.g.DisconnectAll(leader)
			tester.AnnotateConnection(ts.g.GetConnected())
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if !ts.g.IsConnected(s) {
				ts.g.ConnectOne(s)
				tester.AnnotateConnection(ts.g.GetConnected())
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if !ts.g.IsConnected(i) {
			ts.g.ConnectOne(i)
		}
	}
	tester.AnnotateConnection(ts.g.GetConnected())

	ts.myone(rand.Int()%10000, servers, true)
}
