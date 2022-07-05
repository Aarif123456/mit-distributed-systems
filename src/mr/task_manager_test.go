package mr

import (
	"sync"
	"testing"
	"time"
)

type testReply struct {
	val string
}

func Test_TaskManager(t *testing.T) {
	tests := []struct {
		name    string
		replies []testReply
	}{
		{
			name: "Empty task manager does not crash",
		},
		{
			name: "One reply",
			replies: []testReply{
				{
					val: "reply one",
				},
			},
		},
		{
			name: "Some reply",
			replies: []testReply{
				{
					val: "reply one",
				},
				{
					val: "reply two",
				},
				{
					val: "reply three",
				},
				{
					val: "reply four",
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tm := newTaskManager(tc.replies)
			// test concurrent marking done
			for i := range tc.replies {
				i := i
				go tm.MarkDone(i)
				go tm.MarkDone(i)
			}

			// test concurrent calls to run
			var wg sync.WaitGroup
			for i := range tc.replies {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					tm.Run()
					tm.MarkDone(i)
				}(i)
			}
			wg.Wait()

			if len(tc.replies) > 0 {
				// Job stream will be closed after we are done
				select {
				case <-tm.jobStream:
				case <-time.After(1 * time.Second):
					t.Fatalf("Channel wasn't marked as done within expected time frame, Job Done %d", tm.NumJobDone())
				}
			}

			if !tm.IsDone() {
				t.Fatalf("We expected to have finished")
			}
		})
	}
}
