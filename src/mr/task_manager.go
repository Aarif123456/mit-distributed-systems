package mr

import (
	"sync"
	"sync/atomic"
	"time"
)

// How we should wait on a job before timing out
const _jobTimeout = 10 * time.Second

type (
	taskManager[T any] struct {
		jobs      []T
		taskDone  []*doneTracker // done channel per job
		jobStream chan int       // gives us a job we have to do
		nJobsDone uint64         // incremented atomically
	}
	doneTracker struct {
		// make sure task is marked as done only once
		once   sync.Once
		doneCh chan empty
	}
	empty struct{}
)

// newTaskManager returns a task manager that can handle job of type T.
// This can also be done with an interface, but generics
// give us type safety
func newTaskManager[T any](jobs []T) *taskManager[T] {
	numJobs := len(jobs)

	taskDone := make([]*doneTracker, 0, numJobs)
	for range jobs {
		taskDone = append(taskDone, &doneTracker{
			// need to initialize since nil channel block forever
			doneCh: make(chan empty),
		})
	}

	// Fill up job stream
	jobStream := make(chan int, numJobs)
	for i := range jobs {
		jobStream <- i
	}

	return &taskManager[T]{jobs, taskDone, jobStream, 0 /* # jobs done */}
}

func (tm *taskManager[T]) Run() (T, bool) {
	nJob, ok := <-tm.jobStream
	if !ok {
		var zeroVal T
		return zeroVal, false
	}
	go func() {
		// Check if job is done within time frame
		select {
		case <-tm.taskDone[nJob].doneCh:
			tm.IncrementJobsDone()
			// If job are done, close the stream to cancel
			// any waiting tasks
			if tm.IsDone() {
				close(tm.jobStream)
			}
		case <-time.After(_jobTimeout):
			// Add job back into queue
			tm.jobStream <- nJob
		}
	}()

	return tm.jobs[nJob], true
}

func (tm *taskManager[T]) MarkDone(nJob int) {
	tm.taskDone[nJob].MarkDone()
}

func (tm *taskManager[T]) IsDone() bool {
	return tm.NumJobDone() == uint64(len(tm.jobs))
}

func (tm *taskManager[T]) IncrementJobsDone() {
	atomic.AddUint64(&tm.nJobsDone, 1)
}

func (tm *taskManager[T]) NumJobDone() uint64 {
	return atomic.LoadUint64(&tm.nJobsDone)
}

func (d *doneTracker) MarkDone() {
	d.once.Do(func() {
		close(d.doneCh)
	})
}
