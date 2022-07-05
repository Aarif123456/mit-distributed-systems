package mr

import (
    "sync"
    "sync/atomic"
    "time"
)

const _jobTimeout = 10 * time.Second

type (
    taskManager[T any] struct {
        jobs      []T
        nJobsDone uint64         // incremented atomically
        taskDone  []*doneTracker // done channel per job
        jobStream chan int       // gives us a job we have to do
    }
    doneTracker struct {
        // make sure task is marked as done only once
        once   sync.Once
        doneCh chan struct{}
    }
)

func newTaskManager[T any](jobs []T) *taskManager[T] {
    numJobs := len(jobs)

    taskDone := make([]*doneTracker, 0, numJobs)
    for _ = range jobs {
        taskDone = append(taskDone, &doneTracker{
            // need to initialize since nil channel block forever
            doneCh: make(chan struct{}),
        })
    }

    // Fill up job stream
    jobStream := make(chan int, numJobs)
    for i := range jobs {
        jobStream <- i
    }

    return &taskManager[T]{jobs, 0 /* jobs done */, taskDone, jobStream}
}

func (tm *taskManager[T]) Run() (T, bool) {
    nJob, ok := <-tm.jobStream
    if !ok {
        var zeroVal T
        return zeroVal, false
    }
    go func() {
        select {
        case <-tm.taskDone[nJob].doneCh:
            // Increment done task
            atomic.AddUint64(&tm.nJobsDone, 1)
            // If job are done, close the stream to unblock waiting tasks
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

func (tm *taskManager[T]) NumJobDone() uint64 {
    return atomic.LoadUint64(&tm.nJobsDone)
}

func (d *doneTracker) MarkDone() {
    d.once.Do(func() {
        close(d.doneCh)
    })
}
