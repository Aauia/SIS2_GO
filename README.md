# SIS Project – Checkpoint 2: Concurrent Processing

## Purpose
A text-analysis task service. Each task holds a title and a text; processing
computes word count, number of distinct words (case-insensitive) and the
longest word (`WordStats` in `internal/tasks/task.go`).
Tasks are created/validated, stored in memory with unique IDs (`task-N`),
and retrieved with `Get` / `List`.

## Creating and submitting tasks
```go
svc, _ := tasks.New(tasks.Config{Workers: 3, QueueCapacity: 8})
t, _ := svc.Create("title", "some text")
err := svc.Submit(t.ID) // returns immediately, processing is asynchronous
t, _ = svc.Get(t.ID)    // snapshot copy of the task
svc.Shutdown()
```

## Architecture
```
Create -> [store: map + RWMutex]
Submit -> bounded channel (queue of task IDs) -> Worker 1..N -> store (result)
```
* **Workers:** fixed number set in `Config.Workers` (minimum 2; demo uses 3).
  They live for the whole life of the service (`for id := range queue`).
* **Queue:** buffered channel of capacity `Config.QueueCapacity` (demo: 8).
* **Full queue:** `Submit` never blocks; it returns `ErrQueueFull` and the task
  stays `Pending` (or `Failed`) so it can be submitted again later.

## Task lifecycle
```
Pending --Submit--> Queued --worker claims--> Processing --> Completed
   ^                                              |
   |                                              v
   +-------------- Submit (retry) ------------- Failed
```
Only `Pending` and `Failed` tasks can be submitted. Submitting a
`Queued`, `Processing` or `Completed` task returns `ErrInvalidState`.
A worker only starts a task if it is `Queued` (claim done under the lock),
so one task is never processed by two workers.

## Synchronization
* `mu sync.RWMutex` protects the task map **and** all fields of the stored
  tasks. The check-state + enqueue + mark-Queued sequence in `Submit` and the
  claim in the worker are single critical sections. The lock is **not** held
  while the processor runs. `Get`/`List` return copies.
* `stopMu sync.RWMutex` + `stopped` flag: `Submit` holds the read lock while
  sending to the queue; `Shutdown` takes the write lock to set the flag and
  close the channel. So a send on a closed channel is impossible.
  Lock order is always `stopMu` then `mu` (no deadlock).

## Stopping workers
`Shutdown()` stops accepting tasks (`ErrStopped`), closes the queue (only
`Shutdown` closes it), workers drain the remaining accepted tasks and exit,
and `Shutdown` waits on a `sync.WaitGroup`. Calling it multiple times or
concurrently is safe (no-op / just waits).

## Running
```bash
go run ./cmd/sis
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
```
