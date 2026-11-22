package tasks

import (
	"fmt"
	"strings"
	"sync"
)

// Config configures a Service.
type Config struct {
	Workers       int       // number of worker goroutines, must be >= 2
	QueueCapacity int       // capacity of the bounded queue, must be >= 1
	Processor     Processor // defaults to WordStats when nil
}

// Service stores tasks and processes them asynchronously.
//
// Synchronization design:
//   - mu (RWMutex) guards the tasks map AND every mutable field of the
//     stored *Task values. Get/List return copies, so no caller ever
//     touches shared memory. mu is never held while a Processor runs.
//   - stopMu (RWMutex) guards `stopped` and closing of `queue`. Submit
//     holds the read lock while sending, Shutdown holds the write lock
//     while closing, so a send on a closed channel is impossible.
//   - Only Shutdown closes `queue` (it is the channel's owner).
type Service struct {
	proc Processor

	mu     sync.RWMutex
	tasks  map[string]*Task
	nextID int

	stopMu  sync.RWMutex
	stopped bool
	queue   chan string // carries task IDs; bounded

	wg sync.WaitGroup
}

// New creates a Service and starts its worker goroutines.
func New(cfg Config) (*Service, error) {
	if cfg.Workers < 2 {
		return nil, fmt.Errorf("%w: need at least 2 workers", ErrInvalidConfig)
	}
	if cfg.QueueCapacity < 1 {
		return nil, fmt.Errorf("%w: queue capacity must be >= 1", ErrInvalidConfig)
	}
	if cfg.Processor == nil {
		cfg.Processor = WordStats
	}
	s := &Service{
		proc:  cfg.Processor,
		tasks: make(map[string]*Task),
		queue: make(chan string, cfg.QueueCapacity),
	}
	s.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go s.worker()
	}
	return s, nil
}

// Create validates and stores a new Pending task.
func (s *Service) Create(title, text string) (Task, error) {
	if strings.TrimSpace(title) == "" {
		return Task{}, ErrEmptyTitle
	}
	if strings.TrimSpace(text) == "" {
		return Task{}, ErrEmptyText
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	t := &Task{ID: fmt.Sprintf("task-%d", s.nextID), Title: title, Text: text, State: Pending}
	s.tasks[t.ID] = t
	return *t, nil
}

// Get returns a snapshot of the task with the given ID.
func (s *Service) Get(id string) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, ErrNotFound
	}
	return *t, nil
}

// List returns snapshots of all tasks ordered by ID string.
func (s *Service) List() []Task {
	s.mu.RLock()
	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, *t)
	}
	s.mu.RUnlock()
	sortTasks(out)
	return out
}

// Submit queues a task for asynchronous processing and returns without
// waiting for it to be processed. Only Pending and Failed (retry) tasks
// can be submitted. If the queue is full, ErrQueueFull is returned and the
// task keeps its previous state (submission never blocks).
func (s *Service) Submit(id string) error {
	s.stopMu.RLock()
	defer s.stopMu.RUnlock()
	if s.stopped {
		return ErrStopped
	}

	// The state check, the enqueue and the Queued transition form one
	// critical section so two concurrent Submits cannot both succeed.
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if t.State != Pending && t.State != Failed {
		return fmt.Errorf("%w: %s is %s", ErrInvalidState, id, t.State)
	}
	select {
	case s.queue <- id: // non-blocking: the queue is buffered
		t.State = Queued
		t.Error = ""
		return nil
	default:
		return ErrQueueFull
	}
}

// Shutdown stops accepting new tasks, lets workers finish every task that
// was already accepted, and waits for all workers to exit. It is safe to
// call multiple times and concurrently with Submit.
func (s *Service) Shutdown() {
	s.stopMu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.queue)
	}
	s.stopMu.Unlock()
	s.wg.Wait()
}

func (s *Service) worker() {
	defer s.wg.Done()
	// range ends once the queue is closed AND drained, so accepted tasks
	// are never lost.
	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	// Claim the task: Queued -> Processing, atomically. A task that is not
	// Queued is never processed, so no task runs on two workers at once.
	s.mu.Lock()
	t, ok := s.tasks[id]
	if !ok || t.State != Queued {
		s.mu.Unlock()
		return
	}
	t.State = Processing
	text := t.Text
	s.mu.Unlock()

	// Long-running work happens WITHOUT holding any lock.
	res, err := s.safeProcess(text)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		t.State = Failed
		t.Error = err.Error()
		return
	}
	t.Result = res
	t.State = Completed
}

// safeProcess converts a panic in the Processor into an error so one bad
// task cannot kill a worker.
func (s *Service) safeProcess(text string) (res Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor panic: %v", r)
		}
	}()
	return s.proc(text)
}
