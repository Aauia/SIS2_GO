package tasks

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const wait = 5 * time.Second

func newSvc(t *testing.T, workers, capacity int, p Processor) *Service {
	t.Helper()
	s, err := New(Config{Workers: workers, QueueCapacity: capacity, Processor: p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Shutdown)
	return s
}

func mustCreate(t *testing.T, s *Service, text string) Task {
	t.Helper()
	tk, err := s.Create("title", text)
	if err != nil {
		t.Fatal(err)
	}
	return tk
}

// waitState polls (with a deadline, no fixed sleeps) until the task reaches
// the state; used only where the service gives no completion signal.
func waitState(t *testing.T, s *Service, id string, st State) Task {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		tk, _ := s.Get(id)
		if tk.State == st {
			return tk
		}
		time.Sleep(time.Millisecond)
	}
	tk, _ := s.Get(id)
	t.Fatalf("task %s: want state %s, got %s", id, st, tk.State)
	return tk
}

// gated builds a Processor that reports when it starts and blocks until
// release is closed.
func gated() (p Processor, started chan struct{}, release chan struct{}) {
	started = make(chan struct{}, 100)
	release = make(chan struct{})
	p = func(text string) (Result, error) {
		started <- struct{}{}
		<-release
		return WordStats(text)
	}
	return
}

func recv(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(wait):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func TestNewConfigValidation(t *testing.T) {
	if _, err := New(Config{Workers: 1, QueueCapacity: 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("1 worker: got %v", err)
	}
	if _, err := New(Config{Workers: 2, QueueCapacity: 0}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("0 capacity: got %v", err)
	}
}

func TestCreateValidationAndUniqueIDs(t *testing.T) {
	s := newSvc(t, 2, 2, nil)
	if _, err := s.Create(" ", "x"); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("empty title: %v", err)
	}
	if _, err := s.Create("t", "  "); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("empty text: %v", err)
	}
	a, b := mustCreate(t, s, "a"), mustCreate(t, s, "b")
	if a.ID == b.ID || a.State != Pending {
		t.Fatalf("bad tasks %+v %+v", a, b)
	}
	if _, err := s.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestStartupAndSubmitIsAsynchronous(t *testing.T) {
	p, started, release := gated()
	s := newSvc(t, 2, 2, p)
	tk := mustCreate(t, s, "hello world")

	if err := s.Submit(tk.ID); err != nil { // returns while processor is blocked
		t.Fatal(err)
	}
	recv(t, started, "worker start")
	got, _ := s.Get(tk.ID) // retrieval works while processing
	if got.State != Processing {
		t.Fatalf("want Processing, got %s", got.State)
	}
	close(release)
	got = waitState(t, s, tk.ID, Completed)
	if got.Result.Words != 2 {
		t.Fatalf("bad result %+v", got.Result)
	}
}

func TestMultipleTasksCorrectResults(t *testing.T) {
	s := newSvc(t, 3, 50, nil)
	want := map[string]Result{}
	for i := 1; i <= 40; i++ {
		text := ""
		for j := 0; j < i; j++ {
			text += "w "
		}
		text += "extraordinary"
		tk := mustCreate(t, s, text)
		want[tk.ID] = Result{Words: i + 1, UniqueWords: 2, Longest: "extraordinary"}
		if err := s.Submit(tk.ID); err != nil {
			t.Fatal(err)
		}
	}
	s.Shutdown()
	for id, w := range want {
		got, _ := s.Get(id)
		if got.State != Completed || got.Result != w {
			t.Fatalf("%s: got %+v want %+v", id, got, w)
		}
	}
}

func TestTasksProcessedConcurrently(t *testing.T) {
	// Each call waits until BOTH workers are inside the processor at the
	// same time; that can only succeed if processing truly overlaps.
	var mu sync.Mutex
	inside := 0
	bothIn := make(chan struct{})
	p := func(text string) (Result, error) {
		mu.Lock()
		inside++
		if inside == 2 {
			close(bothIn)
		}
		mu.Unlock()
		select {
		case <-bothIn:
		case <-time.After(wait):
			return Result{}, errors.New("no overlap")
		}
		return WordStats(text)
	}
	s := newSvc(t, 2, 4, p)
	a, b := mustCreate(t, s, "one"), mustCreate(t, s, "two words")
	if err := s.Submit(a.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(b.ID); err != nil {
		t.Fatal(err)
	}
	s.Shutdown()
	for _, id := range []string{a.ID, b.ID} {
		got, _ := s.Get(id)
		if got.State != Completed {
			t.Fatalf("%s: %s (%s)", id, got.State, got.Error)
		}
	}
}

func TestConcurrencyLimitedToWorkerCount(t *testing.T) {
	var mu sync.Mutex
	cur, max := 0, 0
	p := func(text string) (Result, error) {
		mu.Lock()
		cur++
		if cur > max {
			max = cur
		}
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		cur--
		mu.Unlock()
		return WordStats(text)
	}
	s := newSvc(t, 3, 100, p)
	for i := 0; i < 30; i++ {
		tk := mustCreate(t, s, "x")
		if err := s.Submit(tk.ID); err != nil {
			t.Fatal(err)
		}
	}
	s.Shutdown()
	if max > 3 || max < 1 {
		t.Fatalf("max concurrency %d, want 1..3", max)
	}
}

func TestInvalidAndDuplicateSubmissions(t *testing.T) {
	p, started, release := gated()
	s := newSvc(t, 2, 2, p)
	if err := s.Submit("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	tk := mustCreate(t, s, "a b")
	if err := s.Submit(tk.ID); err != nil {
		t.Fatal(err)
	}
	// Queued or Processing: both must be rejected.
	if err := s.Submit(tk.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("duplicate: got %v", err)
	}
	recv(t, started, "start")
	if err := s.Submit(tk.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("while processing: got %v", err)
	}
	close(release)
	waitState(t, s, tk.ID, Completed)
	if err := s.Submit(tk.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("completed: got %v", err)
	}
}

func TestConcurrentDuplicateSubmitProcessedOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	p := func(text string) (Result, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return WordStats(text)
	}
	s := newSvc(t, 4, 50, p)
	tk := mustCreate(t, s, "same task")
	var wg sync.WaitGroup
	var okMu sync.Mutex
	ok := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Submit(tk.ID) == nil {
				okMu.Lock()
				ok++
				okMu.Unlock()
			}
		}()
	}
	wg.Wait()
	s.Shutdown()
	if ok != 1 || calls != 1 {
		t.Fatalf("accepted=%d processed=%d, want 1 and 1", ok, calls)
	}
}

func TestFailedTaskAndRetry(t *testing.T) {
	var mu sync.Mutex
	fail := true
	p := func(text string) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return Result{}, errors.New("boom")
		}
		return WordStats(text)
	}
	s := newSvc(t, 2, 2, p)
	tk := mustCreate(t, s, "retry me")
	if err := s.Submit(tk.ID); err != nil {
		t.Fatal(err)
	}
	got := waitState(t, s, tk.ID, Failed)
	if got.Error != "boom" || got.Result != (Result{}) {
		t.Fatalf("bad failed task %+v", got)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	if err := s.Submit(tk.ID); err != nil { // retry allowed from Failed
		t.Fatal(err)
	}
	got = waitState(t, s, tk.ID, Completed)
	if got.Error != "" || got.Result.Words != 2 {
		t.Fatalf("bad retried task %+v", got)
	}
}

func TestProcessorPanicDoesNotKillWorker(t *testing.T) {
	p := func(text string) (Result, error) {
		if text == "panic" {
			panic("bad")
		}
		return WordStats(text)
	}
	s := newSvc(t, 2, 4, p)
	bad := mustCreate(t, s, "panic")
	_ = s.Submit(bad.ID)
	waitState(t, s, bad.ID, Failed)
	good := mustCreate(t, s, "fine")
	_ = s.Submit(good.ID)
	waitState(t, s, good.ID, Completed)
}

func TestQueueFull(t *testing.T) {
	p, started, release := gated()
	s := newSvc(t, 2, 2, p)
	submit := func() Task {
		tk := mustCreate(t, s, "x")
		if err := s.Submit(tk.ID); err != nil {
			t.Fatal(err)
		}
		return tk
	}
	submit()
	submit()
	recv(t, started, "worker 1")
	recv(t, started, "worker 2") // both workers busy, queue empty
	submit()
	submit() // queue (cap 2) now full

	extra := mustCreate(t, s, "x")
	if err := s.Submit(extra.ID); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	got, _ := s.Get(extra.ID)
	if got.State != Pending {
		t.Fatalf("rejected task must stay Pending, is %s", got.State)
	}
	close(release)
	// After space frees up the rejected task can be submitted.
	deadline := time.Now().Add(wait)
	for {
		err := s.Submit(extra.ID)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrQueueFull) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	waitState(t, s, extra.ID, Completed)
}

func TestConcurrentAccessToSharedData(t *testing.T) {
	s := newSvc(t, 4, 200, nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				tk, err := s.Create(fmt.Sprintf("t%d-%d", g, i), "alpha beta alpha")
				if err != nil {
					t.Error(err)
					return
				}
				if err := s.Submit(tk.ID); err != nil {
					t.Error(err)
					return
				}
				_, _ = s.Get(tk.ID)
				_ = s.List()
			}
		}(g)
	}
	wg.Wait()
	s.Shutdown()
	all := s.List()
	if len(all) != 160 {
		t.Fatalf("got %d tasks", len(all))
	}
	seen := map[string]bool{}
	for _, tk := range all {
		if seen[tk.ID] {
			t.Fatalf("duplicate ID %s", tk.ID)
		}
		seen[tk.ID] = true
		if tk.State != Completed || tk.Result != (Result{Words: 3, UniqueWords: 2, Longest: "alpha"}) {
			t.Fatalf("bad task %+v", tk)
		}
	}
}

func TestShutdownDrainsAcceptedTasksAndRejectsNew(t *testing.T) {
	p, started, release := gated()
	s := newSvc(t, 2, 10, p)
	var ids []string
	for i := 0; i < 8; i++ {
		tk := mustCreate(t, s, "a b c")
		if err := s.Submit(tk.ID); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, tk.ID)
	}
	recv(t, started, "w1")
	recv(t, started, "w2")

	done := make(chan struct{})
	go func() { s.Shutdown(); close(done) }()

	// Shutdown must not finish while tasks are still running.
	select {
	case <-done:
		t.Fatal("Shutdown returned before accepted tasks finished")
	case <-time.After(20 * time.Millisecond):
	}
	late := mustCreate(t, s, "late")
	deadline := time.Now().Add(wait)
	for {
		err := s.Submit(late.ID)
		if errors.Is(err, ErrStopped) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("submit after shutdown: got %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	recv(t, done, "Shutdown to return (workers terminated)")

	for _, id := range ids {
		got, _ := s.Get(id)
		if got.State != Completed || got.Result.Words != 3 {
			t.Fatalf("accepted task lost/incorrect: %+v", got)
		}
	}
	if got, _ := s.Get(late.ID); got.State != Pending {
		t.Fatalf("rejected task state %s", got.State)
	}
}

func TestShutdownTwiceAndConcurrently(t *testing.T) {
	s := newSvc(t, 2, 2, nil)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Shutdown() }()
	}
	wg.Wait()
	s.Shutdown()
	tk := mustCreate(t, s, "x")
	if err := s.Submit(tk.ID); !errors.Is(err, ErrStopped) {
		t.Fatalf("got %v", err)
	}
}

func TestSubmitConcurrentWithShutdownNoPanicNoLoss(t *testing.T) {
	for round := 0; round < 50; round++ {
		s, _ := New(Config{Workers: 3, QueueCapacity: 5})
		var wg sync.WaitGroup
		accepted := make(chan string, 200)
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					tk, _ := s.Create("t", "some text here")
					if s.Submit(tk.ID) == nil {
						accepted <- tk.ID
					}
				}
			}()
		}
		s.Shutdown()
		wg.Wait()
		s.Shutdown() // drain anything accepted racing with first Shutdown
		close(accepted)
		for id := range accepted {
			if got, _ := s.Get(id); got.State != Completed {
				t.Fatalf("accepted task %s ended %s", id, got.State)
			}
		}
	}
}
