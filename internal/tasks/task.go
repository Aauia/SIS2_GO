// Package tasks implements a text-analysis task service whose tasks are
// processed concurrently by a fixed-size worker pool.
package tasks

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// State is the lifecycle state of a task.
type State string

const (
	Pending    State = "Pending"
	Queued     State = "Queued"
	Processing State = "Processing"
	Completed  State = "Completed"
	Failed     State = "Failed"
)

// Errors returned by the service.
var (
	ErrInvalidTask    = errors.New("invalid task")
	ErrNotFound       = errors.New("task not found")
	ErrInvalidState   = errors.New("task is not in a submittable state")
	ErrQueueFull      = errors.New("queue is full")
	ErrStopped        = errors.New("service is stopped")
	ErrInvalidConfig  = errors.New("invalid configuration")
	ErrEmptyText      = fmt.Errorf("%w: text must not be empty", ErrInvalidTask)
	ErrEmptyTitle     = fmt.Errorf("%w: title must not be empty", ErrInvalidTask)
	ErrNoWordsInInput = errors.New("text contains no words")
)

// Result is the outcome of processing a task. It contains only value
// fields, so copying a Task yields a fully independent snapshot.
type Result struct {
	Words       int
	UniqueWords int
	Longest     string
}

// Task is a text-analysis task. Instances held by the Service are never
// handed out directly; callers always receive copies.
type Task struct {
	ID     string
	Title  string
	Text   string
	State  State
	Result Result
	Error  string // set when State == Failed
}

// Processor performs the domain-specific work for a task's text.
type Processor func(text string) (Result, error)

// WordStats is the default Processor: it counts words, distinct words
// (case-insensitive) and finds the longest word.
func WordStats(text string) (Result, error) {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '\''
	})
	if len(words) == 0 {
		return Result{}, ErrNoWordsInInput
	}
	seen := make(map[string]struct{}, len(words))
	longest := ""
	for _, w := range words {
		seen[strings.ToLower(w)] = struct{}{}
		if len([]rune(w)) > len([]rune(longest)) {
			longest = w
		}
	}
	return Result{Words: len(words), UniqueWords: len(seen), Longest: longest}, nil
}

func sortTasks(ts []Task) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
}
