package tasks

import (
	"errors"
	"testing"
)

func TestWordStats(t *testing.T) {
	r, err := WordStats("The cat and the hat, the END")
	if err != nil {
		t.Fatal(err)
	}
	want := Result{Words: 7, UniqueWords: 5, Longest: "The"}
	if r != want {
		t.Fatalf("got %+v want %+v", r, want)
	}
	if _, err := WordStats("... !!!"); !errors.Is(err, ErrNoWordsInInput) {
		t.Fatalf("want ErrNoWordsInInput, got %v", err)
	}
}
