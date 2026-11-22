package main

import (
	"fmt"
	"log"

	"sis/internal/tasks"
)

func main() {
	svc, err := tasks.New(tasks.Config{Workers: 3, QueueCapacity: 8})
	if err != nil {
		log.Fatal(err)
	}
	texts := []string{
		"The quick brown fox jumps over the lazy dog",
		"Go makes concurrency simple, concurrency is not parallelism",
		"To be or not to be that is the question",
		"   ",
	}
	var ids []string
	for i, txt := range texts {
		t, err := svc.Create(fmt.Sprintf("doc-%d", i+1), txt)
		if err != nil {
			fmt.Println("create error:", err)
			continue
		}
		if err := svc.Submit(t.ID); err != nil {
			fmt.Println("submit error:", err)
			continue
		}
		ids = append(ids, t.ID)
	}
	svc.Shutdown() // waits for all accepted tasks
	for _, id := range ids {
		t, _ := svc.Get(id)
		fmt.Printf("%s %-10s %+v\n", t.ID, t.State, t.Result)
	}
}
