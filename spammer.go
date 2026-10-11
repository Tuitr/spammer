package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	close(in)
	for _, c := range cmds {
		wg.Add(1)
		out := make(chan interface{})
		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			c(in, out)
		}(c, in, out)
		in = out
	}
	wg.Wait()

}

func SelectUsers(in, out chan interface{}) {
	users := make(map[uint64]bool)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	worker := func(email string) {
		defer wg.Done()
		user := GetUser(email)

		mu.Lock()
		if users[user.ID] == true {
			mu.Unlock()
			return
		}
		users[user.ID] = true
		mu.Unlock()

		out <- user
	}

	for v := range in {
		email := v.(string)
		wg.Add(1)
		go worker(email)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	batch := make([]User, 0, 2)
	worker := func(batch []User) {
		defer wg.Done()
		message, err := GetMessages(batch...)
		if err != nil {
			return
		}
		for _, message := range message {
			out <- message
		}

	}
	for v := range in {
		user := v.(User)
		batch = append(batch, user)
		if len(batch) == 2 {
			wg.Add(1)
			go worker(batch)
			batch = make([]User, 0, 2)
		}
	}
	if len(batch) > 0 {
		wg.Add(1)
		go worker(batch)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	quotaCh := make(chan struct{}, 5)

	worker := func(id MsgID, quotaCh chan struct{}) {
		defer wg.Done()
		quotaCh <- struct{}{}
		defer func() { <-quotaCh }()
		answer, err := HasSpam(id)
		if err != nil {
			return
		}
		data := MsgData{ID: id, HasSpam: answer}
		out <- data
	}

	for v := range in {
		wg.Add(1)
		id := v.(MsgID)
		go worker(id, quotaCh)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {

	results := make([]MsgData, 0)

	for v := range in {
		val := v.(MsgData)
		results = append(results, val)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})

	for _, r := range results {
		out <- fmt.Sprintf("%t %d", r.HasSpam, r.ID)
	}
}
