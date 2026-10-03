package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Metadata struct {
	URL          string
	StatusCode   int
	ResponseTime int
}

type Result struct {
	InfoURL Metadata
	Error   error
}

var requestCount int64 = 0

func main() {
	urls := DataDummy()

	jobs := make(chan string, len(urls))
	result := make(chan Result, len(urls))

	for _, url := range urls {
		jobs <- url
	}
	close(jobs)

	wg := &sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go WorkerURLChecker(jobs, result, wg)
	}

	go func() {
		wg.Wait()
		close(result)
	}()

	errs := make([]string, 0, len(urls))

	for res := range result {
		if res.Error != nil {
			errs = append(errs, res.Error.Error())
			continue
		}
		fmt.Printf("URL : %s | StatusCode : %d | ResponseTime : %dms\n", res.InfoURL.URL, res.InfoURL.StatusCode, res.InfoURL.ResponseTime)
	}

	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Printf("Terjadi error : %v\n", err)
		}
	}
}

func WorkerURLChecker(jobChan <-chan string, resultChan chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobChan {
		result, err := ProcessURLChecker(job)
		resultChan <- Result{InfoURL: result, Error: err}
	}
}

func ProcessURLChecker(job string) (Metadata, error) {
	start := time.Now()

	res, err := http.Get(job)
	if err != nil {
		return Metadata{}, err
	}
	defer res.Body.Close()

	duration := time.Since(start).Milliseconds()

	return Metadata{URL: job, StatusCode: res.StatusCode, ResponseTime: int(duration)}, nil
}

func DataDummy() []string {
	return []string{
		"https://google.com",
		"https://github.com",
		"https://domain-palsu-yang-pasti-error-123.com",
		"https://httpbin.org",
		"https://wikipedia.org",
		"https://situs-tidak-ada-sama-sekali-xyz.net",
		"https://go.dev",
		"https://web-asal-asal.net",
		"https://stackoverflow.com",
		"ftp://skema-salah-bukan-http.com",
	}
}
