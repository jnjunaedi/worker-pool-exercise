package main

import (
	"fmt"
	"sync"
)

type User struct {
	ID   string
	Name string
}

type Order struct {
	ID          string
	UserOrder   User
	ProductName string
	Price       int64
	Amount      int32
}

type Result struct {
	OrderResult Order
	Error       error
}

func main() {
	data := DataDummy()

	jobs := make(chan Order, len(data))
	result := make(chan Result, len(data))

	Generator(jobs, data)

	wg := &sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go Worker(jobs, result, wg)
	}

	go func() {
		wg.Wait()
		close(result)
	}()

	var err error

	for res := range result {
		if res.Error != nil {
			err = res.Error
			continue
		}
		fmt.Printf("User : %s | Product : %s | Price : %d | Amount : %d\n", res.OrderResult.UserOrder.Name, res.OrderResult.ProductName, res.OrderResult.Price, res.OrderResult.Amount)
	}

	if err != nil {
		fmt.Printf("Terjadi error : %v\n", err)
	}

}

func Generator(jobChan chan<- Order, jobs []Order) {
	go func() {
		for _, job := range jobs {
			jobChan <- job
		}
		close(jobChan)
	}()
}

func Worker(jobChan <-chan Order, resultChan chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobChan {
		hasil, err := ProcessJob(job)
		resultChan <- Result{OrderResult: hasil, Error: err}
	}

}

func ProcessJob(job Order) (Order, error) {
	if job.Price > 1000000 {
		job.Price = job.Price - 30000
	}

	if job.Amount == 0 {
		if job.Price > 1000 {
			return Order{}, fmt.Errorf("Sistem sedang error : User %s | Jumlah %d | Price | %d", job.UserOrder.Name, job.Amount, job.Price)
		}
	}

	return job, nil
}

func DataDummy() []Order {
	users := []User{
		{ID: "U001", Name: "Budi Santoso"},
		{ID: "U002", Name: "Siti Rahma"},
		{ID: "U003", Name: "Andi Wijaya"},
		{ID: "U004", Name: "Dewi Lestari"},
		{ID: "U005", Name: "Eko Prasetyo"},
	}

	orders := []Order{
		{ID: "ORD-001", UserOrder: users[0], ProductName: "Laptop ASUS ROG", Price: 15000000, Amount: 1},
		{ID: "ORD-002", UserOrder: users[1], ProductName: "Smartphone Samsung S24", Price: 12000000, Amount: 1},
		{ID: "ORD-003", UserOrder: users[2], ProductName: "Mouse Wireless Logitech", Price: 300000, Amount: 2},
		{ID: "ORD-004", UserOrder: users[3], ProductName: "Keyboard Mechanical", Price: 750000, Amount: 1},
		{ID: "ORD-005", UserOrder: users[4], ProductName: "Monitor Gaming 24 Inch", Price: 2500000, Amount: 0},
	}

	return orders
}
