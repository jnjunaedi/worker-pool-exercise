package main

import (
	"fmt"
	"sync"
	"time"
)

type Transaction struct {
	ID       string
	UserID   string
	Amount   float64
	Location string
}
type AnalysisReport struct {
	TxID        string
	UserID      string
	Amount      float64
	IsFraud     bool
	FraudReason string
}

type DetailInfo struct {
	Analysis AnalysisReport
	Error    error
}

func main() {
	data := DataDummy()

	jobChan := make(chan Transaction, len(data))
	resultChan := make(chan DetailInfo, len(data))

	for _, job := range data {
		jobChan <- job
	}
	close(jobChan)

	wg := &sync.WaitGroup{}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go WorkerProcessTransaction(jobChan, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	errs := make([]string, 0, len(data))
	frauds := make([]AnalysisReport, 0, len(data))

	for result := range resultChan {
		if result.Error != nil {
			errs = append(errs, result.Error.Error())
			continue
		}
		if result.Analysis.IsFraud {
			frauds = append(frauds, result.Analysis)
			continue
		}

		fmt.Printf("TxID : %s | UserID : %s | Amount : %.2f | IsFraud : %t\n", result.Analysis.TxID, result.Analysis.UserID, result.Analysis.Amount, result.Analysis.IsFraud)
	}

	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Printf("Error : %v\n", err)
		}
	}

	if len(frauds) > 0 {
		for _, fraud := range frauds {
			fmt.Printf("TxID : %s | UserID : %s | Amount : %.2f | IsFraud : %t | FraudReason  : %s\n", fraud.TxID, fraud.UserID, fraud.Amount, fraud.IsFraud, fraud.FraudReason)
		}
	}
}

func WorkerProcessTransaction(jobChan <-chan Transaction, resultChan chan<- DetailInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobChan {
		result, err := ProcessTransaction(job)
		resultChan <- DetailInfo{Analysis: result, Error: err}
	}
}

func ProcessTransaction(job Transaction) (AnalysisReport, error) {
	time.Sleep(100 * time.Millisecond)

	if job.Amount <= 0 {
		return AnalysisReport{}, fmt.Errorf("nominal transaksi tidak valid dari UserID : %s", job.UserID)
	}

	if job.Amount > 10000000 || job.Location == "Unknown-IP" {
		return AnalysisReport{TxID: job.ID, UserID: job.UserID, Amount: job.Amount, IsFraud: true, FraudReason: "Transaksi mencurigakan di atas 10jt atau Lokasi tidak dikenal❌"}, nil
	}

	return AnalysisReport{TxID: job.ID, UserID: job.UserID, Amount: job.Amount, IsFraud: false}, nil
}

func DataDummy() []Transaction {
	return []Transaction{
		{ID: "TX-001", UserID: "U1", Amount: 500000, Location: "Jakarta"},
		{ID: "TX-002", UserID: "U2", Amount: 15000000, Location: "Bandung"},  // Fraud (>10jt)
		{ID: "TX-003", UserID: "U3", Amount: -50000, Location: "Jakarta"},    // Error (<=0)
		{ID: "TX-004", UserID: "U4", Amount: 250000, Location: "Unknown-IP"}, // Fraud (Unknown-IP)
		{ID: "TX-005", UserID: "U5", Amount: 7500000, Location: "Surabaya"},
		{ID: "TX-006", UserID: "U6", Amount: 0, Location: "Medan"},             // Error (<=0)
		{ID: "TX-007", UserID: "U7", Amount: 12000000, Location: "Unknown-IP"}, // Fraud (Dua alasan)
		{ID: "TX-008", UserID: "U8", Amount: 300000, Location: "Bali"},
		{ID: "TX-009", UserID: "U9", Amount: 1000000, Location: "Jakarta"},
		{ID: "TX-010", UserID: "U10", Amount: 50000000, Location: "Semarang"}, // Fraud (>10jt)
	}
}