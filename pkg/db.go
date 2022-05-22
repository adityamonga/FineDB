package db

import (
	"fmt"
)

var GlobalStore = make(map[string]string)

type Transaction struct {
	store map[string]string
	next *Transaction
}

type TransactionStack struct {
	top *Transaction
	size int
}

func (ts *TransactionStack) PushTransaction() {
	var transaction = &Transaction{store: make(map[string]string)}
	transaction.next = ts.top
	ts.top = transaction
	ts.size++
}

func (ts *TransactionStack) PopTransaction() {
	if ts.top == nil {
		fmt.Println("ERROR: No Active Transaction")	
	} else {
		ts.top = ts.top.next
		ts.size--
	}
}

func (ts *TransactionStack) Peek() *Transaction {
	return ts.top
}


func (ts *TransactionStack) Commit() {
	var ActiveTransaction = ts.Peek()
	if ActiveTransaction != nil {
		for key, value := range ActiveTransaction.store {
			if ActiveTransaction.next != nil {
				ActiveTransaction.next.store[key] = value
			} else {
				GlobalStore[key] = value
			}
		}
	} else {
		fmt.Println("INFO: Nothing to commit")
	}
}


func (ts *TransactionStack) Rollback() {
	if ts.top != nil {
		for key := range ts.top.store {
			delete(ts.top.store, key)
		}
	} else {
		fmt.Println("Error: No Active Transaction")
	}
}


func (ts *TransactionStack) Set(key, value string) {
	ActiveTransaction := ts.Peek()
	if ActiveTransaction != nil {
		ActiveTransaction.store[key] = value
	} else {
		GlobalStore[key] = value
	}
}

func (ts *TransactionStack) Get(key string) {
	ActiveTransaction := ts.Peek()
	if ActiveTransaction != nil {
		if val, ok := ActiveTransaction.store[key]; ok {
			fmt.Println(val)
		} else {
			fmt.Println(key, "not set")
		}
	} else {
		if val, ok := GlobalStore[key]; ok {
			fmt.Println(val)
		} else {
			fmt.Println(key, "not set")
		}
	}
}

func (ts *TransactionStack) Delete(key string) {
	ActiveTransaction := ts.Peek()
	if ActiveTransaction != nil {
		if _, ok := ActiveTransaction.store[key]; ok {
			delete(ActiveTransaction.store, key)
		} else {
			fmt.Println(key, "not set")
		}
	} else {
		if _, ok := GlobalStore[key]; ok {
			delete(GlobalStore, key)
		} else {
			fmt.Println(key, "not set")
		}
	}
}
