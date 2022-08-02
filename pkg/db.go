package db

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
)

var GlobalStore map[string]string

type Storage struct {
	path       string
	permission fs.FileMode
}

type Transaction struct {
	store map[string]string
	next  *Transaction
}

type TransactionStack struct {
	top     *Transaction
	size    int
	Storage *Storage
}

func NewStorage(load_history bool) (s *Storage) {
	s = &Storage{
		path:       "/tmp/KosDB.json",
		permission: fs.FileMode(0644)}
	GlobalStore = make(map[string]string)

	if load_history {
		jsonFile, err := os.Open(s.path)
		if err != nil {
			fmt.Println(err)
		}
		defer jsonFile.Close()
		byteValue, _ := ioutil.ReadAll(jsonFile)
		json.Unmarshal(byteValue, &GlobalStore)
	}
	return
}

func (ts *TransactionStack) Persist() {
	data, err := json.Marshal(GlobalStore)
	if err != nil {
		fmt.Println("ERROR: Could not serialize store")
	}
	err = os.WriteFile(ts.Storage.path, data, ts.Storage.permission)
	if err != nil {
		fmt.Println("ERROR: Persist failed")
	}
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

func (ts *TransactionStack) Count(val string) {
	ActiveTransaction := ts.Peek()
	count := 0
	var store map[string]string
	if ActiveTransaction != nil {
		store = ActiveTransaction.store
	} else {
		store = GlobalStore
	}
	for _, value := range store {
		if value == val {
			count++
		}
	}
	fmt.Println(count)
}

func (ts *TransactionStack) Keys() {
	ActiveTransaction := ts.Peek()
	keys := make([]string, 0)

	for k := range GlobalStore {
		keys = append(keys, k)
	}
	if ActiveTransaction != nil {
		for {
			for k := range ActiveTransaction.store {
				keys = append(keys, k)
			}
			if ActiveTransaction.next == nil {
				break
			}
			ActiveTransaction = ActiveTransaction.next
		}
	}
	fmt.Println(keys)
}
