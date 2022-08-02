package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	db "github.com/adityamonga/KosDB/pkg"
)

func main() {
	var load_history bool
	flag.BoolVar(&load_history, "load_history", false, "Load last persisted data.")
	flag.Parse()
	reader := bufio.NewReader(os.Stdin)
	storage := db.NewStorage(load_history)
	items := &db.TransactionStack{Storage: storage}
	for {
		fmt.Printf("> ")
		text, _ := reader.ReadString('\n')
		if text == "\n" {
			continue
		}
		// split the text into operation strings
		operation := strings.Fields(text)

		switch strings.ToUpper(operation[0]) {
		case "BEGIN":
			items.PushTransaction()
		case "ROLLBACK":
			items.Rollback()
		case "COMMIT":
			items.Commit()
			items.PopTransaction()
		case "END":
			items.PopTransaction()
		case "SET":
			items.Set(operation[1], operation[2])
		case "GET":
			items.Get(operation[1])
		case "DELETE":
			items.Delete(operation[1])
		case "COUNT":
			items.Count(operation[1])
		case "PERSIST":
			items.Persist()
		case "STOP":
			os.Exit(0)
		case "EXIT":
			os.Exit(0)
		default:
			fmt.Printf("ERROR: Unrecognised Operation %s\n", operation[0])
		}
	}
}
