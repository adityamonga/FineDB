package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	db "github.com/adityamonga/KosDB/pkg"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	items := &db.TransactionStack{}
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
