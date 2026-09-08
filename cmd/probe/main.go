package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(
		os.Stderr,
		"direct probe invocation is disabled; use the fenced cmd/shim worker assignment",
	)
	os.Exit(2)
}
