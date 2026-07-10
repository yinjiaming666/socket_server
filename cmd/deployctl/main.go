package main

import (
	"context"
	"fmt"
	"os"

	"socket_server/internal/deployctl"
)

func main() {
	root, err := deployctl.FindRootDir(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cfg := deployctl.ConfigFromEnv(root)
	if err := deployctl.Execute(context.Background(), cfg, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
