package main

import (
	"log"

	"github.com/appbrew/redis-inventory/cmd/app"
	"github.com/spf13/cobra/doc"
)

func main() {
	err := doc.GenMarkdownTree(app.RootCmd, "./docs/cobra")
	if err != nil {
		log.Fatal(err)
	}
}
