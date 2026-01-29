package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "0.1.0"

var rootCmd = &cobra.Command{
	Use:   "qwer-q",
	Short: "QWER-Q message queue",
	Long:  "A lightweight protobuf-first message queue",
}

func init() {
	rootCmd.Version = version
	rootCmd.SetVersionTemplate("qwer-q version {{.Version}}\n")
	rootCmd.PersistentFlags().StringP("broker", "b", "localhost:9876", "broker address")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
