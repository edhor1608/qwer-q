package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/jonas/qwer-q/pkg/client"
	"github.com/spf13/cobra"
)

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Queue management commands",
}

var queueListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all queues",
	RunE:  runQueueList,
}

func init() {
	queueCmd.AddCommand(queueListCmd)
	rootCmd.AddCommand(queueCmd)
}

func runQueueList(cmd *cobra.Command, args []string) error {
	broker, _ := cmd.Flags().GetString("broker")

	c, err := client.Dial(broker)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer c.Close()

	resp, err := c.QueueList()
	if err != nil {
		return fmt.Errorf("failed to list queues: %w", err)
	}

	if len(resp.Queues) == 0 {
		fmt.Println("No queues")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tMESSAGES\tIN-FLIGHT")
	for _, q := range resp.Queues {
		fmt.Fprintf(w, "%s\t%d\t%d\n", q.Name, q.MessageCount, q.InFlightCount)
	}
	w.Flush()
	return nil
}
