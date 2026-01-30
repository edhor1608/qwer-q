package main

import (
	"fmt"
	"os"
	"os/exec"
	"text/tabwriter"

	"github.com/jonas/qwer-q/pkg/client"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Schema management commands",
}

var schemaRegisterCmd = &cobra.Command{
	Use:   "register",
	Short: "Register a schema for a queue",
	RunE:  runSchemaRegister,
}

var schemaListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all registered schemas",
	RunE:  runSchemaList,
}

func init() {
	schemaRegisterCmd.Flags().StringP("queue", "q", "", "queue name (required)")
	schemaRegisterCmd.Flags().StringP("proto", "p", "", "proto file path (required)")
	schemaRegisterCmd.Flags().StringP("message", "m", "", "message type (required)")
	schemaRegisterCmd.MarkFlagRequired("queue")
	schemaRegisterCmd.MarkFlagRequired("proto")
	schemaRegisterCmd.MarkFlagRequired("message")

	schemaCmd.AddCommand(schemaRegisterCmd)
	schemaCmd.AddCommand(schemaListCmd)
	rootCmd.AddCommand(schemaCmd)
}

func runSchemaRegister(cmd *cobra.Command, args []string) error {
	broker, _ := cmd.Flags().GetString("broker")
	queue, _ := cmd.Flags().GetString("queue")
	protoFile, _ := cmd.Flags().GetString("proto")
	messageType, _ := cmd.Flags().GetString("message")

	// Generate FileDescriptorSet from proto file
	descriptor, err := compileProto(protoFile)
	if err != nil {
		return fmt.Errorf("failed to compile proto: %w", err)
	}

	c, err := client.Dial(broker)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer c.Close()

	resp, err := c.SchemaRegister(queue, descriptor, messageType)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	fmt.Printf("Schema registered for queue %q (version %d)\n", queue, resp.Version)
	return nil
}

func runSchemaList(cmd *cobra.Command, args []string) error {
	broker, _ := cmd.Flags().GetString("broker")

	c, err := client.Dial(broker)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer c.Close()

	resp, err := c.SchemaList()
	if err != nil {
		return fmt.Errorf("failed to list schemas: %w", err)
	}

	if len(resp.Schemas) == 0 {
		fmt.Println("No schemas registered")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tMESSAGE TYPE\tVERSION")
	for _, s := range resp.Schemas {
		fmt.Fprintf(w, "%s\t%s\t%d\n", s.Queue, s.MessageType, s.Version)
	}
	w.Flush()
	return nil
}

// compileProto compiles a .proto file to FileDescriptorSet bytes.
func compileProto(protoFile string) ([]byte, error) {
	tmpFile, err := os.CreateTemp("", "descriptor-*.pb")
	if err != nil {
		return nil, err
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cmd := exec.Command("protoc",
		"--descriptor_set_out="+tmpFile.Name(),
		"--include_imports",
		protoFile,
	)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("protoc failed: %w", err)
	}

	return os.ReadFile(tmpFile.Name())
}
