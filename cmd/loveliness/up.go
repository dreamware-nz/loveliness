package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// ANSI colors for node output prefixes.
var nodeColors = []string{
	"\033[36m", // cyan
	"\033[33m", // yellow
	"\033[35m", // magenta
	"\033[32m", // green
	"\033[34m", // blue
	"\033[31m", // red
}

const colorReset = "\033[0m"

func runUp(args []string) {
	nodeCount := 3
	clean := false
	basePort := 8080

	for i, arg := range args {
		switch {
		case arg == "--clean":
			clean = true
		case strings.HasPrefix(arg, "--port="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "--port=")); err == nil {
				basePort = n
			}
		default:
			if n, err := strconv.Atoi(arg); err == nil && n > 0 && i == 0 {
				nodeCount = n
			}
		}
	}

	dataDir := filepath.Join(homeDir(), ".loveliness", "up")

	if clean {
		os.RemoveAll(dataDir)
		fmt.Println("Cleaned data directory:", dataDir)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "error creating data dir: %v\n", err)
		os.Exit(1)
	}

	// Find available base port.
	basePort = findAvailablePort(basePort)

	fmt.Printf("Starting %d-node Loveliness cluster...\n", nodeCount)
	fmt.Printf("Data: %s\n", dataDir)
	fmt.Println()

	// Get the path to this executable.
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error finding executable: %v\n", err)
		os.Exit(1)
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup

	for i := 0; i < nodeCount; i++ {
		httpPort := basePort + i
		boltPort := 7687 + i
		raftPort := 9000 + i
		grpcPort := 9100 + i
		nodeID := fmt.Sprintf("node-%d", i)
		nodeDataDir := filepath.Join(dataDir, nodeID)

		env := []string{
			fmt.Sprintf("LOVELINESS_NODE_ID=%s", nodeID),
			fmt.Sprintf("LOVELINESS_BIND_ADDR=127.0.0.1:%d", httpPort),
			fmt.Sprintf("LOVELINESS_BOLT_ADDR=127.0.0.1:%d", boltPort),
			fmt.Sprintf("LOVELINESS_RAFT_ADDR=127.0.0.1:%d", raftPort),
			fmt.Sprintf("LOVELINESS_GRPC_ADDR=127.0.0.1:%d", grpcPort),
			fmt.Sprintf("LOVELINESS_DATA_DIR=%s", nodeDataDir),
			fmt.Sprintf("LOVELINESS_SHARD_COUNT=%d", shardCount()),
		}

		if i == 0 {
			env = append(env, "LOVELINESS_BOOTSTRAP=true")
		} else {
			env = append(env, fmt.Sprintf("LOVELINESS_PEERS=127.0.0.1:%d", 9000))
		}

		cmd := exec.Command(self, "serve")
		cmd.Env = append(os.Environ(), env...)

		color := nodeColors[i%len(nodeColors)]
		prefix := fmt.Sprintf("%s[%s]%s ", color, nodeID, colorReset)

		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		wg.Add(1)
		go func(r io.ReadCloser, pfx string) {
			defer wg.Done()
			prefixLines(r, pfx)
		}(stdout, prefix)

		wg.Add(1)
		go func(r io.ReadCloser, pfx string) {
			defer wg.Done()
			prefixLines(r, pfx)
		}(stderr, prefix)

		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "error starting %s: %v\n", nodeID, err)
			// Kill any already-started nodes.
			for _, c := range cmds {
				c.Process.Signal(syscall.SIGTERM)
			}
			os.Exit(1)
		}

		cmds = append(cmds, cmd)
	}

	fmt.Println()
	fmt.Printf("Cluster ready (%d nodes).\n", nodeCount)
	fmt.Println()
	fmt.Printf("  HTTP:  http://localhost:%d\n", basePort)
	fmt.Printf("  Bolt:  bolt://localhost:7687\n")
	fmt.Printf("  Nodes: %d\n", nodeCount)
	fmt.Println()
	fmt.Println("Press Ctrl-C to stop.")
	fmt.Println()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down cluster...")

	// Send SIGTERM to all children.
	for _, cmd := range cmds {
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
		}
	}

	// Wait for all children to exit.
	for _, cmd := range cmds {
		cmd.Wait()
	}

	wg.Wait()
	fmt.Println("All nodes stopped.")
}

func prefixLines(r io.Reader, prefix string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Printf("%s%s\n", prefix, scanner.Text())
	}
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "."
}

func shardCount() int {
	if v := os.Getenv("LOVELINESS_SHARD_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 3
}

// findAvailablePort tries the given port and increments by 10 until one is free.
func findAvailablePort(start int) int {
	for port := start; port < start+100; port += 10 {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			ln.Close()
			return port
		}
	}
	return start
}
