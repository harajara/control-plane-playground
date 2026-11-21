// logcheck asserts the logging standard: every application log line is one
// structured JSON record, no credential or connection string ever appears,
// and an idle stack is quiet. It reads container logs through the Docker API,
// so it runs anywhere the socket is mounted.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"tenant-control-plane/internal/dockerapi"
)

// forbidden substrings: structural markers that must never leak, plus the
// actual configured secrets so a check cannot pass by accident.
func forbidden() []string {
	v := []string{"postgres://", "postgresql://", "sealed_database_url", "\"password\"", "password="}
	for _, key := range []string{"CP_DB_PASSWORD", "TENANT_DB_PASSWORD", "ADMIN_PASSWORD", "M2M_SECRET", "WORKER_SEAL_KEY", "CONTROL_PLANE_RECEIVE_KEY"} {
		if value := os.Getenv(key); len(value) >= 8 {
			v = append(v, value)
		}
	}
	return v
}

func main() {
	tail := flag.Int("tail", 200, "log lines to inspect per container")
	maxLines := flag.Int("max-lines", 800, "maximum lines allowed across the tail window")
	window := flag.Duration("recent-window", time.Minute, "idle window used to measure the log rate")
	maxRecent := flag.Int("max-recent-lines", 8, "maximum lines an idle stack may produce within the window")
	flag.Parse()
	if err := run(*tail, *maxLines, *window, *maxRecent); err != nil {
		fmt.Fprintln(os.Stderr, "log check failed:", err)
		os.Exit(1)
	}
	fmt.Println("logs: meaningful, sanitized, and quiet while idle")
}

func run(tail, maxLines int, window time.Duration, maxRecent int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := dockerapi.New()
	if err != nil {
		return err
	}
	lines, err := collect(ctx, client, tail, 0)
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return errors.New("no application logs found; is the stack up?")
	}
	if len(lines) > maxLines {
		return fmt.Errorf("%d log lines in the tail window, limit %d", len(lines), maxLines)
	}
	for _, line := range lines {
		if err := checkLine(line); err != nil {
			return err
		}
	}
	recent, err := collect(ctx, client, 0, window)
	if err != nil {
		return err
	}
	if len(recent) > maxRecent {
		return fmt.Errorf("%d log lines in %v while idle, limit %d: something logs on a timer", len(recent), window, maxRecent)
	}
	fmt.Printf("checked %d historical and %d recent lines\n", len(lines), len(recent))
	return nil
}

// collect reads application lines for both roles, optionally restricted to a
// trailing time window.
func collect(ctx context.Context, client *dockerapi.Client, tail int, window time.Duration) ([]string, error) {
	var lines []string
	for _, role := range []string{"control-plane", "worker"} {
		out, err := client.Logs(ctx, role, tail, window)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) != "" {
				lines = append(lines, line)
			}
		}
	}
	return lines, nil
}

func checkLine(line string) error {
	for _, bad := range forbidden() {
		if strings.Contains(line, bad) {
			return fmt.Errorf("log line contains %q: %s", bad, trim(line))
		}
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return fmt.Errorf("log line is not a JSON record: %s", trim(line))
	}
	for _, key := range []string{"msg", "level", "time"} {
		if _, ok := record[key]; !ok {
			return fmt.Errorf("log record is missing %q: %s", key, trim(line))
		}
	}
	if strings.Contains(strings.ToLower(fmt.Sprint(record["msg"])), "heartbeat") {
		return fmt.Errorf("heartbeat noise is not allowed at info level: %s", trim(line))
	}
	return nil
}

func trim(s string) string {
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}
