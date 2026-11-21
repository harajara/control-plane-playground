// Package applog installs the process-wide logger. The standard is
// deliberately strict: JSON, one line per consequential event (startup,
// request failure with its problem code, task transition, publish/retry/DLQ,
// provisioning outcome, shutdown), and never a credential, connection string
// or generated password. Anything that fires on a timer or a heartbeat belongs
// at debug level so a quiet system stays quiet in the log volume.
package applog

import (
	"log/slog"
	"os"
	"strings"
)

// Init configures slog once per process. CP_LOG_LEVEL selects the level.
func Init() {
	lvl := slog.LevelInfo
	switch strings.ToLower(os.Getenv("CP_LOG_LEVEL")) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}
