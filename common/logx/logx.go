package logx

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// ANSI color codes (simple and portable). Disable with NO_COLOR or LOG_NO_COLOR.
const (
	ColorReset   = "\u001b[0m"
	ColorDim     = "\u001b[2m"
	ColorRed     = "\u001b[31m"
	ColorGreen   = "\u001b[32m"
	ColorYellow  = "\u001b[33m"
	ColorBlue    = "\u001b[34m"
	ColorMagenta = "\u001b[35m"
	ColorCyan    = "\u001b[36m"
	ColorGray    = "\u001b[90m"
)

// Logger is a tiny, colored logger for terminals.
// It keeps output serialized to avoid interleaving in concurrent goroutines.
type Logger struct {
	mu        sync.Mutex
	component string
	color     string
	out       io.Writer
}

// New creates a logger for a component (e.g., BROKER, PUB-1).
func New(component string, color string) *Logger {
	return &Logger{
		component: component,
		color:     color,
		out:       os.Stdout,
	}
}

// SetOutput changes the writer (useful for tests).
func (l *Logger) SetOutput(out io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = out
}

// Infof prints an informational message.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log("INFO", ColorGreen, format, args...)
}

// Warnf prints a warning message.
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log("WARN", ColorYellow, format, args...)
}

// Errorf prints an error message.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", ColorRed, format, args...)
}

// Debugf prints a debug message (kept simple, always enabled).
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", ColorGray, format, args...)
}

func (l *Logger) log(level string, levelColor string, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if colorEnabled() {
		fmt.Fprintf(
			l.out,
			"%s%s%s %s[%s]%s %s%s%s | %s\n",
			ColorDim,
			timestamp,
			ColorReset,
			l.color,
			l.component,
			ColorReset,
			levelColor,
			level,
			ColorReset,
			msg,
		)
		return
	}

	fmt.Fprintf(l.out, "%s [%s] %s | %s\n", timestamp, l.component, level, msg)
}

func colorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("LOG_NO_COLOR") != "" {
		return false
	}
	return true
}
