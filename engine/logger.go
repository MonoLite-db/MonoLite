// Created by Yanjunhui

package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// 日志级别
// EN: Log levels.
const (
	LogLevelDebug = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// 日志级别名称
// EN: Log level names.
var logLevelNames = map[int]string{
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
}

// LogEntry 结构化日志条目
// EN: LogEntry is a structured log record.
type LogEntry struct {
	Timestamp  time.Time              `json:"ts"`
	Level      string                 `json:"level"`
	Component  string                 `json:"component,omitempty"`
	Message    string                 `json:"msg"`
	Context    map[string]interface{} `json:"ctx,omitempty"`
	DurationMs int64                  `json:"durationMs,omitempty"`
}

// Logger 日志器
// EN: Logger writes structured JSON logs.
type Logger struct {
	mu            sync.Mutex
	output        io.Writer
	level         int
	component     string
	slowThreshold time.Duration // 慢查询阈值 (EN: slow query threshold)
}

// 全局日志器
// EN: Global default logger.
var defaultLogger = NewLogger(os.Stdout)

// NewLogger 创建新的日志器
// EN: NewLogger creates a new logger.
func NewLogger(output io.Writer) *Logger {
	return &Logger{
		output:        output,
		level:         LogLevelInfo,
		component:     "MONODB",
		slowThreshold: 100 * time.Millisecond, // 默认 100ms 为慢查询 (EN: default slow threshold is 100ms)
	}
}

// SetLevel 设置日志级别
// EN: SetLevel sets the minimum log level.
func (l *Logger) SetLevel(level int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetSlowThreshold 设置慢查询阈值
// EN: SetSlowThreshold sets the slow-operation threshold.
func (l *Logger) SetSlowThreshold(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slowThreshold = d
}

// SetOutput 设置输出目标
// EN: SetOutput sets the output writer.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

// WithComponent 创建带组件名的日志器副本
// EN: WithComponent returns a logger copy with a different component name.
func (l *Logger) WithComponent(name string) *Logger {
	return &Logger{
		output:        l.output,
		level:         l.level,
		component:     name,
		slowThreshold: l.slowThreshold,
	}
}

// log 写入日志
// EN: log writes a log entry.
func (l *Logger) log(level int, msg string, ctx map[string]interface{}, duration time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level {
		return
	}

	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     logLevelNames[level],
		Component: l.component,
		Message:   msg,
		Context:   ctx,
	}

	if duration > 0 {
		entry.DurationMs = duration.Milliseconds()
	}

	// JSON 格式输出
	// EN: Output in JSON format.
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(l.output, "[ERROR] Failed to marshal log entry: %v\n", err)
		return
	}

	l.output.Write(data)
	l.output.Write([]byte("\n"))
}

// Debug 调试日志
// EN: Debug logs at DEBUG level.
func (l *Logger) Debug(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelDebug, msg, c, 0)
}

// Info 信息日志
// EN: Info logs at INFO level.
func (l *Logger) Info(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelInfo, msg, c, 0)
}

// Warn 警告日志
// EN: Warn logs at WARN level.
func (l *Logger) Warn(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelWarn, msg, c, 0)
}

// Error 错误日志
// EN: Error logs at ERROR level.
func (l *Logger) Error(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelError, msg, c, 0)
}

// LogSlowOperation 记录慢操作
// EN: LogSlowOperation records slow operations (duration >= threshold).
func (l *Logger) LogSlowOperation(op string, duration time.Duration, ctx map[string]interface{}) {
	if duration < l.slowThreshold {
		return
	}

	if ctx == nil {
		ctx = make(map[string]interface{})
	}
	ctx["operation"] = op
	ctx["slowThreshold"] = l.slowThreshold.String()

	l.log(LogLevelWarn, "slow operation detected", ctx, duration)
}

// LogQuery 记录查询（如果超过阈值则记录为慢查询）
// EN: LogQuery records queries; if duration exceeds threshold, it logs as a slow query.
func (l *Logger) LogQuery(collection string, filter interface{}, duration time.Duration, docsScanned, docsReturned int64) {
	ctx := map[string]interface{}{
		"collection":   collection,
		"docsScanned":  docsScanned,
		"docsReturned": docsReturned,
	}

	if filter != nil {
		ctx["filter"] = filter
	}

	if duration >= l.slowThreshold {
		l.log(LogLevelWarn, "slow query", ctx, duration)
	} else {
		l.log(LogLevelDebug, "query", ctx, duration)
	}
}

// LogCommand 记录命令执行
// EN: LogCommand records command execution.
func (l *Logger) LogCommand(cmdName string, duration time.Duration, success bool, ctx map[string]interface{}) {
	if ctx == nil {
		ctx = make(map[string]interface{})
	}
	ctx["command"] = cmdName
	ctx["success"] = success

	level := LogLevelDebug
	msg := "command executed"

	if !success {
		level = LogLevelWarn
		msg = "command failed"
	} else if duration >= l.slowThreshold {
		level = LogLevelWarn
		msg = "slow command"
	}

	l.log(level, msg, ctx, duration)
}

// 全局日志函数
// EN: Global logging helpers.

// GetLogger 获取默认日志器
// EN: GetLogger returns the default logger.
func GetLogger() *Logger {
	return defaultLogger
}

// SetLogLevel 设置全局日志级别
// EN: SetLogLevel sets the global log level.
func SetLogLevel(level int) {
	defaultLogger.SetLevel(level)
}

// SetSlowQueryThreshold 设置慢查询阈值
// EN: SetSlowQueryThreshold sets the global slow-query threshold.
func SetSlowQueryThreshold(d time.Duration) {
	defaultLogger.SetSlowThreshold(d)
}

// LogInfo 全局信息日志
// EN: LogInfo writes an INFO log using the default logger.
func LogInfo(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Info(msg, ctx...)
}

// LogWarn 全局警告日志
// EN: LogWarn writes a WARN log using the default logger.
func LogWarn(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Warn(msg, ctx...)
}

// LogError 全局错误日志
// EN: LogError writes an ERROR log using the default logger.
func LogError(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Error(msg, ctx...)
}

// LogDebug 全局调试日志
// EN: LogDebug writes a DEBUG log using the default logger.
func LogDebug(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Debug(msg, ctx...)
}
