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
const (
	LogLevelDebug = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// 日志级别名称
var logLevelNames = map[int]string{
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
}

// LogEntry 结构化日志条目
type LogEntry struct {
	Timestamp   time.Time              `json:"ts"`
	Level       string                 `json:"level"`
	Component   string                 `json:"component,omitempty"`
	Message     string                 `json:"msg"`
	Context     map[string]interface{} `json:"ctx,omitempty"`
	DurationMs  int64                  `json:"durationMs,omitempty"`
}

// Logger 日志器
type Logger struct {
	mu         sync.Mutex
	output     io.Writer
	level      int
	component  string
	slowThreshold time.Duration // 慢查询阈值
}

// 全局日志器
var defaultLogger = NewLogger(os.Stdout)

// NewLogger 创建新的日志器
func NewLogger(output io.Writer) *Logger {
	return &Logger{
		output:        output,
		level:         LogLevelInfo,
		component:     "MONODB",
		slowThreshold: 100 * time.Millisecond, // 默认 100ms 为慢查询
	}
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetSlowThreshold 设置慢查询阈值
func (l *Logger) SetSlowThreshold(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slowThreshold = d
}

// SetOutput 设置输出目标
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

// WithComponent 创建带组件名的日志器副本
func (l *Logger) WithComponent(name string) *Logger {
	return &Logger{
		output:        l.output,
		level:         l.level,
		component:     name,
		slowThreshold: l.slowThreshold,
	}
}

// log 写入日志
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
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(l.output, "[ERROR] Failed to marshal log entry: %v\n", err)
		return
	}
	
	l.output.Write(data)
	l.output.Write([]byte("\n"))
}

// Debug 调试日志
func (l *Logger) Debug(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelDebug, msg, c, 0)
}

// Info 信息日志
func (l *Logger) Info(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelInfo, msg, c, 0)
}

// Warn 警告日志
func (l *Logger) Warn(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelWarn, msg, c, 0)
}

// Error 错误日志
func (l *Logger) Error(msg string, ctx ...map[string]interface{}) {
	var c map[string]interface{}
	if len(ctx) > 0 {
		c = ctx[0]
	}
	l.log(LogLevelError, msg, c, 0)
}

// LogSlowOperation 记录慢操作
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

// GetLogger 获取默认日志器
func GetLogger() *Logger {
	return defaultLogger
}

// SetLogLevel 设置全局日志级别
func SetLogLevel(level int) {
	defaultLogger.SetLevel(level)
}

// SetSlowQueryThreshold 设置慢查询阈值
func SetSlowQueryThreshold(d time.Duration) {
	defaultLogger.SetSlowThreshold(d)
}

// LogInfo 全局信息日志
func LogInfo(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Info(msg, ctx...)
}

// LogWarn 全局警告日志
func LogWarn(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Warn(msg, ctx...)
}

// LogError 全局错误日志
func LogError(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Error(msg, ctx...)
}

// LogDebug 全局调试日志
func LogDebug(msg string, ctx ...map[string]interface{}) {
	defaultLogger.Debug(msg, ctx...)
}

