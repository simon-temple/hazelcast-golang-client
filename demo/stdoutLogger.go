package main

import (
	"fmt"
	"os"
)

type StdoutLogger struct{}

func NewLogger() *StdoutLogger {
	return new(StdoutLogger)
}

func (l *StdoutLogger) Trace(format string, v ...interface{}) {
	fmt.Print("TRACE: ")
	fmt.Printf(format + "\n", v...)
}

func (l *StdoutLogger) Info(format string, v ...interface{}) {
	fmt.Print("INFO: ")
	fmt.Printf(format + "\n", v...)
}

func (l *StdoutLogger) Warn(format string, v ...interface{}) {
	fmt.Print("WARN: ")
	fmt.Printf(format + "\n", v...)
}

func (l *StdoutLogger) Error(format string, v ...interface{}) {
	fmt.Print("ERROR: ")
	fmt.Printf(format + "\n", v...)
}

func (l *StdoutLogger) Fatal(format string, v ...interface{}) {
	fmt.Print("FATAL: ")
	fmt.Printf(format + "\n", v...)
	os.Exit(1)
}
