package logger

import (
	"log/slog"
	"os"
)

func GetLogger() *slog.Logger {
	textHandler := slog.NewTextHandler(os.Stdout, nil)
	return slog.New(textHandler)
}
