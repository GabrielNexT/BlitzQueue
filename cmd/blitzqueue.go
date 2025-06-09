package main

import (
	service "BlitzQueue/internal/service/queue"
	"BlitzQueue/internal/service/writer"
	"BlitzQueue/internal/storage"
	"context"
	"github.com/gin-gonic/gin"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx := createContext()

	queueStorage := storage.NewQueueStorage()
	writerService := writer.NewWriterService(ctx, queueStorage)
	queueService := service.NewQueueService(queueStorage, writerService)

	router := gin.Default()

	router.POST("/queue", queueService.CreateQueue)
	router.GET("/queue/:name", queueService.GetQueueByName)
	router.POST("/queue/:name/push", queueService.PushMessages)
	router.GET("/queue/:name/peek", queueService.PeekMessages)
	router.GET("/queue/:name/consume", queueService.ConsumeMessages)
	router.POST("/queue/:name/confirm", queueService.ConfirmMessages)
	router.POST("/queue/:name/extend", queueService.ExtendMessageTime)

	gracefulShutdown(ctx, writerService)

	err := router.Run(":52525")

	if err != nil {
		panic(err)
	}

}

func createContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		_ = <-sigs
		cancel()
	}()

	return ctx
}

func gracefulShutdown(ctx context.Context, writerService writer.WriterService) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = <-writerService.GetStopChannel()
				os.Exit(0)
			}
		}
	}()

}
