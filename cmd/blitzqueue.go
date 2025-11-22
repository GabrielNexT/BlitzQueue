package main

import (
	service "BlitzQueue/internal/service/queue"
	"BlitzQueue/internal/service/reader"
	"BlitzQueue/internal/service/writer"
	"BlitzQueue/internal/storage"
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
)

func main() {

	ctx := createContext()

	queueStorage := storage.NewQueueStorage()
	writerService := writer.NewWriterService(ctx, queueStorage)
	readerService := reader.NewReaderService(ctx, queueStorage)
	queueService := service.NewQueueService(queueStorage, writerService, readerService)

	app := fiber.New()

	app.Post("/queue", queueService.CreateQueue)
	app.Get("/queue/:name", queueService.GetQueueByName)
	app.Post("/queue/:name/push", queueService.PushMessages)
	app.Get("/queue/:name/peek", queueService.PeekMessages)
	app.Get("/queue/:name/consume", queueService.ConsumeMessages)
	app.Post("/queue/:name/confirm", queueService.ConfirmMessages)
	app.Post("/queue/:name/extend", queueService.ExtendMessageTime)

	gracefulShutdown(ctx, writerService)

	err := app.Listen(":52525")

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
