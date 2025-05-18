package main

import (
	service "BlitzQueue/internal/service/queue"
	"BlitzQueue/internal/service/writer"
	"BlitzQueue/internal/storage"
	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	queueStorage := storage.NewQueueStorage()
	writerService := writer.NewWriterService(queueStorage)
	queueService := service.NewQueueService(queueStorage, writerService)

	router.POST("/queue", queueService.CreateQueue)
	router.GET("/queue/:name", queueService.GetQueueByName)
	router.POST("/queue/:name/push", queueService.PushMessages)
	router.GET("/queue/:name/peek", queueService.PeekMessages)
	router.GET("/queue/:name/consume", queueService.ConsumeMessages)
	router.POST("/queue/:name/confirm", queueService.ConfirmMessages)
	router.POST("/queue/:name/extend", queueService.ExtendMessageTime)

	err := router.Run(":52525")

	if err != nil {
		panic(err)
	}
}
