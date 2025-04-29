package main

import (
	service "BlitzQueue/internal/service/queue"
	"BlitzQueue/internal/storage"
	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	queueStorage := storage.NewQueueStorage()
	queueService := service.NewQueueService(queueStorage)

	router.POST("/queue", queueService.CreateQueue)
	router.GET("/queue/:name", queueService.GetQueueByName)

	err := router.Run(":52525")

	if err != nil {
		panic(err)
	}
}
