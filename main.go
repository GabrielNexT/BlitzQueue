package main

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
)

func main() {
	log := logger.GetLogger()
	queueStorage := storage.NewQueueStorage()

	_, err := queueStorage.CreateQueue("Teste", model.QueueTypeStandard)

	if err != nil {
		panic(err)
	}

	log.Info("Queue created")
}
