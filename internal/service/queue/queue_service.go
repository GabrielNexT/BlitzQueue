package service

import (
	"BlitzQueue/internal/storage"
	"github.com/gin-gonic/gin"
)

type QueueService interface {
	CreateQueue(c *gin.Context)
	GetQueueByName(c *gin.Context)
	PushMessages(c *gin.Context)
	PeekMessages(c *gin.Context)
	ConsumeMessages(c *gin.Context)
}
type queueService struct {
	queueStorage storage.QueueStorage
}

func NewQueueService(queueStorage storage.QueueStorage) QueueService {
	return &queueService{
		queueStorage: queueStorage,
	}
}
