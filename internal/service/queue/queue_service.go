package service

import (
	"BlitzQueue/internal/service/writer"
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
	queueStorage  storage.QueueStorage
	writerService writer.WriterService
}

func NewQueueService(queueStorage storage.QueueStorage, writerService writer.WriterService) QueueService {
	return &queueService{
		queueStorage:  queueStorage,
		writerService: writerService,
	}
}
