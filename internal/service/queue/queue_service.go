package service

import (
	"BlitzQueue/internal/service/reader"
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
	ConfirmMessages(c *gin.Context)
	ExtendMessageTime(c *gin.Context)
}
type queueService struct {
	queueStorage  storage.QueueStorage
	writerService writer.WriterService
	readerService reader.ReaderService
}

func NewQueueService(queueStorage storage.QueueStorage, writerService writer.WriterService, readerService reader.ReaderService) QueueService {
	return &queueService{
		queueStorage:  queueStorage,
		writerService: writerService,
		readerService: readerService,
	}
}
