package service

import (
	"BlitzQueue/internal/service/reader"
	"BlitzQueue/internal/service/writer"
	"BlitzQueue/internal/storage"
	"github.com/gofiber/fiber/v2"
)

type QueueService interface {
	CreateQueue(c *fiber.Ctx) error
	GetQueueByName(c *fiber.Ctx) error
	PushMessages(c *fiber.Ctx) error
	PeekMessages(c *fiber.Ctx) error
	ConsumeMessages(c *fiber.Ctx) error
	ConfirmMessages(c *fiber.Ctx) error
	ExtendMessageTime(c *fiber.Ctx) error
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
