package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"github.com/gofiber/fiber/v2"
	"log/slog"
)

func (s *queueService) ConsumeMessages(c *fiber.Ctx) error {
	log := logger.GetLogger()
	queue, err := s.getQueueByNameFromContext(c)

	if err != nil {
		log.Error("error getting queue from context")
		return err
	}

	log = log.With(slog.String("queueName", queue.Name))
	log.Debug("got queue from path param")

	messages, err := s.readerService.ConsumeMessages(queue)

	if err != nil {
		httpError := model.CreateInternalServerError("error consuming messages")
		model.ErrorResponse(c, httpError)
		return err
	}

	_ = c.Status(200).JSON(messages)
	return nil
}
