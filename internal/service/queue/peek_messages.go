package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"github.com/gofiber/fiber/v2"
	"log/slog"
)

func (s *queueService) PeekMessages(c *fiber.Ctx) error {
	log := logger.GetLogger()
	queue, err := s.getQueueByNameFromContext(c)

	if err != nil {
		log.Error("error getting queue from context")
		return err
	}

	log = log.With(slog.String("queueName", queue.Name))
	log.Debug("got queue from path param")

	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		httpError := model.CreateInternalServerError("error getting message storage")
		model.ErrorResponse(c, httpError)
		log.Error("error getting message storage")
		return err
	}

	log.Debug("got storage for queue")

	data, err := queueStorage.PeekMessages()

	if err != nil {
		httpError := model.CreateInternalServerError("error peeking messages")
		model.ErrorResponse(c, httpError)
		return err
	}

	_ = c.Status(200).JSON(data)
	return nil
}
