package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

func (s *queueService) GetAllQueues(c *fiber.Ctx) error {
	log := logger.GetLogger()
	log.Debug("getting all queues")

	queues, err := s.queueStorage.GetAllQueues()

	if err != nil {
		log.Error("error getting all queues", slog.String("error", err.Error()))
		httpError := model.CreateInternalServerError("error getting all queues")
		model.ErrorResponse(c, httpError)
		return err
	}

	log.Debug("got all queues", slog.Int("count", len(queues)))

	_ = c.Status(200).JSON(queues)
	return nil
}
