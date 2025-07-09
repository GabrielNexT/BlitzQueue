package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"github.com/gofiber/fiber/v2"
	"log/slog"
)

type pushMessageOkResponse struct{}

func (s *queueService) PushMessages(c *fiber.Ctx) error {
	log := logger.GetLogger()
	queue, err := s.getQueueByNameFromContext(c)

	if err != nil {
		log.Error("error getting queue from context")
		return err
	}

	log = log.With(slog.String("queueName", queue.Name))
	log.Debug("got queue from path param")

	_, err = s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		httpError := model.CreateInternalServerError("error getting message storage")
		model.ErrorResponse(c, httpError)
		log.Error("error getting message storage")
		return err
	}

	log.Debug("got storage for queue")
	var messagesRequest []model.CreateMessageRequest

	err = c.BodyParser(&messagesRequest)

	if err != nil {
		log.Error("error parsing request body", slog.String("error", err.Error()))
		httpError := model.CreateInternalServerError("error parsing request body")
		model.ErrorResponse(c, httpError)
		return err
	}

	messagesToPush := make([]*model.Message, len(messagesRequest))
	for idx, message := range messagesRequest {
		messagesToPush[idx] = model.CreateMessageFromRequest(message, queue)

	}

	err = s.writerService.PushMessages(queue, messagesToPush...)

	if err != nil {
		httpError := model.CreateInternalServerError("error pushing messages")
		model.ErrorResponse(c, httpError)
		return err
	}

	_ = c.Status(200).JSON(pushMessageOkResponse{})
	return nil
}
