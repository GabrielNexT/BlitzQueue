package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

func (s *queueService) ExtendMessageTime(c *fiber.Ctx) error {
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

	var request model.ExtendMessageTimeRequest

	err = c.BodyParser(&request)

	if err != nil {
		log.Error("error parsing request body", slog.String("error", err.Error()))
		httpError := model.CreateInternalServerError("error parsing request body")
		model.ErrorResponse(c, httpError)
		return err
	}

	storageError := queueStorage.GetMoreTimeByIds(1+queue.MessageLockTimeout, request.MessageIds)

	if storageError == nil {
		_ = c.SendStatus(200)
		return err
	}

	if storageError.Type == storage.ErrMessageDoesNotExist {
		httpError := model.CreateNotFoundError(storageError.Error())
		model.ErrorResponse(c, httpError)
		return err
	}

	log.Error("error extending time", slog.String("error", storageError.Error()))
	httpError := model.CreateInternalServerError(storageError.Message)
	model.ErrorResponse(c, httpError)
	return nil
}
