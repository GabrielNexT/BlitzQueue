package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"github.com/gin-gonic/gin"
	"log/slog"
)

func (s *queueService) ConfirmMessages(c *gin.Context) {
	log := logger.GetLogger()
	queue, err := s.getQueueByNameFromContext(c)

	if err != nil {
		log.Error("error getting queue from context")
		return
	}

	log = log.With(slog.String("queueName", queue.Name))
	log.Debug("got queue from path param")

	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		httpError := model.CreateInternalServerError("error getting message storage")
		model.ErrorResponse(c, httpError)
		log.Error("error getting message storage")
		return
	}
	log.Debug("got storage for queue")

	var request model.ConfirmMessagesRequest

	err = c.BindJSON(&request)

	if err != nil {
		log.Error("error parsing request body", slog.String("error", err.Error()))
		httpError := model.CreateInternalServerError("error parsing request body")
		model.ErrorResponse(c, httpError)
		return
	}

	storageError := queueStorage.ConfirmMessagesByIds(request.MessageIds)

	if storageError == nil {
		c.JSON(200, nil)
		return
	}

	if storageError.Type == storage.ErrMessageDoesNotExist {
		httpError := model.CreateNotFoundError(storageError.Error())
		model.ErrorResponse(c, httpError)
		return
	}

	log.Error("error confirming messages", slog.String("error", storageError.Error()))
	httpError := model.CreateNotFoundError(storageError.Message)
	model.ErrorResponse(c, httpError)
	return
}
