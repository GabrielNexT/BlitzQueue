package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"github.com/gin-gonic/gin"
	"log/slog"
)

func (s *queueService) ConsumeMessages(c *gin.Context) {
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

	messages, err := queueStorage.ConsumeMessages()

	if err != nil {
		httpError := model.CreateInternalServerError("error consuming messages")
		model.ErrorResponse(c, httpError)
		return
	}

	c.JSON(200, messages)
}
