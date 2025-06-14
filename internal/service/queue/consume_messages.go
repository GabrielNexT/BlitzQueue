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

	messages, err := s.readerService.ConsumeMessages(queue)

	if err != nil {
		httpError := model.CreateInternalServerError("error consuming messages")
		model.ErrorResponse(c, httpError)
		return
	}

	c.JSON(200, messages)
}
