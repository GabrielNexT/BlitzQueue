package service

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"github.com/gin-gonic/gin"
	"log/slog"
)

func (s *queueService) PushMessages(c *gin.Context) {
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
	var messagesRequest []model.CreateMessageRequest

	err = c.BindJSON(&messagesRequest)

	if err != nil {
		httpError := model.CreateInternalServerError("error parsing request body")
		model.ErrorResponse(c, httpError)
		return
	}

	messagesToPush := make([]*model.Message, len(messagesRequest))
	for idx, message := range messagesRequest {
		messagesToPush[idx] = model.CreateMessageFromRequest(message)
	}

	err = queueStorage.PushMessages(messagesToPush...)

	if err != nil {
		httpError := model.CreateInternalServerError("error pushing messages")
		model.ErrorResponse(c, httpError)
		return
	}

}
