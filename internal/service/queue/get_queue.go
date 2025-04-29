package service

import (
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"errors"
	"github.com/gin-gonic/gin"
	"strings"
)

func (s *queueService) GetQueueByName(c *gin.Context) {
	queueName := c.Param("name")

	if strings.TrimSpace(queueName) == "" {
		httpError := model.CreateBadRequestError("queue name cannot be empty")
		model.ErrorResponse(c, httpError)
		return
	}

	queueName = strings.ToLower(queueName)

	queue, err := s.queueStorage.GetQueueByName(queueName)

	if errors.Is(err, storage.ErrQueueNotExist) {
		httpError := model.CreateNotFoundError("queue not found")
		model.ErrorResponse(c, httpError)
		return
	}

	if err != nil {
		httpError := model.CreateInternalServerError("error getting queue")
		model.ErrorResponse(c, httpError)
		return
	}

	c.JSON(200, queue)
}
