package service

import (
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"errors"
	"github.com/gin-gonic/gin"
	"strings"
	"unicode"
)

func (s *queueService) GetQueueByName(c *gin.Context) {
	queue, err := s.getQueueByNameFromContext(c)

	if err != nil {
		return
	}

	c.JSON(200, queue)
}

func (s *queueService) getQueueByNameFromContext(c *gin.Context) (*model.Queue, error) {
	queueName := c.Param("name")

	if err := validateQueueName(queueName); err != nil {
		httpError := model.CreateBadRequestError(err.Error())
		model.ErrorResponse(c, httpError)
		return nil, httpError.Error()
	}

	queueName = strings.ToLower(queueName)

	queue, err := s.queueStorage.GetQueueByName(queueName)

	if errors.Is(err, storage.ErrQueueNotExist) {
		httpError := model.CreateNotFoundError("queue not found")
		model.ErrorResponse(c, httpError)
		return nil, err
	}

	if err != nil {
		httpError := model.CreateInternalServerError("error getting queue")
		model.ErrorResponse(c, httpError)
		return nil, err
	}

	return queue, nil
}

func validateQueueName(queueName string) error {
	if strings.TrimSpace(queueName) == "" {
		return errors.New("queue name cannot be empty")
	}

	for _, char := range queueName {
		if unicode.IsLetter(char) || unicode.IsNumber(char) || char == '-' || char == '_' {
			continue
		}
		return errors.New("queue name can only contain letters and numbers")
	}

	return nil
}
