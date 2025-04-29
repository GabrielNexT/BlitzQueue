package service

import (
	"BlitzQueue/internal/model"
	"github.com/gin-gonic/gin"
	"strings"
)

func (s *queueService) CreateQueue(c *gin.Context) {
	request := model.CreateQueueRequest{}

	err := c.ShouldBindJSON(&request)

	if err != nil {
		httpError := model.CreateBadRequestError("failed to bind request")
		model.ErrorResponse(c, httpError)
		return
	}

	// TODO: Create a regex to validate the name
	request.Name = strings.ToLower(request.Name)

	q, err := s.queueStorage.GetQueueByName(request.Name)

	if q != nil {
		httpError := model.CreateBadRequestError("queue already exist")
		model.ErrorResponse(c, httpError)
		return
	}

	q, err = s.queueStorage.CreateQueue(request.Name, request.Type)

	if err != nil {
		httpError := model.CreateBadRequestError("failed to create queue")
		model.ErrorResponse(c, httpError)
	}

	c.JSON(200, q)
}
