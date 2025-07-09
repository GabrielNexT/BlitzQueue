package service

import (
	"BlitzQueue/internal/model"
	"github.com/gofiber/fiber/v2"
	"strings"
)

func (s *queueService) CreateQueue(c *fiber.Ctx) error {
	request := model.CreateQueueRequest{}

	err := c.BodyParser(&request)

	if err != nil {
		httpError := model.CreateBadRequestError("failed to bind request")
		model.ErrorResponse(c, httpError)
		return err
	}

	// TODO: Create a regex to validate the name
	request.Name = strings.ToLower(request.Name)

	q, err := s.queueStorage.GetQueueByName(request.Name)

	if q != nil {
		httpError := model.CreateBadRequestError("queue already exist")
		model.ErrorResponse(c, httpError)
		return err
	}

	queue := &model.Queue{
		Name:             request.Name,
		Type:             request.Type,
		UseUniqueMessage: request.UseUniqueMessage,
	}

	q, err = s.queueStorage.CreateQueue(queue)

	if err != nil {
		httpError := model.CreateBadRequestError("failed to create queue")
		model.ErrorResponse(c, httpError)
	}

	_ = c.Status(200).JSON(q)
	return nil
}
