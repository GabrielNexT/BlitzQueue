package model

import (
	"errors"
	"github.com/gofiber/fiber/v2"
)

type HttpError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *HttpError) Error() error {
	return errors.New(e.Code)
}

func CreateBadRequestError(message string) HttpError {
	return HttpError{
		StatusCode: 400,
		Code:       "BAD_REQUEST",
		Message:    message,
	}
}

func CreateNotFoundError(message string) HttpError {
	return HttpError{
		StatusCode: 404,
		Code:       "NOT_FOUND",
		Message:    message,
	}
}

func CreateInternalServerError(message string) HttpError {
	return HttpError{
		StatusCode: 500,
		Code:       "INTERNAL_SERVER_ERROR",
		Message:    message,
	}
}

func ErrorResponse(ctx *fiber.Ctx, error HttpError) {
	err := ctx.Status(error.StatusCode).JSON(fiber.Map{
		"message":    error.Message,
		"statusCode": error.StatusCode,
		"code":       error.Code,
	})
	if err != nil {
		return
	}
}
