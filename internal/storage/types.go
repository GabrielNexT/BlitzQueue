package storage

import "BlitzQueue/internal/model"

type MessageStorage interface {
	PushMessages(messages ...*model.Message) error
	PeekMessages() ([]*model.Message, error)
	GetType() string
	ConsumeMessages() ([]*model.ConsumeMessageResponse, error)
	ConfirmMessagesByIds(messageIds []string) *MessageStorageError
	GetMoreTimeByIds(minutes int, messageIds []string) *MessageStorageError
}

type MessageStorageErrorType int8

const (
	ErrMessageDoesNotExist = iota
	ErrInternalError
	ErrMessageAlreadyProcessed
	ErrMessageIsNotInProcessingState
)

type MessageStorageError struct {
	Message string
	Type    MessageStorageErrorType
}

func (e *MessageStorageError) Error() string {
	return e.Message
}

func NewMessageStorageError(message string, errorType MessageStorageErrorType) *MessageStorageError {
	return &MessageStorageError{
		Message: message,
		Type:    errorType,
	}
}
