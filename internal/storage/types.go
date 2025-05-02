package storage

import "BlitzQueue/internal/model"

type MessageStorage interface {
	PushMessages(messages ...*model.Message) error
	PeekMessages() ([]*model.Message, error)
	GetType() string
	ConsumeMessages() ([]*model.ConsumeMessageResponse, error)
}
