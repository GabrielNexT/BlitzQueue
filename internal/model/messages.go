package model

import (
	"github.com/oklog/ulid/v2"
	"time"
)

type MessageStatus int8

const (
	MessageStatusInQueue MessageStatus = iota
	MessageStatusProcessing
	MessageStatusProcessed
)

type CreateMessageRequest struct {
	Data     string
	Priority int
}

type Message struct {
	Id        string
	QueueId   *string
	Data      string
	Status    MessageStatus `gorm:"index"`
	Hash      *string       // Only for unique queue config
	Priority  *int          // Only for the priority queue type
	NextRun   *time.Time    // Enable only when queue backoff is enable
	LockUntil *time.Time    `gorm:"index"`
}

type ConsumeMessageResponse struct {
	Id        string
	Data      string
	LockUntil time.Time
}

func CreateMessageFromRequest(request CreateMessageRequest) *Message {
	message := &Message{
		Id:     ulid.Make().String(),
		Data:   request.Data,
		Status: MessageStatusInQueue,
	}
	return message
}
