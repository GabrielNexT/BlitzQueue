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

type Message struct {
	Id        string
	QueueId   *string
	Data      string
	Status    MessageStatus `gorm:"index:status_lock_until_idx,priority:1"`
	Hash      *string       // Only for unique queue config
	Priority  *int          // Only for the priority queue type
	NextRun   *time.Time    // Enable only when queue backoff is enable
	LockUntil *time.Time    `gorm:"index:status_lock_until_idx,priority:2"`
}

type CreateMessageRequest struct {
	Data     string
	Priority int
}

func CreateMessageFromRequest(request CreateMessageRequest) *Message {
	message := &Message{
		Id:     ulid.Make().String(),
		Data:   request.Data,
		Status: MessageStatusInQueue,
	}
	return message
}

type ConsumeMessageResponse struct {
	Id        string
	Data      string
	LockUntil time.Time
}

type ConfirmMessagesRequest struct {
	MessageIds []string
}
