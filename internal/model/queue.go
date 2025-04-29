package model

import "time"

type QueueType int8

const (
	QueueTypeStandard QueueType = iota
	QueueTypeFifo
	QueueTypePriority
	QueueTypeScheduled
)

type Queue struct {
	Id        string
	Name      string
	Type      QueueType
	CreatedAt time.Time
}

type CreateQueueRequest struct {
	Name string
	Type QueueType
}
