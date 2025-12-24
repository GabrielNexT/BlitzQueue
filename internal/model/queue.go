package model

import "time"

type QueueType int8

const (
	QueueTypeStandard QueueType = iota
	QueueTypeFifo
	QueueTypePriority
	QueueTypeScheduled
)

const UniqueMessageTimeWindowDefaultValue = 10 * time.Minute

type Queue struct {
	Id                      string
	Name                    string
	Type                    QueueType
	CreatedAt               time.Time
	UseUniqueMessage        bool
	UniqueMessageTimeWindow int
	MessageLockTimeout      int
}

func (q *Queue) CanUseBuffer() bool {
	return q.Type == QueueTypeStandard
}

func (q *Queue) GetUniqueMessageTimeWindow() time.Duration {
	if q.UniqueMessageTimeWindow == 0 {
		return UniqueMessageTimeWindowDefaultValue
	}

	return time.Duration(q.UniqueMessageTimeWindow) * time.Minute
}

type CreateQueueRequest struct {
	Name             string
	Type             QueueType
	UseUniqueMessage bool
}
