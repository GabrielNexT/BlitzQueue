package model

import (
	"BlitzQueue/internal/util"
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
	Id        string `gorm:"index:queue_id_idx,priority:1"`
	QueueId   *string
	Data      string
	Status    MessageStatus `gorm:"index:status_lock_until_idx,priority:1;index:queue_status_priority_idx,priority:1"`
	Hash      string        `gorm:"index"`                                                // Only for unique queue config
	Priority  *int          `gorm:"index:queue_status_priority_idx,sort:desc,priority:2"` // Only for the priority queue type
	LockUntil *time.Time    `gorm:"index:status_lock_until_idx,priority:2"`
	SubQueue  string        `gorm:"index:queue_id_idx,priority:2"`
}

type CreateMessageRequest struct {
	Data             string
	Priority         int
	DeduplicationKey string
	SubQueue         string
}

func CreateMessageFromRequest(request CreateMessageRequest, queue *Queue) *Message {
	message := &Message{
		Id:     ulid.Make().String(),
		Data:   request.Data,
		Status: MessageStatusInQueue,
	}

	if queue.UseUniqueMessage {
		if request.DeduplicationKey == "" {
			request.DeduplicationKey = request.Data
		}
		message.Hash = util.HashString(&request.DeduplicationKey)
	}

	if queue.Type == QueueTypePriority {
		message.Priority = &request.Priority
	}

	if queue.Type == QueueTypeFifo {
		message.SubQueue = request.SubQueue
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

type ExtendMessageTimeRequest struct {
	MessageIds []string
}

func RemoveDuplicatesByHash(messages []*Message) []*Message {
	hashMap := make(map[string]bool)
	res := make([]*Message, 0)
	for _, message := range messages {
		if message.Hash != "" {
			if _, ok := hashMap[message.Hash]; !ok {
				hashMap[message.Hash] = true
				res = append(res, message)
			}
		}
	}
	return res
}

func GetHashList(messages []*Message) []string {
	hashList := make([]string, len(messages))
	for i, message := range messages {
		if message.Hash != "" {
			hashList[i] = message.Hash
		}
	}
	return hashList
}

func CreateMapByHash(messages []*Message) map[string]*Message {
	hashMap := make(map[string]*Message)
	for _, message := range messages {
		if message.Hash != "" {
			hashMap[message.Hash] = message
		}
	}
	return hashMap
}

// TimeBetweenIds calculates the time duration between two Message IDs based on their ULID timestamps.
func TimeBetweenIds(l, r *Message) time.Duration {
	if l == nil || r == nil {
		return 0
	}

	id1, _ := ulid.Parse(l.Id)
	id2, _ := ulid.Parse(r.Id)

	diff := id2.Time() - id1.Time()

	return time.Duration(diff) * time.Millisecond
}

// FilterUniqueMessages filters a list of messages to include only unique ones based on hash and time window constraints.
// It considers existing messages from the same queue to enforce uniqueness according to the queue's configuration.
func FilterUniqueMessages(messages []*Message, existingMessages []*Message, queue *Queue) []*Message {
	existingMessagesByHash := CreateMapByHash(existingMessages)
	filteredMessages := make([]*Message, 0, len(messages))

	timeWindow := queue.GetUniqueMessageTimeWindow()
	for _, newMessage := range messages {
		existingMessage, exists := existingMessagesByHash[newMessage.Hash]
		if !exists {
			filteredMessages = append(filteredMessages, newMessage)
			continue
		}

		timeDiff := TimeBetweenIds(existingMessage, newMessage)

		if timeDiff >= timeWindow {
			filteredMessages = append(filteredMessages, newMessage)
		}
	}

	return filteredMessages
}
