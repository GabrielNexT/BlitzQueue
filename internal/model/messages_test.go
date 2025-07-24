package model

import (
	"BlitzQueue/internal/util"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateMessageFromRequest(t *testing.T) {
	tests := []struct {
		name     string
		request  CreateMessageRequest
		queue    *Queue
		expected *Message
	}{
		{
			name:    "basicRequestHandling",
			request: CreateMessageRequest{Data: "testData"},
			queue:   &Queue{UseUniqueMessage: false, Type: QueueTypeStandard},
			expected: &Message{
				Data:   "testData",
				Status: MessageStatusInQueue,
			},
		},
		{
			name:    "deduplicationKeyHandling",
			request: CreateMessageRequest{Data: "testData", DeduplicationKey: "dedupKey"},
			queue:   &Queue{UseUniqueMessage: true, Type: QueueTypeStandard},
			expected: &Message{
				Data:   "testData",
				Hash:   util.HashString(ptr("dedupKey")),
				Status: MessageStatusInQueue,
			},
		},
		{
			name:    "uniqueMessageWithoutDeduplicationKey",
			request: CreateMessageRequest{Data: "testData"},
			queue:   &Queue{UseUniqueMessage: true, Type: QueueTypeStandard},
			expected: &Message{
				Data:   "testData",
				Hash:   util.HashString(ptr("testData")),
				Status: MessageStatusInQueue,
			},
		},
		{
			name:    "priorityQueueHandling",
			request: CreateMessageRequest{Data: "testData", Priority: 5},
			queue:   &Queue{Type: QueueTypePriority},
			expected: &Message{
				Data:     "testData",
				Status:   MessageStatusInQueue,
				Priority: ptr(5),
			},
		},
		{
			name:    "emptyData",
			request: CreateMessageRequest{Data: ""},
			queue:   &Queue{UseUniqueMessage: false, Type: QueueTypeStandard},
			expected: &Message{
				Data:   "",
				Status: MessageStatusInQueue,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateMessageFromRequest(tt.request, tt.queue)

			// Check dynamic fields
			assert.NotEmpty(t, result.Id, "Id should not be empty")
			tt.expected.Id = result.Id // Set expected Id to match dynamic field

			// Assert the rest of the fields
			assert.Equal(t, tt.expected, result)
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestFilterUniqueMessages(t *testing.T) {
	newID := ulid.Make().String()

	tests := []struct {
		name             string
		messages         []*Message
		existingMessages []*Message
		queue            *Queue
		expected         []*Message
	}{
		{
			name: "noDuplicates",
			messages: []*Message{
				{Id: "1", Hash: "hash1"},
				{Id: "2", Hash: "hash2"},
			},
			existingMessages: []*Message{},
			queue:            &Queue{UniqueMessageTimeWindow: 1},
			expected: []*Message{
				{Id: "1", Hash: "hash1"},
				{Id: "2", Hash: "hash2"},
			},
		},
		{
			name: "duplicatesWithinTimeWindow",
			messages: []*Message{
				{Id: "1", Hash: "hash1"},
			},
			existingMessages: []*Message{
				{Id: "2", Hash: "hash1"},
			},
			queue:    &Queue{UniqueMessageTimeWindow: 1},
			expected: []*Message{},
		},
		{
			name: "duplicatesOutsideTimeWindow",
			messages: []*Message{
				{Id: newID, Hash: "hash1"},
			},
			existingMessages: []*Message{
				{Id: "01JVJ9QH4690T4Q3NTCT8RFPTH", Hash: "hash1"},
			},
			queue: &Queue{UniqueMessageTimeWindow: 1},
			expected: []*Message{
				{Id: newID, Hash: "hash1"},
			},
		},
		{
			name:             "nilAndEmptyMessages",
			messages:         nil,
			existingMessages: nil,
			queue:            &Queue{UniqueMessageTimeWindow: 1},
			expected:         []*Message{},
		},
		{
			name: "duplicates inside time window",
			messages: []*Message{
				{Id: "01JVJ9QH4690T4Q3NTCT8RFPTH", Hash: "hash1"},
			},
			existingMessages: []*Message{
				{Id: "01JVJ9QH4690T4Q3NTCXTPPHCP", Hash: "hash1"},
			},
			queue:    &Queue{UniqueMessageTimeWindow: 1},
			expected: []*Message{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FilterUniqueMessages(tt.messages, tt.existingMessages, tt.queue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveDuplicatesByHash(t *testing.T) {
	tests := []struct {
		name     string
		input    []*Message
		expected []*Message
	}{
		{
			name:     "noDuplicates",
			input:    []*Message{{Hash: "hash1"}, {Hash: "hash2"}, {Hash: "hash3"}},
			expected: []*Message{{Hash: "hash1"}, {Hash: "hash2"}, {Hash: "hash3"}},
		},
		{
			name:     "withDuplicates",
			input:    []*Message{{Id: "123", Hash: "hash1"}, {Id: "1234", Hash: "hash2"}, {Id: "12345", Hash: "hash1"}},
			expected: []*Message{{Id: "123", Hash: "hash1"}, {Id: "1234", Hash: "hash2"}},
		},
		{
			name:     "emptyInput",
			input:    []*Message{},
			expected: []*Message{},
		},
		{
			name:     "empty hash values",
			input:    []*Message{{Hash: ""}, {Hash: "hash1"}, {Hash: ""}},
			expected: []*Message{{Hash: "hash1"}},
		},
		{
			name:     "allSameHashes",
			input:    []*Message{{Id: "1", Hash: "hash1"}, {Id: "2", Hash: "hash1"}, {Id: "3", Hash: "hash1"}},
			expected: []*Message{{Id: "1", Hash: "hash1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RemoveDuplicatesByHash(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateOldMessageId(t *testing.T) {
	oldMessageId := CreateOldMessageId()
	fmt.Println(oldMessageId)
	assert.NotEmpty(t, oldMessageId)
	assert.Less(t, oldMessageId, ulid.Make().String())
}
