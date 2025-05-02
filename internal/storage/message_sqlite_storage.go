package storage

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"fmt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"log/slog"
	"os"
	"time"
)

const MessagesPath = "bq_data/messages"

func init() {
	err := os.MkdirAll(MessagesPath, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
}

type messageSqliteStorage struct {
	db    *gorm.DB
	queue *model.Queue
}

func NewMessageSqliteStorage(queue *model.Queue) (MessageStorage, error) {
	dbPath := fmt.Sprintf("%s/%s.db", MessagesPath, queue.Name)
	log := logger.GetLogger()

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})

	if err != nil {
		panic(err)
	}

	err = db.AutoMigrate(&model.Message{})

	if err != nil {
		log.Error("failed to migrate messages in sqlite", slog.String("error", err.Error()), slog.String("queue", queue.Name))
	}

	return &messageSqliteStorage{
		db:    db,
		queue: queue,
	}, nil
}

func (s *messageSqliteStorage) PushMessages(messages ...*model.Message) error {

	result := s.db.Create(messages)

	return result.Error
}

func (s *messageSqliteStorage) PeekMessages() ([]*model.Message, error) {
	var messages []*model.Message
	res := s.db.Order("id asc").Limit(20).Find(&messages)

	if res.Error != nil {
		return nil, res.Error
	}

	for _, message := range messages {
		message.QueueId = &s.queue.Id
	}

	return messages, nil
}

func (s *messageSqliteStorage) ConsumeMessages() ([]*model.ConsumeMessageResponse, error) {
	var messages []*model.Message
	var consumeMessages []*model.ConsumeMessageResponse

	err := s.db.Transaction(func(tx *gorm.DB) error {
		res := s.db.Where("status = ? or (status = ? and lock_until <= ?)", model.MessageStatusInQueue, model.MessageStatusProcessing, time.Now()).
			Order("id asc").
			Limit(20).
			Find(&messages)

		if res.Error != nil {
			return res.Error
		}

		for _, message := range messages {
			message.Status = model.MessageStatusProcessing
		}

		ids := make([]string, len(messages))
		for idx, message := range messages {
			ids[idx] = message.Id
		}

		lockUntil := time.Now().Add(time.Minute)

		res = s.db.Model(&model.Message{}).Where("id in ?", ids).Updates(map[string]interface{}{"status": model.MessageStatusProcessing, "lock_until": lockUntil})

		if res.Error != nil {
			return res.Error
		}

		for _, message := range messages {
			consumeMessages = append(consumeMessages, &model.ConsumeMessageResponse{
				Id:        message.Id,
				Data:      message.Data,
				LockUntil: lockUntil,
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return consumeMessages, nil
}

func (s *messageSqliteStorage) GetType() string {
	return "sqlite"
}
