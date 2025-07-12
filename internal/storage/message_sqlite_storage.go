package storage

import (
	"BlitzQueue/internal/model"
	"embed"
	"fmt"
	"github.com/doug-martin/goqu/v9"
	"github.com/pressly/goose/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"time"
)

//go:embed migrations/sqlite/*.sql
var embedMigrations embed.FS

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

	gdb, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})

	if err != nil {
		panic(err)
	}

	db, err := gdb.DB()

	if err != nil {
		panic(err)
	}

	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("sqlite"); err != nil {
		panic(err)
	}

	if err := goose.Up(db, "migrations/sqlite"); err != nil {
		panic(err)
	}

	return &messageSqliteStorage{
		db:    gdb,
		queue: queue,
	}, nil
}

func (s *messageSqliteStorage) PushMessages(messages ...*model.Message) error {

	return s.db.Transaction(func(tx *gorm.DB) error {
		if s.queue.UseUniqueMessage == true {
			messages = model.RemoveDuplicatesByHash(messages)
			hashes := model.GetHashList(messages)

			existingMessages, err := getPendingMessagesByHash(tx, hashes...)

			if err != nil {
				return err
			}

			messages = model.FilterUniqueMessages(messages, existingMessages, s.queue)
		}

		if len(messages) == 0 {
			return nil
		}

		sqlInsert := createSqlInsert(messages...)

		result := tx.Exec(sqlInsert)

		if result.Error != nil {
			fmt.Println(result.Error.Error())
		}

		return result.Error
	})

}

func (s *messageSqliteStorage) PeekMessages() ([]*model.Message, error) {
	messages, err := s.getNextMessages(s.db)
	if err != nil {
		return nil, err
	}

	for _, message := range messages {
		message.QueueId = &s.queue.Id
	}

	return messages, nil
}

func (s *messageSqliteStorage) consumeMessages(expirationMinutes int) ([]*model.ConsumeMessageResponse, error) {
	var consumeMessages []*model.ConsumeMessageResponse
	err := s.db.Transaction(func(tx *gorm.DB) error {
		messages, err := s.getNextMessages(tx)

		if err != nil {
			return err
		}

		for _, message := range messages {
			message.Status = model.MessageStatusProcessing
		}

		ids := make([]string, len(messages))
		for idx, message := range messages {
			ids[idx] = message.Id
		}

		lockUntil := time.Now().Add(time.Duration(expirationMinutes) * time.Minute)

		dbRes := tx.Model(&model.Message{}).Where("id in ?", ids).Updates(map[string]interface{}{"status": model.MessageStatusProcessing, "lock_until": lockUntil})

		if dbRes.Error != nil {
			return dbRes.Error
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

func (s *messageSqliteStorage) ConsumeMessages() ([]*model.ConsumeMessageResponse, error) {
	return s.consumeMessages(1)
}

func (s *messageSqliteStorage) ConsumeMessagesWithCustomTime(timeInMinutes int) ([]*model.ConsumeMessageResponse, error) {
	return s.consumeMessages(timeInMinutes)
}

func (s *messageSqliteStorage) ConfirmMessagesByIds(messageIds []string) *MessageStorageError {

	err := s.db.Transaction(func(tx *gorm.DB) error {
		messages, err := getMessagesByIds(tx, messageIds)

		if err != nil {
			return err
		}

		idDict := make(map[string]bool)

		for _, message := range messages {
			if message.Status == model.MessageStatusProcessed {
				return NewMessageStorageError(fmt.Sprintf("message with id %s already processed", message.Id), ErrMessageAlreadyProcessed)
			}

			if message.Status == model.MessageStatusInQueue {
				return NewMessageStorageError(fmt.Sprintf("message with id %s is still in queue and not being processed", message.Id), ErrMessageIsNotInProcessingState)
			}

			idDict[message.Id] = true
		}

		for _, messageId := range messageIds {
			if _, ok := idDict[messageId]; !ok {
				return NewMessageStorageError(fmt.Sprintf("message with id %s not found", messageId), ErrMessageDoesNotExist)
			}
		}

		res := updateStatusByMessagesIds(tx, messageIds, model.MessageStatusProcessed)

		if res.Error != nil {
			return NewMessageStorageError(res.Error.Error(), ErrInternalError)
		}

		return nil
	})

	if err != nil {
		return NewMessageStorageError(err.Error(), ErrInternalError)
	}

	return nil
}

func (s *messageSqliteStorage) GetMessagesByIds(messageIds []string) ([]*model.Message, *MessageStorageError) {
	return getMessagesByIds(s.db, messageIds)
}

func (s *messageSqliteStorage) GetMoreTimeByIds(amount int, messageIds []string) *MessageStorageError {

	err := s.db.Transaction(func(tx *gorm.DB) error {
		messages, err := getMessagesByIds(tx, messageIds)
		idsToUpdate := make([]string, 0, len(messageIds))

		if err != nil {
			return err
		}

		idDict := make(map[string]bool)

		for _, message := range messages {
			if message.Status == model.MessageStatusInQueue {
				return NewMessageStorageError(fmt.Sprintf("message with id %s is still in queue and not being processed", message.Id), ErrMessageIsNotInProcessingState)
			}
			idDict[message.Id] = true
			idsToUpdate = append(idsToUpdate, message.Id)
		}

		for _, messageId := range messageIds {
			if _, ok := idDict[messageId]; !ok {
				return NewMessageStorageError(fmt.Sprintf("message with id %s not found", messageId), ErrMessageDoesNotExist)
			}
		}

		lockUntil := time.Now().Add(time.Minute * time.Duration(amount))

		res := updateLockUntilByIdsTransaction(tx, idsToUpdate, lockUntil)

		if res.Error != nil {
			return NewMessageStorageError(res.Error.Error(), ErrInternalError)
		}

		return nil
	})

	if err != nil {
		return NewMessageStorageError(err.Error(), ErrInternalError)
	}

	return nil
}

func (s *messageSqliteStorage) GetType() string {
	return "sqlite"
}

func (s *messageSqliteStorage) getNextMessages(db *gorm.DB) ([]*model.Message, error) {
	var messages []*model.Message

	var res *gorm.DB

	switch s.queue.Type {
	case model.QueueTypeStandard:
		res = db.Where("status = ? or (status = ? and lock_until <= ?)", model.MessageStatusInQueue, model.MessageStatusProcessing, time.Now()).
			Order("id asc").
			Limit(20).
			Find(&messages)
	case model.QueueTypeFifo:
		res = db.
			Raw(`
				select *
				from messages m
						 join (select min(id) as id, sub_queue from messages where status != 2 group by sub_queue) am on m.id = am.id
				where status = ?
				   or (status = ? and lock_until <= ?)
				limit 20`, model.MessageStatusInQueue, model.MessageStatusProcessing, time.Now()).
			Scan(&messages)
	case model.QueueTypePriority:
		res = db.Where("status = ? or (status = ? and lock_until <= ?)", model.MessageStatusInQueue, model.MessageStatusProcessing, time.Now()).
			Order("priority desc").
			Limit(20).
			Find(&messages)

	default:
		panic("invalid queue type")
	}
	return messages, res.Error
}

func getMessagesByIds(db *gorm.DB, messageIds []string) ([]*model.Message, *MessageStorageError) {
	var messages []*model.Message
	res := db.Where("id in ?", messageIds).Find(&messages)

	if res.Error != nil {
		return nil, NewMessageStorageError(res.Error.Error(), ErrInternalError)
	}

	return messages, nil
}

func getPendingMessagesByHash(db *gorm.DB, hash ...string) ([]*model.Message, *MessageStorageError) {
	var messages []*model.Message
	res := db.Where("hash in ?", hash).Find(&messages)

	if res.Error != nil {
		return nil, NewMessageStorageError(res.Error.Error(), ErrInternalError)
	}

	return messages, nil
}

func updateLockUntilByIdsTransaction(db *gorm.DB, messageIds []string, lockUntil time.Time) *gorm.DB {
	return db.Model(&model.Message{}).Where("id in ?", messageIds).Update("lock_until", lockUntil)
}

func updateStatusByMessagesIds(db *gorm.DB, messageIds []string, status model.MessageStatus) *gorm.DB {
	return db.Model(&model.Message{}).Where("id in ?", messageIds).Update("status", status)
}

func createSqlInsert(messages ...*model.Message) string {

	var values [][]interface{}
	for _, message := range messages {
		values = append(values, []interface{}{
			message.Id,
			message.QueueId,
			message.Data,
			message.Status,
			message.Priority,
			message.Hash,
			message.SubQueue,
			message.LockUntil,
		})
	}

	ds := goqu.Insert("messages").
		Cols("id", "queue_id", "data", "status", "priority", "hash", "sub_queue", "lock_until").
		Vals(values...)

	insertSQL, _, _ := ds.ToSQL()
	return insertSQL
}
