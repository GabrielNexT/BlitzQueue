package storage

import (
	"BlitzQueue/internal/model"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/pelletier/go-toml/v2"
	"log"
	"os"
	"time"
)

const QueuesPath = "bq_data/queues"

func init() {
	err := os.MkdirAll(QueuesPath, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
}

var ErrQueueNotExist = errors.New("queue not exist")

type QueueStorage interface {
	CreateQueue(name string, queueType model.QueueType) (*model.Queue, error)
	GetQueueByName(name string) (*model.Queue, error)
	GetMessageStorage(queue *model.Queue) (MessageStorage, error)
}

type queueStorage struct {
}

func NewQueueStorage() QueueStorage {
	return &queueStorage{}
}

func (s *queueStorage) CreateQueue(name string, queueType model.QueueType) (*model.Queue, error) {
	q := &model.Queue{
		Id:        ulid.Make().String(),
		Name:      name,
		Type:      queueType,
		CreatedAt: time.Now(),
	}

	tomlData, err := toml.Marshal(q)

	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("%s/%s.toml", QueuesPath, name)
	err = os.WriteFile(path, tomlData, os.ModePerm)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return q, nil
}

func (s *queueStorage) GetQueueByName(name string) (*model.Queue, error) {
	path := fmt.Sprintf("%s/%s.toml", QueuesPath, name)

	data, err := os.ReadFile(path)

	if os.IsNotExist(err) {
		return nil, ErrQueueNotExist
	}

	if err != nil {
		return nil, err
	}

	q := &model.Queue{}

	err = toml.Unmarshal(data, q)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return q, nil
}

func (s *queueStorage) GetMessageStorage(queue *model.Queue) (MessageStorage, error) {
	messageStorage, err := NewMessageSqliteStorage(queue)

	if err != nil {
		return nil, err
	}

	return messageStorage, nil
}
