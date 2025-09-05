package storage

import (
	"BlitzQueue/internal/model"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/patrickmn/go-cache"
	"github.com/pelletier/go-toml/v2"
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
	CreateQueue(*model.Queue) (*model.Queue, error)
	GetQueueByName(name string) (*model.Queue, error)
	GetAllQueues() ([]*model.Queue, error)
	GetMessageStorage(queue *model.Queue) (MessageStorage, error)
}

type queueStorage struct {
	storageCache *cache.Cache
}

func NewQueueStorage() QueueStorage {
	return &queueStorage{
		storageCache: cache.New(5*time.Minute, 10*time.Minute),
	}
}

func (s *queueStorage) CreateQueue(q *model.Queue) (*model.Queue, error) {
	q.Id = ulid.Make().String()
	q.CreatedAt = time.Now()

	tomlData, err := toml.Marshal(q)

	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("%s/%s.toml", QueuesPath, q.Name)
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

func (s *queueStorage) GetAllQueues() ([]*model.Queue, error) {
	files, err := os.ReadDir(QueuesPath)
	if err != nil {
		return nil, err
	}

	var queues []*model.Queue
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		if len(fileName) < 5 || fileName[len(fileName)-5:] != ".toml" {
			continue
		}
		queueName := fileName[:len(fileName)-5]

		queue, err := s.GetQueueByName(queueName)
		if err != nil {
			log.Printf("Error loading queue %s: %v", queueName, err)
			continue
		}

		queues = append(queues, queue)
	}

	return queues, nil
}

func (s *queueStorage) GetMessageStorage(queue *model.Queue) (MessageStorage, error) {
	queueId := queue.Id

	if value, ok := s.storageCache.Get(queueId); ok {
		return value.(MessageStorage), nil
	}

	messageStorage, err := NewMessageSqliteStorage(queue)

	if err != nil {
		return nil, err
	}

	s.storageCache.Set(queueId, messageStorage, cache.DefaultExpiration)

	return messageStorage, nil
}
