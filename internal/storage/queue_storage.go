package storage

import (
	"BlitzQueue/internal/model"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/pelletier/go-toml/v2"
	"log"
	"os"
	"time"
)

var ErrQueueNotExist = errors.New("queue not exist")

type QueueStorage interface {
	CreateQueue(name string, queueType model.QueueType) (*model.Queue, error)
	GetQueueByName(name string) (*model.Queue, error)
}

type queueStorage struct {
}

func NewQueueStorage() QueueStorage {
	return &queueStorage{}
}

func (s *queueStorage) CreateQueue(name string, queueType model.QueueType) (*model.Queue, error) {
	id, _ := uuid.NewV7()
	q := &model.Queue{
		Id:        id.String(),
		Name:      name,
		Type:      queueType,
		CreatedAt: time.Now(),
	}

	tomlData, err := toml.Marshal(q)

	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("queues/%s.toml", name)
	err = os.WriteFile(path, tomlData, os.ModePerm)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return q, nil
}

func (s *queueStorage) GetQueueByName(name string) (*model.Queue, error) {
	path := fmt.Sprintf("queues/%s.toml", name)

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

func createFolderIfNotExists() {
	err := os.Mkdir("queues", os.ModePerm)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
}

func init() {
	uuid.EnableRandPool()
	createFolderIfNotExists()
}
