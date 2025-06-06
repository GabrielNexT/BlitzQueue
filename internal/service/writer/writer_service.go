package writer

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const DefaultBufferSize = 5000

var emptyBufferErr = errors.New("queue buffer is empty")

type WriterService interface {
	PushMessages(queue *model.Queue, messages ...*model.Message) error
}

type writerService struct {
	sync.Mutex
	queueStorage storage.QueueStorage
	buffer       map[string][]*model.Message
	bufferLock   map[string]*sync.Mutex
	log          *slog.Logger
}

func NewWriterService(queueStorage storage.QueueStorage) WriterService {
	return &writerService{
		queueStorage: queueStorage,
		buffer:       make(map[string][]*model.Message),
		bufferLock:   make(map[string]*sync.Mutex),
		log:          logger.GetLogger(),
	}
}

// TODO: Quando o software reeber um SIGTERM, precisamos salvar todas as mensagens do buffer antes de encerrar
func (s *writerService) PushMessages(queue *model.Queue, messages ...*model.Message) error {
	s.Lock()
	queueMutex, ok := s.bufferLock[queue.Id]
	if !ok {
		s.buffer[queue.Id] = make([]*model.Message, 0, DefaultBufferSize)
		queueMutex = &sync.Mutex{}
		s.bufferLock[queue.Id] = queueMutex
		go s.flushMessagesPeriodically(queue)
	}
	s.Unlock()

	queueMutex.Lock()
	s.buffer[queue.Id] = append(s.buffer[queue.Id], messages...)
	queueMutex.Unlock()

	return nil
}

func (s *writerService) flushMessagesPeriodically(queue *model.Queue) {
	timeInterval := time.Duration(200 + rand.Intn(200))

	ticker := time.NewTicker(timeInterval * time.Millisecond)

	emptyCount := 0
	for {
		select {
		case <-ticker.C:
			err := s.flushMessages(queue)
			if errors.Is(err, emptyBufferErr) {
				emptyCount++
				if emptyCount >= 100 {
					ticker.Stop()
					s.deleteQueueBuffer(queue)
					s.log.Info("stopped flushing messages for queue", slog.String("queueId", queue.Id))
				}
				continue
			}
			if err != nil {
				s.log.Error("error flushing messages for queue", slog.String("queueId", queue.Id), slog.String("error", err.Error()))
			}
		}
	}
}

func (s *writerService) flushMessages(queue *model.Queue) error {
	log := s.log.With(slog.String("queueId", queue.Id))

	queueLock, ok := s.bufferLock[queue.Id]
	if !ok {
		log.Error("failed to get lock for queue", slog.String("queueId", queue.Id))
		return errors.New("failed to get lock for queue")
	}
	queueLock.Lock()
	defer queueLock.Unlock()
	messages := s.buffer[queue.Id]
	if len(messages) == 0 {
		return emptyBufferErr
	}

	log.Info("Flushing messages", slog.Int("messagesCount", len(messages)))
	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		log.Error("error getting message storage")
		return err
	}

	err = queueStorage.PushMessages(messages...)

	if err != nil {
		log.Error("error pushing messages to storage", slog.String("error", err.Error()))
		return err
	}

	s.buffer[queue.Id] = make([]*model.Message, 0, DefaultBufferSize)

	return nil
}

func (s *writerService) deleteQueueBuffer(queue *model.Queue) {
	s.Lock()
	defer s.Unlock()
	queueMutex, ok := s.bufferLock[queue.Id]
	if !ok {
		return
	}
	queueMutex.Lock()
	defer queueMutex.Unlock()
	delete(s.buffer, queue.Id)
	delete(s.bufferLock, queue.Id)
}
