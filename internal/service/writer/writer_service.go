package writer

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"context"
	"errors"
	"github.com/gammazero/deque"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const DefaultBufferSize = 10000
const defaultBatchSize = 10000

var emptyBufferErr = errors.New("queue buffer is empty")

type WriterService interface {
	PushMessages(queue *model.Queue, messages ...*model.Message) error
	GetStopChannel() chan bool
}

type writerService struct {
	ctx context.Context
	sync.Mutex
	queueStorage storage.QueueStorage
	buffer       map[string]*deque.Deque[*model.Message]
	bufferLock   map[string]*sync.Mutex
	log          *slog.Logger
	doneChan     chan bool
}

func NewWriterService(ctx context.Context, queueStorage storage.QueueStorage) WriterService {
	writer := &writerService{
		ctx:          ctx,
		queueStorage: queueStorage,
		buffer:       make(map[string]*deque.Deque[*model.Message]),
		bufferLock:   make(map[string]*sync.Mutex),
		log:          logger.GetLogger(),
		doneChan:     make(chan bool, 1),
	}

	writer.gracefulShutdown()

	return writer
}

func (s *writerService) PushMessages(queue *model.Queue, messages ...*model.Message) error {
	s.Lock()
	queueMutex, ok := s.bufferLock[queue.Name]
	if !ok {
		dq := &deque.Deque[*model.Message]{}
		dq.SetBaseCap(100 * DefaultBufferSize)
		s.buffer[queue.Name] = dq
		queueMutex = &sync.Mutex{}
		s.bufferLock[queue.Name] = queueMutex
		go s.flushMessagesPeriodically(queue)
	}
	s.Unlock()

	queueMutex.Lock()
	for _, message := range messages {
		s.buffer[queue.Name].PushBack(message)
	}
	queueMutex.Unlock()

	return nil
}

func (s *writerService) flushMessagesPeriodically(queue *model.Queue) {
	timeInterval := time.Duration(250 + rand.Intn(200))

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
					s.log.Info("stopped flushing messages for queue", slog.String("queueName", queue.Name))
				}
				continue
			}
			if err != nil {
				s.log.Error("error flushing messages for queue", slog.String("queueName", queue.Name), slog.String("error", err.Error()))
			}
		}
	}
}

func (s *writerService) flushMessages(queue *model.Queue) error {
	log := s.log.With(slog.String("queueName", queue.Name))

	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		log.Error("error getting message storage", slog.String("error", err.Error()))
		return err
	}

	queueLock, ok := s.bufferLock[queue.Name]

	if !ok {
		return nil
	}

	queueLock.Lock()

	if s.buffer[queue.Name].Len() == 0 {
		queueLock.Unlock()
		return emptyBufferErr
	}

	log.Info("Flushing queue", slog.Int("bufferSize", s.buffer[queue.Name].Len()))

	messagesToInsert := make([]*model.Message, 0, defaultBatchSize)
	for i := 0; i < defaultBatchSize; i++ {
		if s.buffer[queue.Name].Len() == 0 {
			break
		}
		message := s.buffer[queue.Name].PopFront()
		messagesToInsert = append(messagesToInsert, message)
	}

	err = queueStorage.PushMessages(messagesToInsert...)

	if err != nil {
		queueLock.Unlock()
		log.Error("error pushing messages to storage", slog.String("error", err.Error()))
		return err
	}
	queueLock.Unlock()

	return nil
}

func (s *writerService) deleteQueueBuffer(queue *model.Queue) {
	s.log.Info("deleting queue buffer", slog.String("queueName", queue.Name))

	s.Lock()
	queueMutex, ok := s.bufferLock[queue.Name]
	if !ok {
		s.Unlock()
		return
	}
	s.Unlock()
	err := s.flushMessages(queue)
	if err != nil {
		s.log.Error("error flushing messages for queue", slog.String("queueName", queue.Name), slog.String("error", err.Error()))
	}

	s.Lock()
	queueMutex.Lock()
	delete(s.buffer, queue.Name)
	delete(s.bufferLock, queue.Name)
	queueMutex.Unlock()
	s.Unlock()
}

func (s *writerService) listAllQueues() []*model.Queue {
	res := make([]*model.Queue, 0, len(s.buffer))
	s.Lock()
	for queueName := range s.bufferLock {
		queue, err := s.queueStorage.GetQueueByName(queueName)
		if err != nil {
			s.log.Error("error getting queue from storage", slog.String("queueName", queueName), slog.String("error", err.Error()))
			continue
		}
		res = append(res, queue)
	}
	s.Unlock()
	return res
}

func (s *writerService) gracefulShutdown() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				s.log.Info("shutting down writer service")
				queues := s.listAllQueues()
				for _, queue := range queues {
					s.deleteQueueBuffer(queue)
				}
				s.doneChan <- true
				return
			}
		}
	}()

}

func (s *writerService) GetStopChannel() chan bool {
	return s.doneChan
}
