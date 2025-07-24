package writer

import (
	"BlitzQueue/internal/logger"
	"BlitzQueue/internal/model"
	"BlitzQueue/internal/storage"
	"context"
	"github.com/gammazero/deque"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const defaultBatchSize = 100000

type WriterService interface {
	PushMessages(queue *model.Queue, messages ...*model.Message) error
	GetStopChannel() chan bool
}

type writerService struct {
	ctx context.Context
	sync.Mutex
	queueStorage storage.QueueStorage
	buffer       map[string]*deque.Deque[*model.Message]
	messageChan  map[string]chan *model.Message
	log          *slog.Logger
	doneChan     chan bool
}

func NewWriterService(ctx context.Context, queueStorage storage.QueueStorage) WriterService {
	writer := &writerService{
		ctx:          ctx,
		queueStorage: queueStorage,
		buffer:       make(map[string]*deque.Deque[*model.Message]),
		log:          logger.GetLogger(),
		doneChan:     make(chan bool, 1),
		messageChan:  make(map[string]chan *model.Message),
	}

	writer.gracefulShutdown()

	return writer
}

func (s *writerService) PushMessages(queue *model.Queue, messages ...*model.Message) error {
	log := s.log.With(slog.String("queueName", queue.Name))
	s.Lock()
	queueChannel, ok := s.messageChan[queue.Name]
	if !ok {
		queueChan := make(chan *model.Message)
		s.messageChan[queue.Name] = queueChan
		queueChannel = queueChan
		go s.flushMessagesPeriodically(queue)
		log.Info("created new channel for queue")
	}
	s.Unlock()

	for _, message := range messages {
		queueChannel <- message
	}

	return nil
}

func (s *writerService) flushMessagesPeriodically(queue *model.Queue) {
	log := s.log.With(slog.String("queueName", queue.Name))
	timeInterval := time.Duration(500 + rand.Intn(100))

	flushTicker := time.NewTicker(timeInterval * time.Millisecond)
	cleanTicker := time.NewTicker(5 * time.Second)
	emptyCounter := 0

	log.Info("Listening for messages on channel")
	messagesToInsert := make([]*model.Message, 0, defaultBatchSize)

	for {
		select {
		case message, ok := <-s.messageChan[queue.Name]:
			if !ok {
				log.Info("channel closed")
				s.flushMessages(queue, messagesToInsert)
				return
			}
			messagesToInsert = append(messagesToInsert, message)
			if len(messagesToInsert) >= defaultBatchSize {
				s.flushMessages(queue, messagesToInsert)
				messagesToInsert = make([]*model.Message, 0, defaultBatchSize)
				flushTicker.Reset(timeInterval * time.Millisecond)
			}
		case <-flushTicker.C:
			if len(messagesToInsert) == 0 {
				emptyCounter++
				if emptyCounter >= 1000 {
					log.Info("Empty channel for 100 times, closing channel")
					close(s.messageChan[queue.Name])
					delete(s.messageChan, queue.Name)
					return
				}
				continue
			}
			s.flushMessages(queue, messagesToInsert)
			messagesToInsert = make([]*model.Message, 0, defaultBatchSize)
		case <-cleanTicker.C:
			log.Info("cleaning up")
			s.CleanProcessedMessages(queue)
		}
	}

}

func (s *writerService) flushMessages(queue *model.Queue, messages []*model.Message) {
	if len(messages) == 0 {
		return
	}

	log := s.log.With(slog.String("queueName", queue.Name))
	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		log.Error("error getting message storage", slog.String("error", err.Error()))
		panic(err)
	}

	log.Info("Flushing queue", slog.Int("amount", len(messages)))

	err = queueStorage.PushMessages(messages...)
	if err != nil {
		log.Error("error pushing messages to storage", slog.String("error", err.Error()))
	}
}

func (s *writerService) CleanProcessedMessages(queue *model.Queue) {
	log := s.log.With(slog.String("queueName", queue.Name))

	queueStorage, err := s.queueStorage.GetMessageStorage(queue)

	if err != nil {
		panic(err)
	}

	err = queueStorage.CleanConsumedMessages()

	if err != nil {
		log.Error("error cleaning processed messages", slog.String("error", err.Error()))
	}
}

func (s *writerService) gracefulShutdown() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				s.log.Info("shutting down writer service")
				s.Lock()
				for key, queueChannel := range s.messageChan {
					s.log.Info("closing channel for queue", slog.String("queueName", key))
					close(queueChannel)
				}
				s.doneChan <- true
				s.Unlock()
				return
			}
		}
	}()

}

func (s *writerService) GetStopChannel() chan bool {
	return s.doneChan
}
