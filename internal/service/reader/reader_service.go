package reader

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

type ReaderService interface {
	ConsumeMessages(queue *model.Queue) ([]*model.ConsumeMessageResponse, error)
	GetStopChannel() chan bool
}

const DefaultReaderBufferSize = 100

type readerService struct {
	ctx context.Context
	sync.Mutex
	queueStorage storage.QueueStorage
	buffer       map[string]*deque.Deque[*model.ConsumeMessageResponse]
	bufferLock   map[string]*sync.Mutex
	log          *slog.Logger
	doneChan     chan bool
}

func NewReaderService(ctx context.Context, queueStorage storage.QueueStorage) ReaderService {
	reader := &readerService{
		ctx:          ctx,
		queueStorage: queueStorage,
		buffer:       make(map[string]*deque.Deque[*model.ConsumeMessageResponse]),
		bufferLock:   make(map[string]*sync.Mutex),
		log:          logger.GetLogger(),
		doneChan:     make(chan bool, 1),
	}

	return reader
}

func (r *readerService) ConsumeMessages(queue *model.Queue) ([]*model.ConsumeMessageResponse, error) {
	r.Lock()
	queueMutex, ok := r.bufferLock[queue.Name]
	if !ok {
		var d = deque.Deque[*model.ConsumeMessageResponse]{}
		d.SetBaseCap(DefaultReaderBufferSize)
		r.buffer[queue.Name] = &d
		queueMutex = &sync.Mutex{}
		r.bufferLock[queue.Name] = queueMutex
		r.PeriodicallyFillBuffer(queue)
	}
	r.Unlock()

	queueMutex.Lock()
	messages := r.getMessagesFromBuffer(queue)
	queueMutex.Unlock()

	if len(messages) != 0 {
		return messages, nil
	}

	queueStorage, err := r.queueStorage.GetMessageStorage(queue)

	if err != nil {
		return nil, err
	}

	messages, err = queueStorage.ConsumeMessages()

	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *readerService) PeriodicallyFillBuffer(queue *model.Queue) {
	ticker := time.NewTicker(time.Duration(500+rand.Intn(300)) * time.Millisecond)

	go func() {
		for {
			select {
			case <-ticker.C:
				err := r.fillBuffer(queue)
				if err != nil {
					r.log.Error("error filling buffer", slog.String("queueName", queue.Name), slog.String("error", err.Error()))
				}
			}
		}
	}()

}

func (r *readerService) fillBuffer(queue *model.Queue) error {
	log := r.log.With(slog.String("queueName", queue.Name))

	r.Lock()
	queueMutex, ok := r.bufferLock[queue.Name]
	if !ok {
		return errors.New("queue buffer not found")
	}
	r.Unlock()

	queueMutex.Lock()
	defer queueMutex.Unlock()
	buffer := r.buffer[queue.Name]

	queueStorage, err := r.queueStorage.GetMessageStorage(queue)

	if err != nil {
		log.Info("error getting message storage", slog.String("error", err.Error()))
		return err
	}

	for buffer.Len() > 0 {
		if buffer.Front().LockUntil.Sub(time.Now()) <= time.Minute {
			buffer.PopFront()
			continue
		}
		break
	}

	if buffer.Len() >= DefaultReaderBufferSize {
		log.Debug("buffer is full")
		return nil
	}

	messages, err := queueStorage.ConsumeMessagesWithCustomTime(2)

	if err != nil {
		log.Info("failed to consume message from storage", slog.String("error", err.Error()))
		return err
	}

	for _, message := range messages {
		buffer.PushBack(message)
	}

	log.Debug("messages added to buffer", slog.Int("messagesCount", len(messages)), slog.Int("bufferSize", buffer.Len()))

	return nil
}

func (r *readerService) getMessagesFromBuffer(queue *model.Queue) []*model.ConsumeMessageResponse {
	var messages []*model.ConsumeMessageResponse
	buffer := r.buffer[queue.Name]

	for {
		if buffer.Len() == 0 || len(messages) == 20 {
			break
		}
		message := buffer.PopFront()

		if message.LockUntil.Sub(time.Now()) <= time.Minute {
			continue
		}

		messages = append(messages, message)
	}

	return messages
}

func (r *readerService) GetStopChannel() chan bool {
	r.doneChan <- true
	return r.doneChan
}
