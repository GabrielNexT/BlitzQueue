package writer

import (
	"BlitzQueue/internal/mocks"
	"BlitzQueue/internal/model"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
)

func TestNewWriterService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	service := NewWriterService(ctx, mockQueueStorage)

	if service == nil {
		t.Fatal("expected service to be created, got nil")
	}

	writerSvc, ok := service.(*writerService)
	if !ok {
		t.Fatal("expected service to be of type *writerService")
	}

	if writerSvc.ctx != ctx {
		t.Error("expected context to be set correctly")
	}

	if writerSvc.queueStorage != mockQueueStorage {
		t.Error("expected queueStorage to be set correctly")
	}

	if writerSvc.messageChan == nil {
		t.Error("expected messageChan to be initialized")
	}

	if writerSvc.doneChan == nil {
		t.Error("expected doneChan to be initialized")
	}
}

func TestPushMessages_SingleMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}
	message := &model.Message{
		Id:   "msg-1",
		Data: "test content",
	}

	err := service.PushMessages(queue, message)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Aguarda um pouco para garantir que a goroutine processou
	time.Sleep(100 * time.Millisecond)

	// Verifica se o canal foi criado
	writerSvc := service.(*writerService)
	writerSvc.Lock()
	_, exists := writerSvc.messageChan[queue.Name]
	writerSvc.Unlock()

	if !exists {
		t.Error("expected channel to be created for queue")
	}
}

func TestPushMessages_MultipleMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Espera que PushMessages seja chamado com 3 mensagens
	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		MinTimes(1)

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	messages := []*model.Message{
		{Id: "msg-1", Data: "content 1"},
		{Id: "msg-2", Data: "content 2"},
		{Id: "msg-3", Data: "content 3"},
	}

	err := service.PushMessages(queue, messages...)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Aguarda flush
	time.Sleep(700 * time.Millisecond)
}

func TestPushMessages_BatchFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Espera que PushMessages seja chamado pelo menos uma vez com o batch cheio
	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		MinTimes(1)

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Envia mais mensagens do que o batch size para forçar flush
	messageBatch := make([]*model.Message, defaultBatchSize+10)
	for i := 0; i < defaultBatchSize+10; i++ {
		messageBatch[i] = &model.Message{
			Id:   string(rune(i)),
			Data: "test content",
		}
	}

	err := service.PushMessages(queue, messageBatch...)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Aguarda para garantir que o flush aconteceu
	time.Sleep(200 * time.Millisecond)
}

func TestFlushMessages_EmptyBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	// GetMessageStorage não deve ser chamado para batch vazio
	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Times(0)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}

	// Flush com batch vazio não deve fazer nada
	service.flushMessages(queue, []*model.Message{})
}

func TestFlushMessages_PushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		Times(1)

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(errors.New("storage error")).
		Times(1)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}
	messages := []*model.Message{
		{Id: "msg-1", Data: "content 1"},
	}

	// O erro é logado mas não é retornado, não deve travar
	service.flushMessages(queue, messages)
}

func TestFlushMessages_GetStorageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(nil, errors.New("storage not found")).
		Times(1)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}
	messages := []*model.Message{
		{Id: "msg-1", Data: "content 1"},
	}

	// Este teste deve entrar em panic devido ao erro no GetMessageStorage
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when getting message storage fails")
		}
	}()

	service.flushMessages(queue, messages)
}

func TestCleanProcessedMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		Times(1)

	mockMessageStorage.EXPECT().
		CleanConsumedMessages().
		Return(nil).
		Times(1)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}

	service.CleanProcessedMessages(queue)
}

func TestCleanProcessedMessages_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		Times(1)

	mockMessageStorage.EXPECT().
		CleanConsumedMessages().
		Return(errors.New("clean error")).
		Times(1)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}

	// O erro é logado mas não retornado
	service.CleanProcessedMessages(queue)
}

func TestCleanProcessedMessages_GetStorageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(nil, errors.New("storage error")).
		Times(1)

	service := NewWriterService(ctx, mockQueueStorage).(*writerService)
	queue := &model.Queue{Name: "test-queue"}

	// Este teste deve entrar em panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when getting message storage fails")
		}
	}()

	service.CleanProcessedMessages(queue)
}

func TestGracefulShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Envia uma mensagem para criar o canal
	err := service.PushMessages(queue, &model.Message{Id: "msg-1", Data: "test"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Cancela o contexto para iniciar shutdown
	cancel()

	// Aguarda pelo sinal de done
	select {
	case <-service.GetStopChannel():
		// Shutdown concluído
	case <-time.After(2 * time.Second):
		t.Fatal("expected shutdown to complete within 2 seconds")
	}
}

func TestPushMessages_MultipleQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue1 := &model.Queue{Name: "queue-1"}
	queue2 := &model.Queue{Name: "queue-2"}

	err := service.PushMessages(queue1, &model.Message{Id: "msg-1", Data: "queue1"})
	if err != nil {
		t.Fatalf("expected no error for queue1, got %v", err)
	}

	err = service.PushMessages(queue2, &model.Message{Id: "msg-2", Data: "queue2"})
	if err != nil {
		t.Fatalf("expected no error for queue2, got %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verifica se ambos os canais foram criados
	writerSvc := service.(*writerService)
	writerSvc.Lock()
	defer writerSvc.Unlock()

	if _, exists := writerSvc.messageChan[queue1.Name]; !exists {
		t.Error("expected channel to be created for queue1")
	}

	if _, exists := writerSvc.messageChan[queue2.Name]; !exists {
		t.Error("expected channel to be created for queue2")
	}
}

func TestGetStopChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	service := NewWriterService(ctx, mockQueueStorage)
	stopChan := service.GetStopChannel()

	if stopChan == nil {
		t.Error("expected stop channel to be non-nil")
	}

	// Verifica que o canal tem buffer de 1
	if cap(stopChan) != 1 {
		t.Errorf("expected stop channel to have buffer size 1, got %d", cap(stopChan))
	}
}

func TestPushMessages_ReuseChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any(), gomock.Any()).
		Return(nil).
		MinTimes(1)

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Primeira mensagem cria o canal
	err := service.PushMessages(queue, &model.Message{Id: "msg-1", Data: "first"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verifica que o canal foi criado
	writerSvc := service.(*writerService)
	writerSvc.Lock()
	firstChan := writerSvc.messageChan[queue.Name]
	writerSvc.Unlock()

	// Segunda mensagem reutiliza o canal
	err = service.PushMessages(queue, &model.Message{Id: "msg-2", Data: "second"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verifica que é o mesmo canal
	writerSvc.Lock()
	secondChan := writerSvc.messageChan[queue.Name]
	writerSvc.Unlock()

	if firstChan != secondChan {
		t.Error("expected channel to be reused, but got different channel")
	}

	time.Sleep(700 * time.Millisecond)
}

func TestPushMessages_ConcurrentAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Testa acesso concorrente
	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &model.Message{
					Id:   "msg-" + string(rune(routineNum)) + "-" + string(rune(j)),
					Data: "content",
				}
				err := service.PushMessages(queue, msg)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(1 * time.Second)
}

func TestFlushMessagesPeriodically_ChannelClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Envia mensagem para criar o canal
	err := service.PushMessages(queue, &model.Message{Id: "msg-1", Data: "test"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	writerSvc := service.(*writerService)
	writerSvc.Lock()
	queueChan, exists := writerSvc.messageChan[queue.Name]
	writerSvc.Unlock()

	if !exists {
		t.Fatal("expected channel to exist")
	}

	// Fecha o canal manualmente para simular o comportamento
	close(queueChan)

	// Aguarda para garantir que a goroutine processou o fechamento
	time.Sleep(200 * time.Millisecond)
}

func TestPushMessages_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	mockQueueStorage.EXPECT().
		GetMessageStorage(gomock.Any()).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		PushMessages(gomock.Any()).
		Return(nil).
		AnyTimes()

	service := NewWriterService(ctx, mockQueueStorage)
	queue := &model.Queue{Name: "test-queue"}

	// Testa que PushMessages sempre retorna nil
	err := service.PushMessages(queue, &model.Message{Id: "msg-1", Data: "test"})
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}
