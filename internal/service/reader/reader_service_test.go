package reader

import (
	"BlitzQueue/internal/mocks"
	"BlitzQueue/internal/model"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/mock/gomock"
)

// Helper functions

func setupTestQueue(name string) *model.Queue {
	return &model.Queue{
		Id:   ulid.Make().String(),
		Name: name,
		Type: model.QueueTypeStandard,
	}
}

func createTestConsumeResponse(id string, data string, lockMinutes int) *model.ConsumeMessageResponse {
	return &model.ConsumeMessageResponse{
		Id:        id,
		Data:      data,
		LockUntil: time.Now().Add(time.Duration(lockMinutes) * time.Minute),
	}
}

// Tests

func TestNewReaderService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	service := NewReaderService(ctx, mockQueueStorage)

	if service == nil {
		t.Fatal("expected service to be created, got nil")
	}

	// Verifica tipo concreto
	readerSvc, ok := service.(*readerService)
	if !ok {
		t.Fatal("expected service to be of type *readerService")
	}

	if readerSvc.ctx != ctx {
		t.Error("expected context to be set correctly")
	}

	if readerSvc.queueStorage != mockQueueStorage {
		t.Error("expected queueStorage to be set correctly")
	}

	if readerSvc.buffer == nil {
		t.Error("expected buffer map to be initialized")
	}

	if readerSvc.bufferLock == nil {
		t.Error("expected bufferLock map to be initialized")
	}

	if readerSvc.doneChan == nil {
		t.Error("expected doneChan to be initialized")
	}

	if cap(readerSvc.doneChan) != 1 {
		t.Errorf("expected doneChan buffer size 1, got %d", cap(readerSvc.doneChan))
	}
}

func TestGetStopChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	service := NewReaderService(ctx, mockQueueStorage)
	stopChan := service.GetStopChannel()

	if stopChan == nil {
		t.Error("expected stop channel to be non-nil")
	}

	// Verifica que algo foi enviado para o canal
	select {
	case val := <-stopChan:
		if !val {
			t.Error("expected true value in stop channel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected value in stop channel, got timeout")
	}
}

func TestConsumeMessages_FirstCall_InitializesBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	// Mock: GetMessageStorage retorna storage mockado
	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Mock: ConsumeMessages retorna mensagens
	messages := []*model.ConsumeMessageResponse{
		createTestConsumeResponse("msg-1", "data 1", 2),
	}
	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return(messages, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	// Primeira chamada inicializa buffer
	result, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 message, got %d", len(result))
	}

	// Verifica que buffer foi criado
	readerSvc := service.(*readerService)
	readerSvc.Lock()
	_, bufferExists := readerSvc.buffer[queue.Name]
	_, lockExists := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	if !bufferExists {
		t.Error("expected buffer to be created for queue")
	}

	if !lockExists {
		t.Error("expected buffer lock to be created for queue")
	}
}

func TestConsumeMessages_EmptyBuffer_FallbackToStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Primeira chamada retorna vazio
	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{}, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	result, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 messages, got %d", len(result))
	}
}

func TestConsumeMessages_GetStorageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)

	queue := setupTestQueue("test-queue")

	// Mock retorna erro
	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(nil, errors.New("storage not found")).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	_, err := service.ConsumeMessages(queue)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err.Error() != "storage not found" {
		t.Errorf("expected 'storage not found' error, got '%s'", err.Error())
	}
}

func TestConsumeMessages_StorageConsumeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// ConsumeMessages retorna erro
	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return(nil, errors.New("consume failed")).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	_, err := service.ConsumeMessages(queue)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err.Error() != "consume failed" {
		t.Errorf("expected 'consume failed' error, got '%s'", err.Error())
	}
}

func TestConsumeMessages_MultipleQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage1 := mocks.NewMockMessageStorage(ctrl)
	mockMessageStorage2 := mocks.NewMockMessageStorage(ctrl)

	queue1 := setupTestQueue("queue-1")
	queue2 := setupTestQueue("queue-2")

	// Mocks para queue1
	mockQueueStorage.EXPECT().
		GetMessageStorage(queue1).
		Return(mockMessageStorage1, nil).
		AnyTimes()

	messages1 := []*model.ConsumeMessageResponse{
		createTestConsumeResponse("msg-q1", "data q1", 2),
	}
	mockMessageStorage1.EXPECT().
		ConsumeMessages().
		Return(messages1, nil).
		Times(1)

	// Mocks para queue2
	mockQueueStorage.EXPECT().
		GetMessageStorage(queue2).
		Return(mockMessageStorage2, nil).
		AnyTimes()

	messages2 := []*model.ConsumeMessageResponse{
		createTestConsumeResponse("msg-q2", "data q2", 2),
	}
	mockMessageStorage2.EXPECT().
		ConsumeMessages().
		Return(messages2, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	// Consome da queue1
	result1, err := service.ConsumeMessages(queue1)
	if err != nil {
		t.Fatalf("expected no error for queue1, got %v", err)
	}
	if len(result1) != 1 || result1[0].Id != "msg-q1" {
		t.Error("expected queue1 message")
	}

	// Consome da queue2
	result2, err := service.ConsumeMessages(queue2)
	if err != nil {
		t.Fatalf("expected no error for queue2, got %v", err)
	}
	if len(result2) != 1 || result2[0].Id != "msg-q2" {
		t.Error("expected queue2 message")
	}

	// Verifica que ambos os buffers foram criados
	readerSvc := service.(*readerService)
	readerSvc.Lock()
	defer readerSvc.Unlock()

	if _, exists := readerSvc.buffer[queue1.Name]; !exists {
		t.Error("expected buffer for queue1")
	}

	if _, exists := readerSvc.buffer[queue2.Name]; !exists {
		t.Error("expected buffer for queue2")
	}
}

func TestConsumeMessages_WithMessagesInBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Mock para preencher buffer via ConsumeMessagesWithCustomTime (chamado pelo fillBuffer)
	messages := []*model.ConsumeMessageResponse{
		createTestConsumeResponse("msg-1", "data 1", 3),
		createTestConsumeResponse("msg-2", "data 2", 3),
		createTestConsumeResponse("msg-3", "data 3", 3),
	}
	mockMessageStorage.EXPECT().
		ConsumeMessagesWithCustomTime(2).
		Return(messages, nil).
		AnyTimes()

	// Primeira chamada: ConsumeMessages do storage (buffer ainda vazio)
	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{}, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	// Primeira chamada inicializa buffer mas retorna vazio
	result1, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result1) != 0 {
		t.Errorf("expected 0 messages on first call, got %d", len(result1))
	}

	// Adiciona mensagens manualmente ao buffer (simula fillBuffer)
	readerSvc := service.(*readerService)
	readerSvc.Lock()
	buffer := readerSvc.buffer[queue.Name]
	bufferLock := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	bufferLock.Lock()
	buffer.PushBack(createTestConsumeResponse("msg-buf-1", "buffered 1", 3))
	buffer.PushBack(createTestConsumeResponse("msg-buf-2", "buffered 2", 3))
	bufferLock.Unlock()

	// Segunda chamada deve retornar do buffer
	result2, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error on second call, got %v", err)
	}

	if len(result2) != 2 {
		t.Errorf("expected 2 messages from buffer, got %d", len(result2))
	}

	if result2[0].Id != "msg-buf-1" {
		t.Errorf("expected first message to be 'msg-buf-1', got '%s'", result2[0].Id)
	}
}

func TestBufferManagement_ExpiredMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{}, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	// Inicializa buffer
	service.ConsumeMessages(queue)

	// Adiciona mensagens com locks expirados e válidos
	readerSvc := service.(*readerService)
	readerSvc.Lock()
	buffer := readerSvc.buffer[queue.Name]
	bufferLock := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	bufferLock.Lock()
	// Mensagem expirada (lock <= 1 minuto)
	buffer.PushBack(&model.ConsumeMessageResponse{
		Id:        "expired-1",
		Data:      "expired",
		LockUntil: time.Now().Add(30 * time.Second),
	})
	// Mensagem válida (lock > 1 minuto)
	buffer.PushBack(createTestConsumeResponse("valid-1", "valid", 3))
	bufferLock.Unlock()

	// Consome - deve pular a expirada e retornar apenas a válida
	result, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 valid message, got %d", len(result))
	}

	if result[0].Id != "valid-1" {
		t.Errorf("expected 'valid-1', got '%s'", result[0].Id)
	}
}

func TestBufferManagement_MaxCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{}, nil).
		Times(1)

	service := NewReaderService(ctx, mockQueueStorage)

	// Inicializa buffer
	service.ConsumeMessages(queue)

	// Adiciona 25 mensagens ao buffer
	readerSvc := service.(*readerService)
	readerSvc.Lock()
	buffer := readerSvc.buffer[queue.Name]
	bufferLock := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	bufferLock.Lock()
	for i := 0; i < 25; i++ {
		buffer.PushBack(createTestConsumeResponse(
			"msg-"+string(rune(i)),
			"data",
			3,
		))
	}
	bufferLock.Unlock()

	// getMessagesFromBuffer deve retornar no máximo 20
	result, err := service.ConsumeMessages(queue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result) != 20 {
		t.Errorf("expected max 20 messages, got %d", len(result))
	}

	// Buffer deve ter 5 mensagens restantes
	bufferLock.Lock()
	remainingCount := buffer.Len()
	bufferLock.Unlock()

	if remainingCount != 5 {
		t.Errorf("expected 5 messages remaining in buffer, got %d", remainingCount)
	}
}

func TestIntegration_ConcurrentConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	// Múltiplas chamadas podem acontecer
	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{
			createTestConsumeResponse("msg-1", "data", 2),
		}, nil).
		AnyTimes()

	service := NewReaderService(ctx, mockQueueStorage)

	// Consome concorrentemente
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			_, err := service.ConsumeMessages(queue)
			if err != nil {
				t.Errorf("unexpected error in goroutine: %v", err)
			}
			done <- true
		}()
	}

	// Aguarda todas as goroutines
	for i := 0; i < 5; i++ {
		select {
		case <-done:
			// OK
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for concurrent consume")
		}
	}
}

func TestConsumeMessages_ReuseBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockQueueStorage := mocks.NewMockQueueStorage(ctrl)
	mockMessageStorage := mocks.NewMockMessageStorage(ctrl)

	queue := setupTestQueue("test-queue")

	mockQueueStorage.EXPECT().
		GetMessageStorage(queue).
		Return(mockMessageStorage, nil).
		AnyTimes()

	mockMessageStorage.EXPECT().
		ConsumeMessages().
		Return([]*model.ConsumeMessageResponse{}, nil).
		AnyTimes()

	service := NewReaderService(ctx, mockQueueStorage)

	// Primeira chamada
	service.ConsumeMessages(queue)

	readerSvc := service.(*readerService)
	readerSvc.Lock()
	firstBuffer := readerSvc.buffer[queue.Name]
	firstLock := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	// Segunda chamada
	service.ConsumeMessages(queue)

	readerSvc.Lock()
	secondBuffer := readerSvc.buffer[queue.Name]
	secondLock := readerSvc.bufferLock[queue.Name]
	readerSvc.Unlock()

	// Deve reutilizar o mesmo buffer e lock
	if firstBuffer != secondBuffer {
		t.Error("expected buffer to be reused")
	}

	if firstLock != secondLock {
		t.Error("expected buffer lock to be reused")
	}
}
