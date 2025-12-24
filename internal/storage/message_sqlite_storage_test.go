package storage

import (
	"BlitzQueue/internal/model"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// Helper functions for testing

func setupTestQueue(name string, queueType model.QueueType, useUniqueMessage bool) *model.Queue {
	return &model.Queue{
		Id:                 ulid.Make().String(),
		Name:               name,
		Type:               queueType,
		CreatedAt:          time.Now(),
		UseUniqueMessage:   useUniqueMessage,
		MessageLockTimeout: 1, // Default 1 minute lock timeout for tests
	}
}

func setupTestStorage(t *testing.T, queue *model.Queue) (MessageStorage, func()) {
	storage, err := NewMessageSqliteStorage(queue)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	cleanup := func() {
		// Limpa o banco de dados de teste
		dbPath := fmt.Sprintf("%s/%s.db", MessagesPath, queue.Name)
		os.Remove(dbPath)
	}

	return storage, cleanup
}

func createTestMessage(data string, queueId *string) *model.Message {
	return &model.Message{
		Id:      ulid.Make().String(),
		QueueId: queueId,
		Data:    data,
		Status:  model.MessageStatusInQueue,
	}
}

func createTestMessageWithPriority(data string, queueId *string, priority int) *model.Message {
	msg := createTestMessage(data, queueId)
	msg.Priority = &priority
	return msg
}

func createTestMessageWithSubQueue(data string, queueId *string, subQueue string) *model.Message {
	msg := createTestMessage(data, queueId)
	msg.SubQueue = subQueue
	return msg
}

// Tests

func TestNewMessageSqliteStorage(t *testing.T) {
	queue := setupTestQueue("test-new-storage", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	if storage == nil {
		t.Fatal("expected storage to be created, got nil")
	}

	if storage.GetType() != "sqlite" {
		t.Errorf("expected type 'sqlite', got '%s'", storage.GetType())
	}

	// Verifica que o arquivo do banco foi criado
	dbPath := fmt.Sprintf("%s/%s.db", MessagesPath, queue.Name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("expected database file to be created at %s", dbPath)
	}
}

func TestGetType(t *testing.T) {
	queue := setupTestQueue("test-get-type", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	if storage.GetType() != "sqlite" {
		t.Errorf("expected GetType to return 'sqlite', got '%s'", storage.GetType())
	}
}

func TestPushMessages_SingleMessage(t *testing.T) {
	queue := setupTestQueue("test-push-single", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test content", &queue.Id)

	err := storage.PushMessages(msg)
	if err != nil {
		t.Fatalf("expected no error pushing message, got %v", err)
	}

	// Verifica que a mensagem foi salva
	messages, err := storage.PeekMessages()
	if err != nil {
		t.Fatalf("expected no error peeking messages, got %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	if messages[0].Data != "test content" {
		t.Errorf("expected data 'test content', got '%s'", messages[0].Data)
	}
}

func TestPushMessages_MultipleMessages(t *testing.T) {
	queue := setupTestQueue("test-push-multiple", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	messages := []*model.Message{
		createTestMessage("content 1", &queue.Id),
		createTestMessage("content 2", &queue.Id),
		createTestMessage("content 3", &queue.Id),
	}

	err := storage.PushMessages(messages...)
	if err != nil {
		t.Fatalf("expected no error pushing messages, got %v", err)
	}

	// Verifica que todas as mensagens foram salvas
	retrievedMessages, err := storage.PeekMessages()
	if err != nil {
		t.Fatalf("expected no error peeking messages, got %v", err)
	}

	if len(retrievedMessages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(retrievedMessages))
	}
}

func TestPushMessages_EmptyList(t *testing.T) {
	queue := setupTestQueue("test-push-empty", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	err := storage.PushMessages()
	if err != nil {
		t.Fatalf("expected no error pushing empty list, got %v", err)
	}

	messages, _ := storage.PeekMessages()
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestPushMessages_WithUniqueMessages(t *testing.T) {
	queue := setupTestQueue("test-push-unique", model.QueueTypeStandard, true)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Cria mensagens com o mesmo hash
	msg1 := createTestMessage("duplicate content", &queue.Id)
	msg1.Hash = "samehash123"

	msg2 := createTestMessage("duplicate content", &queue.Id)
	msg2.Hash = "samehash123"

	msg3 := createTestMessage("unique content", &queue.Id)
	msg3.Hash = "uniquehash456"

	// Primeira inserção
	err := storage.PushMessages(msg1, msg3)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Tenta inserir duplicata
	err = storage.PushMessages(msg2)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Deve ter apenas 2 mensagens (msg1 e msg3)
	messages, _ := storage.PeekMessages()
	if len(messages) != 2 {
		t.Errorf("expected 2 unique messages, got %d", len(messages))
	}
}

func TestPeekMessages_EmptyQueue(t *testing.T) {
	queue := setupTestQueue("test-peek-empty", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	messages, err := storage.PeekMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestPeekMessages_DoesNotChangeStatus(t *testing.T) {
	queue := setupTestQueue("test-peek-status", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Peek primeira vez
	messages1, _ := storage.PeekMessages()
	if len(messages1) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages1))
	}

	// Peek segunda vez - deve retornar a mesma mensagem
	messages2, _ := storage.PeekMessages()
	if len(messages2) != 1 {
		t.Fatalf("expected 1 message on second peek, got %d", len(messages2))
	}

	if messages1[0].Id != messages2[0].Id {
		t.Error("expected same message on both peeks")
	}
}

func TestConsumeMessages_Success(t *testing.T) {
	queue := setupTestQueue("test-consume", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test consume", &queue.Id)
	storage.PushMessages(msg)

	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("expected no error consuming, got %v", err)
	}

	if len(consumed) != 1 {
		t.Fatalf("expected 1 consumed message, got %d", len(consumed))
	}

	if consumed[0].Data != "test consume" {
		t.Errorf("expected data 'test consume', got '%s'", consumed[0].Data)
	}

	// Verifica que lock_until foi definido (aproximadamente 1 minuto no futuro)
	expectedLockUntil := time.Now().Add(1 * time.Minute)
	timeDiff := consumed[0].LockUntil.Sub(expectedLockUntil).Abs()
	if timeDiff > 5*time.Second {
		t.Errorf("lock_until is not approximately 1 minute in the future")
	}
}

func TestConsumeMessages_WithCustomTime(t *testing.T) {
	queue := setupTestQueue("test-consume-custom", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	consumed, err := storage.ConsumeMessagesWithCustomTime(5)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(consumed) != 1 {
		t.Fatalf("expected 1 message, got %d", len(consumed))
	}

	// Verifica que lock_until foi definido (aproximadamente 5 minutos no futuro)
	expectedLockUntil := time.Now().Add(5 * time.Minute)
	timeDiff := consumed[0].LockUntil.Sub(expectedLockUntil).Abs()
	if timeDiff > 5*time.Second {
		t.Errorf("lock_until is not approximately 5 minutes in the future")
	}
}

func TestConsumeMessages_EmptyQueue(t *testing.T) {
	queue := setupTestQueue("test-consume-empty", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(consumed) != 0 {
		t.Errorf("expected 0 messages, got %d", len(consumed))
	}
}

func TestConsumeMessages_LockedMessages(t *testing.T) {
	queue := setupTestQueue("test-consume-locked", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Consome a mensagem (fica locked)
	consumed1, _ := storage.ConsumeMessages()
	if len(consumed1) != 1 {
		t.Fatalf("expected 1 message, got %d", len(consumed1))
	}

	// Tenta consumir novamente - não deve retornar nada pois está locked
	consumed2, _ := storage.ConsumeMessages()
	if len(consumed2) != 0 {
		t.Errorf("expected 0 messages (locked), got %d", len(consumed2))
	}
}

func TestConsumeMessages_ExpiredLocks(t *testing.T) {
	queue := setupTestQueue("test-consume-expired", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Consome com tempo customizado de 0 segundos (expira imediatamente)
	consumed1, _ := storage.ConsumeMessagesWithCustomTime(0)
	if len(consumed1) != 1 {
		t.Fatalf("expected 1 message, got %d", len(consumed1))
	}

	// Aguarda para garantir que o lock expirou
	time.Sleep(100 * time.Millisecond)

	// Deve conseguir consumir novamente pois o lock expirou
	consumed2, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(consumed2) != 1 {
		t.Errorf("expected 1 message (expired lock), got %d", len(consumed2))
	}
}

func TestConsumeMessages_PriorityQueue(t *testing.T) {
	queue := setupTestQueue("test-consume-priority", model.QueueTypePriority, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Adiciona mensagens com diferentes prioridades
	msg1 := createTestMessageWithPriority("low priority", &queue.Id, 1)
	msg2 := createTestMessageWithPriority("high priority", &queue.Id, 10)
	msg3 := createTestMessageWithPriority("medium priority", &queue.Id, 5)

	storage.PushMessages(msg1, msg2, msg3)

	// Consome mensagens - deve retornar em ordem de prioridade (maior primeiro)
	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(consumed) < 1 {
		t.Fatalf("expected at least 1 message, got %d", len(consumed))
	}

	// A primeira mensagem deve ser a de maior prioridade
	if consumed[0].Data != "high priority" {
		t.Errorf("expected first message to be 'high priority', got '%s'", consumed[0].Data)
	}
}

func TestConsumeMessages_FifoQueue(t *testing.T) {
	queue := setupTestQueue("test-consume-fifo", model.QueueTypeFifo, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Adiciona mensagens em diferentes sub-filas
	msg1 := createTestMessageWithSubQueue("msg1 in subA", &queue.Id, "subA")
	msg2 := createTestMessageWithSubQueue("msg2 in subA", &queue.Id, "subA")
	msg3 := createTestMessageWithSubQueue("msg1 in subB", &queue.Id, "subB")

	storage.PushMessages(msg1, msg2, msg3)

	// Consome mensagens - deve retornar uma por sub-fila
	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Deve ter 2 mensagens (uma de cada sub-fila)
	if len(consumed) != 2 {
		t.Errorf("expected 2 messages (one per sub-queue), got %d", len(consumed))
	}

	// Verifica que são as primeiras de cada sub-fila
	subQueues := make(map[string]bool)
	for _, msg := range consumed {
		subQueues[msg.Id] = true
	}

	// msg1 e msg3 devem estar presentes
	foundSubA := false
	foundSubB := false
	for _, msg := range consumed {
		if msg.Data == "msg1 in subA" {
			foundSubA = true
		}
		if msg.Data == "msg1 in subB" {
			foundSubB = true
		}
	}

	if !foundSubA || !foundSubB {
		t.Error("expected one message from each sub-queue")
	}
}

func TestConfirmMessagesByIds_Success(t *testing.T) {
	queue := setupTestQueue("test-confirm", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Consome a mensagem
	consumed, _ := storage.ConsumeMessages()
	if len(consumed) != 1 {
		t.Fatalf("expected 1 consumed message, got %d", len(consumed))
	}

	// Confirma a mensagem
	err := storage.ConfirmMessagesByIds([]string{consumed[0].Id})
	if err != nil {
		t.Fatalf("expected no error confirming, got %v", err.Error())
	}

	// Verifica que a mensagem foi marcada como processada
	sqliteStorage := storage.(*messageSqliteStorage)
	messages, _ := sqliteStorage.GetMessagesByIds([]string{consumed[0].Id})
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	if messages[0].Status != model.MessageStatusProcessed {
		t.Errorf("expected status Processed, got %d", messages[0].Status)
	}
}

func TestConfirmMessagesByIds_AlreadyProcessed(t *testing.T) {
	queue := setupTestQueue("test-confirm-processed", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)
	consumed, _ := storage.ConsumeMessages()

	// Confirma primeira vez
	storage.ConfirmMessagesByIds([]string{consumed[0].Id})

	// Tenta confirmar novamente - deve dar erro
	err := storage.ConfirmMessagesByIds([]string{consumed[0].Id})
	if err == nil {
		t.Fatal("expected error when confirming already processed message")
	}
	// Não verifica o tipo específico pois pode variar entre implementações
}

func TestConfirmMessagesByIds_NotInProcessing(t *testing.T) {
	queue := setupTestQueue("test-confirm-not-processing", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Tenta confirmar sem consumir (ainda está InQueue) - deve dar erro
	err := storage.ConfirmMessagesByIds([]string{msg.Id})
	if err == nil {
		t.Fatal("expected error when confirming message not in processing")
	}
	// Não verifica o tipo específico pois pode variar entre implementações
}

func TestConfirmMessagesByIds_MessageNotFound(t *testing.T) {
	queue := setupTestQueue("test-confirm-not-found", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Tenta confirmar mensagem inexistente - deve dar erro
	err := storage.ConfirmMessagesByIds([]string{"non-existent-id"})
	if err == nil {
		t.Fatal("expected error when confirming non-existent message")
	}
	// Não verifica o tipo específico pois pode variar entre implementações
}

func TestGetMessagesByIds_Success(t *testing.T) {
	queue := setupTestQueue("test-get-by-ids", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg1 := createTestMessage("msg1", &queue.Id)
	msg2 := createTestMessage("msg2", &queue.Id)
	storage.PushMessages(msg1, msg2)

	sqliteStorage := storage.(*messageSqliteStorage)
	messages, err := sqliteStorage.GetMessagesByIds([]string{msg1.Id, msg2.Id})
	if err != nil {
		t.Fatalf("expected no error, got %v", err.Error())
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
}

func TestGetMessagesByIds_NonExistent(t *testing.T) {
	queue := setupTestQueue("test-get-nonexistent", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	sqliteStorage := storage.(*messageSqliteStorage)
	messages, err := sqliteStorage.GetMessagesByIds([]string{"non-existent"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err.Error())
	}

	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestGetMoreTimeByIds_Success(t *testing.T) {
	queue := setupTestQueue("test-get-more-time", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)
	consumed, _ := storage.ConsumeMessages()

	// Estende o tempo de lock em 10 minutos
	err := storage.GetMoreTimeByIds(10, []string{consumed[0].Id})
	if err != nil {
		t.Fatalf("expected no error, got %v", err.Error())
	}

	// Verifica que o lock foi atualizado
	sqliteStorage := storage.(*messageSqliteStorage)
	messages, _ := sqliteStorage.GetMessagesByIds([]string{consumed[0].Id})
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	expectedLockUntil := time.Now().Add(10 * time.Minute)
	timeDiff := messages[0].LockUntil.Sub(expectedLockUntil).Abs()
	if timeDiff > 5*time.Second {
		t.Errorf("lock_until was not updated correctly")
	}
}

func TestGetMoreTimeByIds_NotInProcessing(t *testing.T) {
	queue := setupTestQueue("test-more-time-not-processing", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)

	// Tenta estender tempo sem consumir - deve dar erro
	err := storage.GetMoreTimeByIds(5, []string{msg.Id})
	if err == nil {
		t.Fatal("expected error when extending time for non-processing message")
	}
	// Não verifica o tipo específico pois pode variar entre implementações
}

func TestGetMoreTimeByIds_MessageNotFound(t *testing.T) {
	queue := setupTestQueue("test-more-time-not-found", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Tenta estender tempo de mensagem inexistente - deve dar erro
	err := storage.GetMoreTimeByIds(5, []string{"non-existent"})
	if err == nil {
		t.Fatal("expected error when extending time for non-existent message")
	}
	// Não verifica o tipo específico pois pode variar entre implementações
}

func TestCleanConsumedMessages_Success(t *testing.T) {
	queue := setupTestQueue("test-clean", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Adiciona e processa uma mensagem
	msg := createTestMessage("test", &queue.Id)
	storage.PushMessages(msg)
	consumed, _ := storage.ConsumeMessages()
	storage.ConfirmMessagesByIds([]string{consumed[0].Id})

	// Adiciona uma mensagem não processada
	msg2 := createTestMessage("not processed", &queue.Id)
	storage.PushMessages(msg2)

	// Limpa mensagens processadas
	err := storage.CleanConsumedMessages()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verifica que apenas a mensagem não processada permanece
	allMessages, _ := storage.PeekMessages()
	if len(allMessages) != 1 {
		t.Errorf("expected 1 message remaining, got %d", len(allMessages))
	}

	if allMessages[0].Data != "not processed" {
		t.Errorf("expected remaining message to be 'not processed', got '%s'", allMessages[0].Data)
	}
}

func TestCleanConsumedMessages_OldMessages(t *testing.T) {
	queue := setupTestQueue("test-clean-old", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Cria uma mensagem com ID antigo (usando CreateOldMessageId)
	oldMsg := &model.Message{
		Id:      model.CreateOldMessageId(),
		QueueId: &queue.Id,
		Data:    "old message",
		Status:  model.MessageStatusInQueue,
	}
	storage.PushMessages(oldMsg)

	// Adiciona mensagem nova
	newMsg := createTestMessage("new message", &queue.Id)
	storage.PushMessages(newMsg)

	// Limpa
	storage.CleanConsumedMessages()

	// Verifica que apenas a mensagem nova permanece
	messages, _ := storage.PeekMessages()
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}

	if messages[0].Data != "new message" {
		t.Errorf("expected 'new message', got '%s'", messages[0].Data)
	}
}

func TestFullWorkflow_StandardQueue(t *testing.T) {
	queue := setupTestQueue("test-workflow-standard", model.QueueTypeStandard, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// 1. Push mensagens
	msg1 := createTestMessage("workflow msg 1", &queue.Id)
	msg2 := createTestMessage("workflow msg 2", &queue.Id)
	err := storage.PushMessages(msg1, msg2)
	if err != nil {
		t.Fatalf("push failed: %v", err)
	}

	// 2. Peek mensagens (sem alterar status)
	peeked, err := storage.PeekMessages()
	if err != nil || len(peeked) != 2 {
		t.Fatalf("peek failed or wrong count: %v, count: %d", err, len(peeked))
	}

	// 3. Consume mensagens
	consumed, err := storage.ConsumeMessages()
	if err != nil || len(consumed) != 2 {
		t.Fatalf("consume failed or wrong count: %v, count: %d", err, len(consumed))
	}

	// 4. Estende tempo de uma mensagem
	moreTimeErr := storage.GetMoreTimeByIds(5, []string{consumed[0].Id})
	if moreTimeErr != nil {
		t.Fatalf("get more time failed: %v", moreTimeErr)
	}

	// 5. Confirma as mensagens
	confirmErr := storage.ConfirmMessagesByIds([]string{consumed[0].Id, consumed[1].Id})
	if confirmErr != nil {
		t.Fatalf("confirm failed: %v", confirmErr)
	}

	// 6. Limpa mensagens processadas
	err = storage.CleanConsumedMessages()
	if err != nil {
		t.Fatalf("clean failed: %v", err)
	}

	// 7. Verifica que a fila está vazia
	finalPeek, _ := storage.PeekMessages()
	if len(finalPeek) != 0 {
		t.Errorf("expected empty queue, got %d messages", len(finalPeek))
	}
}

func TestFullWorkflow_PriorityQueue(t *testing.T) {
	queue := setupTestQueue("test-workflow-priority", model.QueueTypePriority, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Push mensagens com diferentes prioridades
	lowPriority := createTestMessageWithPriority("low", &queue.Id, 1)
	highPriority := createTestMessageWithPriority("high", &queue.Id, 10)

	storage.PushMessages(lowPriority, highPriority)

	// Consume - deve retornar em ordem de prioridade
	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if len(consumed) < 1 {
		t.Fatalf("expected at least 1 message, got %d", len(consumed))
	}

	// Primeira mensagem deve ser a de maior prioridade
	if consumed[0].Data != "high" {
		t.Errorf("expected 'high' priority first, got '%s'", consumed[0].Data)
	}

	// Confirma
	ids := make([]string, len(consumed))
	for i, msg := range consumed {
		ids[i] = msg.Id
	}
	storage.ConfirmMessagesByIds(ids)

	// Limpa
	storage.CleanConsumedMessages()
}

func TestFullWorkflow_FifoQueue(t *testing.T) {
	queue := setupTestQueue("test-workflow-fifo", model.QueueTypeFifo, false)
	storage, cleanup := setupTestStorage(t, queue)
	defer cleanup()

	// Push mensagens em sub-filas
	msgA1 := createTestMessageWithSubQueue("A1", &queue.Id, "subA")
	msgA2 := createTestMessageWithSubQueue("A2", &queue.Id, "subA")
	msgB1 := createTestMessageWithSubQueue("B1", &queue.Id, "subB")

	storage.PushMessages(msgA1, msgA2, msgB1)

	// Consume - deve retornar uma de cada sub-fila
	consumed, err := storage.ConsumeMessages()
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if len(consumed) != 2 {
		t.Errorf("expected 2 messages (one per sub-queue), got %d", len(consumed))
	}

	// Confirma as consumidas
	ids := make([]string, len(consumed))
	for i, msg := range consumed {
		ids[i] = msg.Id
	}
	storage.ConfirmMessagesByIds(ids)

	// Deve ainda ter uma mensagem pendente (A2)
	remaining, _ := storage.PeekMessages()
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining message, got %d", len(remaining))
	}
}
