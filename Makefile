
run-queue:
	go run cmd/blitzqueue.go


test:
	go test ./...


run-gen-test:
	go run cmd/generate_test_data.go

gen-storage-mock:
	mockgen -source=internal/storage/queue_storage.go -package=mocks -destination=internal/mocks/storage_mock.go QueueStorage

gen-message-storage-mock:
	mockgen -source=internal/storage/types.go -package=mocks -destination=internal/mocks/message_storage.go MessageStorage

