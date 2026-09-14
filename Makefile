.PHONY: test test-integration migrate-up run-server run-connector

test:
	go test ./...

test-integration:
	SMG_TEST_INTEGRATION=1 go test ./test/ -v

migrate-up:
	migrate -path db/migrations -database "$$SMG_DB_URL" up

run-server:
	go run ./cmd/smppgw

run-connector:
	go run ./cmd/smppgw -role connector -id 1
