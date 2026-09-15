BUILD_DIR := bin
UI_DIST := /var/www/smppgw

.PHONY: test test-integration migrate-up migrate-down run-server run-connector build build-ui install-ui deploy

test:
	go test ./...

test-integration:
	SMG_TEST_INTEGRATION=1 go test ./test/ -v

build:
	go build -o $(BUILD_DIR)/smppgw ./cmd/smppgw

build-ui:
	cd web && npm ci && npm run build

install-ui:
	rm -rf $(UI_DIST)
	cp -r web/dist $(UI_DIST)

migrate-up:
	@test -n "$$SMG_DB_URL" || (echo "SMG_DB_URL requerida"; exit 1)
	migrate -path db/migrations -database "$$SMG_DB_URL" up

migrate-down:
	@test -n "$$SMG_DB_URL" || (echo "SMG_DB_URL requerida"; exit 1)
	migrate -path db/migrations -database "$$SMG_DB_URL" down 1

deploy: build build-ui install-ui
	systemctl restart smppgw-server smppgw-connector@1 smppgw-connector@2

run-server:
	go run ./cmd/smppgw

run-connector:
	go run ./cmd/smppgw -role connector -id 1
