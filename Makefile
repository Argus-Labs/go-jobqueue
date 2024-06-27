###########
# Testing #
###########
.PHONY: test
test:
	@go test ./... -v


###########
# Linting #
###########
.PHONY: lint-install lint lint-fix

golangci_version=v1.59.1

# Install golangci-lint if not present
lint-install:
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "--> golangci-lint is not installed. Installing..."; \
		@go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(golangci_version); \
	fi

# Run golangci-lint on the project
lint:
	@$(MAKE) lint-install
	@echo "--> Running golangci-lint"
	@golangci-lint run  --timeout=10m


# Run golangci-lint on the project and fix any issues
lint-fix:
	@$(MAKE) lint-install
	@echo "--> Running golangci-lint"
	@golangci-lint run  --timeout=10m --fix

