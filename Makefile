###########
# Testing #
###########
.PHONY: test
test:
	@go test ./... -v -race -covermode=atomic -coverprofile=coverage.out


###########
# Linting #
###########
.PHONY: lint-install lint lint-fix

GOLANGCI_VERSION=v1.59.1

# Install golangci-lint if not present
lint-install:
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "--> golangci-lint is not installed. Installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@${GOLANGCI_VERSION}; \
	else \
		if ! golangci-lint --version | grep -q ${GOLANGCI_VERSION}; then \
			echo "--> golangci-lint version is incorrect. Updating..."; \
			go install github.com/golangci/golangci-lint/cmd/golangci-lint@${GOLANGCI_VERSION}; \
		fi \
	fi

# Run golangci-lint on the project
lint:
	@$(MAKE) lint-install
	@echo "--> Running golangci-lint"
	@golangci-lint run


# Run golangci-lint on the project and fix any issues
lint-fix:
	@$(MAKE) lint-install
	@echo "--> Running golangci-lint"
	@golangci-lint run --fix

##############################
# Run GitHub Actions Locally #
##############################
.PHONY: gha-preview
gha-preview:
	@echo "--> Running GitHub Actions locally"
	@act -P macos-latest=-self-hosted

