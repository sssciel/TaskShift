.PHONY: help setup test test-unit test-integration clean coverage

# Default target
help:
	@echo "TaskShift Test Suite"
	@echo ""
	@echo "Usage:"
	@echo "  make setup          Setup test environment (create venv, install deps)"
	@echo "  make test           Run all tests"
	@echo "  make test-unit      Run unit tests only"
	@echo "  make test-integration Run integration tests only"
	@echo "  make coverage       Run tests with coverage report"
	@echo "  make clean          Clean test artifacts"
	@echo ""

# Setup test environment
setup:
	@echo "=== Setting up test environment ==="
	@if [ ! -d ".venv" ]; then \
		python3 -m venv .venv; \
		echo "Virtual environment created"; \
	else \
		echo "Virtual environment already exists"; \
	fi
	@. .venv/bin/activate && pip install --upgrade pip -q
	@. .venv/bin/activate && pip install -r requirements-test.txt -q
	@echo "=== Setup complete ==="

# Run all tests
test: test-unit

# Run unit tests
test-unit:
	@echo "=== Running Unit Tests ==="
	@if [ ! -d ".venv" ]; then \
		echo "Error: Virtual environment not found. Run 'make setup' first."; \
		exit 1; \
	fi
	@. .venv/bin/activate && pytest tests/unit -v --tb=short --maxfail=10

# Run integration tests
test-integration:
	@echo "=== Running Integration Tests ==="
	@if [ ! -d ".venv" ]; then \
		echo "Error: Virtual environment not found. Run 'make setup' first."; \
		exit 1; \
	fi
	@. .venv/bin/activate && pytest tests/integration -v --tb=short --maxfail=5

# Run tests with coverage
coverage:
	@echo "=== Running Tests with Coverage ==="
	@if [ ! -d ".venv" ]; then \
		echo "Error: Virtual environment not found. Run 'make setup' first."; \
		exit 1; \
	fi
	@. .venv/bin/activate && pytest tests/unit --cov=src --cov-report=term --cov-report=html --cov-report=xml
	@echo ""
	@echo "Coverage report generated:"
	@echo "  - Terminal: see above"
	@echo "  - HTML: htmlcov/index.html"
	@echo "  - XML: coverage.xml"

# Clean test artifacts
clean:
	@echo "Cleaning test artifacts..."
	@rm -rf .pytest_cache
	@rm -rf htmlcov
	@rm -f coverage.xml
	@rm -f .coverage
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete"
