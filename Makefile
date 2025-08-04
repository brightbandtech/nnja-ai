.PHONY: build-docs serve-docs

serve-docs:
	@echo "Serving docs at http://localhost:8000"
	uv run mkdocs serve

build-docs:
	@echo "Building docs"
	uv run mkdocs build