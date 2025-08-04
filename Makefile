.PHONY: build-docs serve-docs

serve-docs:
	@echo "Serving docs at http://localhost:8000"
	uv run mkdocs serve

build-docs:
	@echo "Building docs"
	uv run mkdocs build

next-version:
	@echo "Determining next version"
	@uv run semantic-release version --print

minor-release:
	@echo "Creating minor release"
	@uv run semantic-release -vvv version --minor --no-changelog

patch-release:
	@echo "Creating patch release"
	@uv run semantic-release -vvv version --patch --no-changelog
