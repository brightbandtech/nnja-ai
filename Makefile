.PHONY: build-docs serve-docs

include .env
export GH_TOKEN

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
	@GH_TOKEN=${GH_TOKEN} uv run semantic-release -vvv --noop version --minor --no-changelog

patch-release:
	@echo "Creating patch release"
	@GH_TOKEN=${GH_TOKEN} uv run semantic-release -vvv --noop version --patch --no-changelog
