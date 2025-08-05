# NOTE: We automatically load a .env file containing the "GH_TOKEN" environment variable
# for use with semantic-release. If this isn't present, then those commands will likely fail.
set dotenv-load

# List all available targets
default:
    @just --list

# Run the complete test suite
test:
    @echo "Running tests"
    uv run pytest

# Serve a local build of the project documentation at http://localhost:8000
serve-docs:
    @echo "Serving docs at http://localhost:8000"
    uv run mkdocs serve

# Build the project documentation
build-docs:
    @echo "Building docs"
    uv run mkdocs build

# Determine the next version number
next-version:
    @echo "Determining next version"
    uv run semantic-release version --print

# Create a minor release
minor-release:
    @echo "Creating minor release"
    uv run semantic-release -vvv --noop version --minor --no-changelog

# Create a patch release
patch-release:
    @echo "Creating patch release"
    uv run semantic-release -vvv --noop version --patch --no-changelog
