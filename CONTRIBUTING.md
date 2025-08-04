# Contributing to nnja-ai

First off, thanks for your interest in contributing!
We welcome bug reports, feature suggestions, and improvements—whether you want to raise an issue, propose an idea, or implement something yourself.

This project is focused on making the large parquet datasets of the [NNJA-AI](https://psl.noaa.gov/data/nnja_obs/) archive easy to search, navigate, prune, select, and load.

---

## Getting Started

- **Python 3.10+** is required.
- We recommend using [`uv`](https://github.com/astral-sh/uv) for dependency management and running tests.
- To set up your environment:
  ```sh
  uv sync --all-extras --dev
  ```

---

## Running Tests

- Run the full test suite with:
  ```sh
  uv run pytest
  ```
- Please ensure all tests pass before submitting a pull request.

---

## Code Style & Linting

- We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [mypy](https://mypy-lang.org/) for type checking.
- Pre-commit hooks are configured to run ruff (linter & formatter) and mypy (type checker).
  You can install and run them locally:
  ```sh
  pre-commit install
  pre-commit run --all-files
  ```
  (Or just rely on CI to catch issues.)
- Please fix any linter/type errors in code you touch, but you don’t need to fix unrelated linter errors elsewhere.

---

## Submitting Changes

- Fork the repository and create a new branch for your work.
- Update or add tests as appropriate for your change.
- **Update `CHANGELOG.md`** with a summary of your change.
- Open a pull request describing your changes and why they’re useful.
- **All PRs require review from either @hansmohrmann or @darothen before merging.**

---

## Issues & Suggestions

- Bug reports, feature requests, and questions are all welcome—please open an issue!
- If you have ideas for making it easier to work with NNJA-AI data, we’d love to hear them.

---

## Releases

- We provide a very minimal set of tools to help automate releases using `python-semantic-release`. 
- The [Makefile]() includes two rules, `{minor,patch}-release`, which document the correct commands to run.
  - You will need to source an appropriate GitHub access token and add it to a local `.env` file with the name **GH_TOKEN** in order to run these commands.
- In general, maintainers are responsible for creating releases.

---

## Project Maintainers

- @hansmohrmann
- @darothen

---

Thank you for helping make NNJA-AI more useful for the community!
