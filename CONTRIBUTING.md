# Contributing

Thanks for your interest in LogScope. Small, focused changes are easiest to review.

## Build and test

Requirements: Python 3.12; Docker for integration tests.

```bash
python -m pip install -e ".[dev]"
```

Run the checks before opening a pull request:

```bash
python -m pytest -m "not integration" --cov=app --cov-fail-under=80
python -m ruff check app tests
python -m mypy app
python -m bandit -q -r app
```

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Keep each pull request to one logical change and add or update tests with it.
3. Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
4. Make sure CI passes and describe what changed and why in the pull request.

Report security issues as described in [SECURITY.md](SECURITY.md), not in public issues.
