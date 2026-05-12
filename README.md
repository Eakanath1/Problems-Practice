# Practice Problems & Design Questions
A repository for practicing coding problems and system design questions.

## Contents
- **Coding Problems**: Algorithm and data structure exercises
- **Design Questions**: System design and architecture problems
- **Solutions**: Implementations and explanations
- **rate-limiter/**: Python rate limiting library implementation

## Python Environment Setup

This workspace uses **Poetry** for Python dependency management across all subdirectories.

### Quick Start
```bash
# Install dependencies
poetry install

# Activate the environment
poetry shell

# Run examples
python rate-limiter/token_bucket_async.py

# Or run without activating shell
poetry run python rate-limiter/token_bucket_async.py
```

### Adding Dependencies
```bash
# Add workspace-level dependencies
poetry add <package-name>

# Add development dependencies
poetry add --group dev pytest mypy pylint
```

### Structure
- `pyproject.toml` - Workspace-level Python dependencies
- `poetry.lock` - Locked dependency versions (committed to git)
- `rate-limiter/` - Rate limiting library

## Usage
Review problems, attempt solutions, and compare with implementations.

## Notes
Document progress, key learnings, and edge cases as you practice.