# Contributing

## Development

```bash
# Install LadybugDB
curl -fsSL https://install.ladybugdb.com | sh

# Build
make build

# Test
make test

# Race detector
make race

# Lint
make lint

# Coverage
make cover
```

## Project Layout

See [docs/project-structure.md](docs/project-structure.md) for the full package map.

## Pull Requests

- Fork the repo and create a branch from `main`
- Add tests for new functionality
- Run `make test` and `make lint` before submitting
- Keep PRs focused — one feature or fix per PR

## Code Style

- Standard Go formatting (`gofmt`)
- No external linter config beyond defaults
- Tests go in `*_test.go` alongside the code they test
- Use `MemoryStore` (not real LadybugDB) in unit tests

## Reporting Issues

Open an issue at [github.com/dreamware-nz/loveliness/issues](https://github.com/dreamware-nz/loveliness/issues).
