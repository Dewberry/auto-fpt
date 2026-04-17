# Contributing

Thank you for your interest in contributing!

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
All commit messages **must** follow this format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | When to use | Changelog section |
|---|---|---|
| `feat` | A new feature | Added |
| `fix` | A bug fix | Fixed |
| `perf` | A performance improvement | Changed |
| `refactor` | Code restructuring, no behavior change | Changed |
| `revert` | Reverting a previous commit | Fixed |
| `docs` | Documentation only | _(skipped)_ |
| `test` | Adding or updating tests | _(skipped)_ |
| `chore` | Maintenance, dependencies, tooling | _(skipped)_ |
| `ci` | CI/CD changes | _(skipped)_ |

### Breaking Changes

Append `!` after the type, or add `BREAKING CHANGE:` in the footer:

```
feat!: drop support for Python 3.11
```

```
feat: new API

BREAKING CHANGE: `old_function` has been removed.
```

### Examples

```
feat(io): add support for GeoParquet output
fix: handle missing CRS in raster inputs
chore: bump ruff to v0.15.5
docs: add example notebook for elevation grid
```

## Submitting Changes

1. Fork the repository.
1. Create a feature branch.
1. Make your changes with tests.
1. Ensure all checks pass.
1. Submit a pull request.