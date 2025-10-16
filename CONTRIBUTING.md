# Contributing to GlobalFS

Thank you for your interest in contributing to GlobalFS! This document provides guidelines and instructions for contributing.

## Project Status

**Note**: GlobalFS is currently in the **Design Phase**. We are actively working on Phase 1: Foundation (see README.md for details).

## Getting Started

### Prerequisites

- Go 1.23 or later
- Git
- ObjectFS (for testing integration)
- etcd v3.5+ (for coordinator development)
- AWS account with S3 access (for testing)

### Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/scttfrdmn/globalfs
   cd globalfs
   ```

2. **Install dependencies**:
   ```bash
   go mod download
   ```

3. **Run tests**:
   ```bash
   go test ./...
   ```

4. **Run with race detector**:
   ```bash
   go test -race ./...
   ```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test improvements

### 2. Make Changes

- Follow Go best practices and idioms
- Write tests for new functionality
- Update documentation as needed
- Ensure all tests pass with race detector
- Run `go fmt` and `golangci-lint` before committing

### 3. Commit Changes

Use clear, descriptive commit messages following conventional commits:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Build process or auxiliary tool changes

Example:
```
feat(coordinator): Add lease-based cache coherency protocol

Implement lease manager with read and write lease support.
Includes automatic lease expiration and invalidation on writes.

Closes #42
```

### 4. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub with:
- Clear description of changes
- Reference to related issues
- Test results
- Breaking changes (if any)

## Code Style

### Go Guidelines

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use `gofmt` for formatting
- Run `golangci-lint run` and fix all issues
- Keep functions small and focused
- Write clear comments for exported functions
- Use meaningful variable names

### Code Organization

```
globalfs/
├── cmd/              # Command-line tools
│   ├── globalfs/     # Main CLI
│   └── coordinator/  # Coordinator service
├── internal/         # Private application code
│   ├── coordinator/  # Coordinator implementation
│   ├── metadata/     # Metadata store
│   ├── lease/        # Lease management
│   ├── replication/  # Replication logic
│   └── policy/       # Policy engine
├── pkg/              # Public library code
│   ├── client/       # GlobalFS client
│   ├── config/       # Configuration
│   └── types/        # Common types
└── tests/            # Integration tests
```

### Testing

- Write unit tests for all new code
- Aim for >80% code coverage
- Use table-driven tests where appropriate
- Include integration tests for major features
- All tests must pass with `-race` flag

Example test structure:
```go
func TestFeature(t *testing.T) {
	tests := []struct {
		name    string
		input   input
		want    output
		wantErr bool
	}{
		{
			name:    "valid case",
			input:   validInput,
			want:    expectedOutput,
			wantErr: false,
		},
		// Add more test cases
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Feature(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Feature() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Feature() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

## Documentation

### Code Documentation

- Add godoc comments to all exported functions, types, and packages
- Include examples in godoc where helpful
- Document edge cases and important behavior

Example:
```go
// LeaseManager manages file access leases across multiple sites.
// It provides read and write leases with automatic expiration and
// cache invalidation on conflicting operations.
type LeaseManager struct {
	// ...
}

// GrantLease grants a lease for the specified path to the requesting site.
// It returns an error if the lease cannot be granted due to conflicts.
//
// Example:
//   lease, err := lm.GrantLease(ctx, "/data/file.txt", "site-1", LeaseWrite)
func (lm *LeaseManager) GrantLease(ctx context.Context, path, siteID string, leaseType LeaseType) (*Lease, error) {
	// ...
}
```

### Project Documentation

- Update README.md for user-facing changes
- Add detailed guides in `docs/` directory
- Include examples and use cases
- Document configuration options

## Issue Tracking

### Reporting Bugs

When reporting bugs, include:
- Clear description of the issue
- Steps to reproduce
- Expected vs. actual behavior
- Environment details (OS, Go version, etc.)
- Relevant logs or error messages

### Feature Requests

For feature requests, include:
- Use case and motivation
- Proposed solution or design
- Alternative approaches considered
- Impact on existing functionality

## Pull Request Process

1. **Ensure CI passes**: All tests and linters must pass
2. **Update documentation**: README, godoc, and guides as needed
3. **Add changelog entry**: For notable changes
4. **Request review**: Tag appropriate reviewers
5. **Address feedback**: Respond to review comments promptly
6. **Squash commits**: Before merge (if requested)

### PR Checklist

- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Changelog updated (for notable changes)
- [ ] Code formatted with `gofmt`
- [ ] `golangci-lint` passes
- [ ] Race detector passes (`go test -race ./...`)
- [ ] Commit messages follow conventional commits
- [ ] PR description is clear and complete

## Community Guidelines

### Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Assume good intentions

### Communication

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and ideas
- **Pull Requests**: Code review and technical discussion

## Development Phases

We're currently in **Phase 1: Foundation** (3-4 months). See README.md for details.

Priority areas for contribution:
1. Coordinator service implementation
2. Metadata store (etcd integration)
3. ObjectFS client library
4. Basic namespace mapping
5. Testing and documentation

## Getting Help

- Check existing [issues](https://github.com/scttfrdmn/globalfs/issues)
- Review [documentation](https://github.com/scttfrdmn/globalfs/tree/main/docs)
- Ask in [discussions](https://github.com/scttfrdmn/globalfs/discussions)

## License

By contributing to GlobalFS, you agree that your contributions will be licensed under the Apache 2.0 License.

---

Thank you for contributing to GlobalFS! Your help makes this project better for the HPC community.
