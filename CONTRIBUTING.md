# Contributing to Solana Private Channels

Thank you for your interest in contributing to Solana Private Channels! This document provides guidelines and instructions for contributing to the project. 

See [TECHNICAL_REQUIREMENTS.md](TECHNICAL_REQUIREMENTS.md) for detailed system requirements.

- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Security Vulnerabilities](#security-vulnerabilities)
- [Report a Bug](#non-security-issuesbugs)
- [Feature Requests](#feature-requests)
- [Code Style Guidelines](#code-style-guidelines)
- [Project Structure](#project-structure)
- [Getting Help](#getting-help)
- [License](#license)


## Getting Started

```bash
# Clone the repository
git clone https://github.com/solana-foundation/solana-private-channels.git
cd private_channel

# Install dependencies for all projects
make install

# Build all components
make build

# Run tests to verify setup
make all-test
```

## Development Workflow

### 1. Branch Strategy
- **Feature branches**: `feature/description` or `fix/description`
- **Main branch**: Always protected
- **No direct pushes**: Use PRs for all changes

### 2. Commit Messages
Use [Conventional Commits](https://www.conventionalcommits.org/) for automatic versioning:

```bash
# Features (minor version bump)
git commit -m "feat(lib): add Token2022 support"
git commit -m "feat(rpc): implement new signAndSend method"

# Bug fixes (patch version bump)  
git commit -m "fix(cli): handle invalid keypair format"
git commit -m "fix(rpc): validate transaction signatures"

# Breaking changes (major version bump)
git commit -m "feat(lib)!: change signer interface"
git commit -m "feat: remove deprecated methods

BREAKING CHANGE: removed getBalance method, use getAccountBalance instead"

# Other types (patch version bump)
git commit -m "chore(deps): update solana-sdk to 2.1.10"
git commit -m "docs(readme): add installation instructions"
git commit -m "refactor(lib): simplify token validation logic"
```

### 3. Pull Request Process
1. **Create feature branch**: `git checkout -b feat/my-feature`
2. **Make changes** with conventional commits
3. **Add tests** for new functionality
4. **Update docs** if needed
5. **Create PR** with descriptive title and body
6. **Address review feedback**
7. **Merge** (squash merge preferred)

All contributions must pass CI/CD checks before requesting a review. The project uses GitHub Actions for continuous integration and deployment. See [.github/workflows/core-ci.yml](../.github/workflows/) for the CI/CD workflow.

## Bug Reporting

### Security Vulnerabilities

For security vulnerabilities, please do NOT report them publicly on GitHub Issues. Instead, contact the Solana Private Channels team offline.

### Non-Security Issues/Bugs

When reporting bugs, include:

1. **Description**: Clear description of the issue
2. **Steps to reproduce**:
   ```
   1. Start Solana Private Channels node
   2. Submit transfer transaction
   3. Observe error XYZ
   ```
3. **Expected behavior**: What should happen
4. **Actual behavior**: What actually happens
5. **Environment**:
   - OS and version
   - Rust version (`rustc --version`)
   - Solana CLI version (`solana --version`)
   - Docker version (if applicable)
6. **Logs**: Relevant log output (use code blocks)

### Feature Requests

When proposing features:

1. **Use case**: Describe the problem you're trying to solve
2. **Proposed solution**: Your suggested approach
3. **Alternatives considered**: Other approaches you've thought about
4. **Impact**: Who benefits and how


## Code Style Guidelines

Use `make fmt` to format all code before committing..

**Naming Conventions**:
- **Functions**: `snake_case`
- **Types**: `PascalCase`
- **Constants**: `SCREAMING_SNAKE_CASE`
- **Modules**: `snake_case`

**Code Organization**:
- Keep functions small and focused
- Use descriptive variable names
- Add comments for complex logic, not obvious code
- Group related functionality into modules

### Logging Guidelines
Always use `tracing` macros for logging, NEVER `println!` or `eprintln!`.

**Import tracing macros directly**:
```rust
use tracing::{debug, error, info, warn, trace};

// GOOD: Direct imports
info!("Starting sequencer with max_tx_per_batch: {}", max_tx_per_batch);
error!("Failed to process transaction: {}", error);

// BAD: Using tracing:: prefix
tracing::info!("This is not preferred");

// BAD: Using println/eprintln
println!("This will not work in production");
```

**Log Levels**:
- `trace!()` - Very detailed debugging information
- `debug!()` - Debugging information for development
- `info!()` - Informational messages about normal operation
- `warn!()` - Warning messages for recoverable issues
- `error!()` - Error messages for failures

### Writing Tests

**Unit tests**: Place in same file as implementation:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn my_test_function() {
        // Arrange
        let x = 1;
        let y = 2;
        
        // Act
        let result = x + y;
        
        // Assert
        assert_eq!(result, 3);
    }
}
```

**Integration tests**: Place in `integration/tests/` directory:
```rust
// integration/tests/private_channel/test_transaction_flow.rs
use private_channel_test_utils::*;

#[tokio::test]
async fn my_test_function() {
    // Arrange
    let environment = setup_test_environment().await;

    // Act
    let result = environment.test_function();

    // Assert
    assert_eq!(result, EXPECTED_RESULT);
}
```

## Getting Help

For questions about contributing to Solana Private Channels:

- **GitHub Issues**: https://github.com/solana-foundation/solana-private-channels/issues
- **Stack Exchange**: Ask on https://solana.stackexchange.com/ (use the `private_channel` tag)

## License

By contributing to Solana Private Channels, you agree that your contributions will be licensed under the MIT License. See [LICENSE](./LICENSE) for the full license text.
