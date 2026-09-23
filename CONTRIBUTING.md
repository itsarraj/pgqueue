# Contributing

We welcome contributions! These tools are written in Rust. Here is how you can help.

## Prerequisites
- [Rust and Cargo](https://rustup.rs/) installed.

## Development Workflow
1. Fork the repository and create a new branch for your feature or bugfix.
2. Make your changes.
3. Ensure the code compiles and passes all lints without warnings:
   ```sh
   cargo check
   cargo clippy -- -D warnings
   ```
4. Format the code before committing:
   ```sh
   cargo fmt
   ```
5. Run the tests to ensure nothing is broken:
   ```sh
   cargo test
   ```

## Commit Guidelines
- Keep commits focused and logically separated.
- **Important**: Do not include AI attribution in commit messages. Do not use `Co-Authored-By: Claude <...>` or similar trailers, and do not mention that code was AI-generated. Write the message as you would for work you did yourself: plain, factual, explaining *why* over *what*.

## Submitting a Pull Request
- Open a Pull Request against the `main` branch.
- Provide a clear description of the problem you're solving or the feature you're adding.
- Ensure all CI checks pass (if applicable).

## License
By contributing, you agree that your contributions will be licensed under the MIT License.
