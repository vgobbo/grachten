# Grachten — Agent Guide

This file contains critical context, repository architecture, and workflow rules for working with the **Grachten** project.

---

## 1. Project Overview & Purpose

**Grachten** is a simple multi-producer, single-consumer (MPSC) channel implementation in Rust, designed with an interface similar to Tokio's MPSC channel, created for educational and learning purposes.

Its primary functions include:
- **MPSC Channel Primitives**: Providing sender (`Sender`) and receiver (`Receiver`) handles for transmitting messages across asynchronous tasks or threads.
- **Tokio-like API**: Matching the ergonomics and behavioral semantics of Tokio's `mpsc` channel (e.g. bounded channels, capacity handling, async `send` and `recv`, permits, disconnection signaling).
- **Educational & Learning Focus**: Designed as an idiomatic implementation for understanding asynchronous channels, concurrency primitives, and synchronization mechanics in Rust.

> [!IMPORTANT]
> ### Agent Role & Working Agreement
> In this project, your goal as an agent is strictly to **support development**, not to write code or come up with the solution yourself.
> - **Do NOT** implement the channel or solve coding problems independently.
> - **DO** assist with code reviews, design discussions, architectural feedback, explaining Rust concurrency or async concepts (e.g., atomics, wakers, memory ordering), setting up test harnesses, and answering the developer's questions.

---

## 2. Crate Architecture

The repository is organized as a single Cargo library crate:

| Crate | Directory | Description |
|---|---|---|
| [`grachten`](Cargo.toml) | `.` (root) | The primary library crate. Implements the MPSC channel abstractions in [`src/lib.rs`](src/lib.rs) and supporting modules. |

---

## 3. Local Development Environment & Scripts

All local environment helper scripts are located in [`local/`](local/). Fish Shell is the preferred execution environment, but standard shell commands and wrappers exist.

### Setup Commands
- **Git Hooks**: Install repository pre-commit hooks:
  ```shell
  ./local/setup-hooks
  ```
- **Speeding up Cargo Builds**:
  - [`local/setup-target`](local/setup-target) binds the cargo `target/` directory to a tmpfs or zram mount (`/var/tmp` or `/tmp`) to save SSD lifespan and compile faster. Run once per reboot.

### Helper Scripts
- [`local/setup-hooks`](local/setup-hooks): Symlinks repository git hooks into `.git/hooks/`.
- [`local/setup-target`](local/setup-target): Configures a symlinked temporary target directory for fast builds.
- [`local/hooks/commit-msg`](local/hooks/commit-msg): Commit message hook wrapper delegating to [`local/hooks/impl/commit-msg`](local/hooks/impl/commit-msg) to enforce Conventional Commits 1.0.
- [`local/hooks/pre-commit`](local/hooks/pre-commit): Pre-commit hook wrapper delegating to [`local/hooks/impl/pre-commit`](local/hooks/impl/pre-commit).

---

## 4. Development & Coding Rules

- **Agent Constraints**:
  - Always remember that the agent's role is to support the developer, not write the implementation code or deliver complete solutions.
- **Code Style**: Formatting rules are defined in [`rustfmt.toml`](rustfmt.toml) and [`.taplo.toml`](.taplo.toml) (tab indentations with `hard_tabs = true`, maximum width of `120` columns).
  - It is not necessary to manually run `rustfmt`, `clippy`, or `taplo`; the repository pre-commit hook automatically formats Rust and TOML files and executes Clippy checks upon commit.
- **Concurrency & Safety**:
  - Focus on soundness, memory safety, and thread safety (`Send` / `Sync`).
  - Document synchronization invariants, atomics, and lock strategies clearly.
- **Git Commits & Hooks**:
  - Always strictly adhere to [Conventional Commits 1.0](https://www.conventionalcommits.org/) format (`<type>(<scope>): <description>` or `<type>: <description>`). Valid types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
  - **Never skip git hooks** (e.g. NEVER use `--no-verify`) unless explicitly and uniquely requested by the developer for a specific operation. Pre-commit hooks automatically format Rust and TOML files and execute Clippy checks; commit-msg hooks enforce Conventional Commits 1.0 formatting.

---

## 5. Typical Tasks & How to Run

- Run cargo checks: `cargo check`
- Run unit/integration tests: `cargo test`
- Build the library: `cargo build`
- **Updating Dependencies**:
  - Update all dependencies and the lockfile via `cargo update`.
  - When bumping minimum versions in [`Cargo.toml`](Cargo.toml), **only specify the major and minor versions** (e.g. `1.2`, never patch versions like `1.2.3`).
  - Always verify with `cargo check` after updating versions.

---

## 6. Release Workflow & Versioning

Releases follow [Semantic Versioning](https://semver.org/) (`vMAJOR.MINOR.PATCH`). When performing a release, adhere to the following steps:

1. **Prompt the User for Release Version**:
   - Inspect changes made since the last git release tag (`git log $(git describe --tags --abbrev=0 2>/dev/null || git rev-list --max-parents=0 HEAD)..HEAD --oneline`).
   - Suggest the next version number following Semantic Versioning:
     - **Major** (`X.0.0`): Breaking API/contract changes or backwards-incompatible refactors.
     - **Minor** (`0.X.0` or `X.Y.0`): New features, new endpoints, or channel API additions.
     - **Patch** (`X.Y.Z`): Bug fixes, internal refactoring, or minor maintenance.
   - Ask the user to confirm or specify the target version `<version>` (e.g., `0.1.0` or `0.2.0`).

2. **Set Release Version & Tag**:
   - In [`Cargo.toml`](Cargo.toml), update `version` under `[package]` to `<version>` (e.g., `version = "0.1.0"`).
   - Update [`Cargo.lock`](Cargo.lock) by running `cargo check`.
   - Create a dedicated release commit containing `Cargo.toml` and `Cargo.lock` following Conventional Commits:
     ```shell
     git add Cargo.toml Cargo.lock
     git commit -m "chore(release): version <version>"
     ```
   - Tag the release commit with `v<version>`:
     ```shell
     git tag v<version>
     ```

3. **Generate Release Builds**:
   - Clean the `target/` directory contents (without removing the `target` directory itself) and build release artifacts:
     ```shell
     rm -r target/*
     nice -n 19 cargo build --profile release
     ```

4. **Prepare for Next Development Cycle (`-NEXT`)**:
   - Immediately update `version` in [`Cargo.toml`](Cargo.toml) to append `-NEXT` (e.g., `version = "0.2.0-NEXT"` or `version = "0.1.1-NEXT"`).
   - Update [`Cargo.lock`](Cargo.lock) by running `cargo check`.
   - Commit the change following Conventional Commits:
     ```shell
     git add Cargo.toml Cargo.lock
     git commit -m "chore(release): prepare for next development cycle"
     ```
