# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-09

### Added

- `metaflow-batch` trial backend for `harbor run --backend` (registered under the
  `harbor.backends` entry point group).
- Sharding of trial configs, staging of task directories and agent files to an
  S3 prefix or a shared directory, and copy-back of finished trial directories
  with the submitted config restored.
- Generated Metaflow flow with `@batch` (queue, image, CPU, memory, IAM role,
  host volumes), `@timeout`, `@environment` and `@secrets`, or a local mode.
- START/END hook replay, cancellation (stops the flow and terminates Batch jobs),
  `aclose()` cleanup of complete submissions, and `runs_environment_remotely`.
- Clear refusal on a Harbor without the trial backend hook.
