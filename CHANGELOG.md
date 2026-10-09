# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Host preparation on the Batch job, before Harbor is imported: `bootstrap` (a
  staged shell script, with `bootstrap_files` and `bootstrap_timeout_sec`, that can
  export environment through `HARBOR_METAFLOW_ENV_FILE`) and `prepare` (a
  `module:function` called with the shard's work dir). A failure fails every trial
  of the shard with "host preparation failed".
- `examples/bootstrap/docker-cli-and-harbor.sh`: installs the Docker CLI, Compose,
  Buildx and Harbor (from a staged wheel or the index) on an image without them.
- `job_user`: registers the flow's Batch job definitions with
  `containerProperties.user` (Metaflow's `@batch` has no `user` option).
- `code_paths` and `package_suffixes`: ship local code with the flow.
- `flow_exit_grace_sec`.
- `HARBOR_TELEMETRY` is forwarded to the Batch jobs when set.
- "Tested on AWS" section in the README.

### Changed

- The package ships itself with the flow (Metaflow code package), so the Batch
  image no longer needs it installed.
- Stopping a run records what it did, and any error, in the run's
  `metaflow-<token>.log`; a failing terminate is logged instead of swallowed.

### Fixed

- The temporary flow directory (and the flow process, if it hangs in its join
  step) was left behind after every complete run, because the last trial comes
  back before the flow exits.

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
