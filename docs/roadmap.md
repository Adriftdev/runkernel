# runkernel Roadmap

## 0.1 Public Shape

- README, design notes, roadmap, contributing guide, and license.
- Compiling examples for basic DAGs, caching, rollback, outputs, parallel branches, and ops.
- Public exports reviewed and documented at a basic level.

## 0.2 Reliable Caching

- Pipeline-scoped cache entries.
- Explicit cache keys for native function tasks.
- Cache hit and miss reasons.
- Tests for invalidation and glob behavior.

## 0.3 Structured Results

- `PipelineResult`, `TaskResult`, `TaskStatus`, and `PipelineSummary`.
- Richer event model.
- Skipped and cancelled task tracking.

## 0.4 Context and Outputs

- Typed task outputs.
- Shared output store.
- Dependency output access.
- Cached output restoration.

## 0.5 Rollback and Failure Policy

- Configurable failure policies.
- Configurable rollback policies.
- Reverse-order rollback.
- Rollback failure reporting.

## 0.6 Manifest CLI

- Full project rename to `runkernel`.
- `runkernel.toml` workflow discovery.
- `runkernel run [task]`.
- `runkernel list`.
- `runkernel graph`.
- `runkernel explain <task>`.
- `runkernel init`.
- `runkernel workflows`.
- `runkernel cache clean/status`.
- `runkernel-cli-support::RunkernelApp` internal protocol support.

## After 0.6

- Improve graph output formats.
- Add shell completions.
- Add richer run progress rendering.

## Non-Goals For Now

- Distributed execution.
- Remote workers.
- Persistent database state.
- Web UI.
- YAML pipeline format.
- Kubernetes-native execution.
- GitHub Actions replacement.
- Plugin system.
- Container runtime abstraction.
