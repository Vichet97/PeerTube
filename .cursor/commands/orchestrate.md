---
description: Orchestrate multi-step PeerTube development tasks. Break down complex tasks, delegate to specialists, run parallel agents, synthesize results.
---

# PeerTube Orchestrate

This command activates the orchestrator for complex PeerTube development tasks.

## When to Use

- Multi-step feature implementations
- Cross-layer changes (server + client)
- New job queue handlers
- Object storage migrations
- Architecture planning

## How It Works

1. **Analyze** → Understand scope, identify all affected files/layers
2. **Plan** → Use TodoWrite to create ordered task list
3. **Delegate** → Spawn specialist agents via Task tool
4. **Parallelize** → All independent tasks in ONE message (CRITICAL)
5. **Collect** → TaskOutput for each agent result
6. **Synthesize** → Merge results, resolve conflicts
7. **Verify** → Run build/lint/test checks

## Arguments

```
$ARGUMENTS:
- feature <description> — Full feature workflow
- bugfix <description> — Bug fix workflow
- refactor <description> — Refactoring workflow
- security <description> — Security review workflow
- custom <agents> <description> — Custom agent sequence
```

## Examples

```
/peertube:orchestrate feature "Add video channel avatar reset with object storage migration"
```

## PeerTube Module Map

```
Server:
  server/core/controllers/api/         → API endpoints
  server/core/lib/job-queue/handlers/  → Job handlers
  server/core/lib/object-storage/      → S3 storage
  server/core/models/                  → Database models

Client:
  client/src/app/+my-library/          → User-facing pages
  client/src/app/shared/              → Shared services
  client/src/app/+admin/              → Admin panel

Shared:
  packages/models/src/server/job.model.ts → Job types
```

## Effort Levels

| Level | Scope | Approach |
|-------|-------|----------|
| Instant | Typo, 1 line | Just fix it |
| Light | 1 file | Read → fix → verify |
| Deep | 2-5 files | Read all → plan → implement → review |
| Exhaustive | Cross-layer | Full plan → parallel agents → verify |

## Parallel Execution Rule

ALL Task calls MUST be in a SINGLE assistant message for true parallelism:

```
[task_1] description="..." run_in_background: true
[task_2] description="..." run_in_background: true
[task_3] description="..." run_in_background: true
```
