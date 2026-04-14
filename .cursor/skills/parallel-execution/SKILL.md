# PeerTube Parallel Execution Patterns

## When to Load

This skill activates for any request involving multiple agents, parallel tasks,
concurrent work, or distributing work across modules.

Triggers: "parallel", "simultaneously", "concurrent", "multiple directories",
"split work", "in parallel", "run simultaneously", "divide and conquer"

## Core Rule

**ALL Task calls MUST be in a SINGLE assistant message for true parallelism.**

```python
# ✅ CORRECT — All in one message
[task_1] run_in_background: true
[task_2] run_in_background: true
[task_3] run_in_background: true

# ❌ WRONG — Sequential execution
[task_1] run_in_background: true
# (waits for task_1 to complete before starting task_2)
[task_2] run_in_background: true
```

## PeerTube-Specific Parallelization

### Pattern 1: Cross-Layer Feature Implementation

```
User: "Add video channel banner reset feature"

Tasks (can run in parallel):
- Task 1: Add job type to packages/models/src/server/job.model.ts
- Task 2: Create job handler in server/core/lib/job-queue/handlers/video-channel-reset.ts
- Task 3: Register handler in server/core/lib/job-queue/job-queue.ts
- Task 4: Add API endpoint in server/core/controllers/api/video-channels/index.ts
- Task 5: Update Angular service in client/src/app/shared/shared-main/channel/video-channel.service.ts
- Task 6: Update Angular component in client/src/app/+my-library/+my-video-channels/edit/pages/video-channel-edit-general.component.ts

All 6 can run in parallel since they touch different files.
```

### Pattern 2: Directory-Based Review

```
User: "Review the job queue system"

Tasks (parallel):
- Task 1: Review handlers in server/core/lib/job-queue/handlers/
- Task 2: Review job-queue.ts registration
- Task 3: Review admin UI in client/src/app/+admin/system/jobs/
- Task 4: Review job model types in packages/models/src/server/job.model.ts
```

### Pattern 3: Perspective-Based Audit

```
User: "Security audit the API layer"

Tasks (parallel):
- Task 1: Auth flow review — server/core/controllers/api/video-channels/
- Task 2: Input validation review — all controller files
- Task 3: Rate limiting review — middleware
- Task 4: Angular security review — services, interceptors
```

## TodoWrite in Parallel Mode

```javascript
// Before launching parallel tasks
todos = [
  { id: "1", content: "Add job type to job.model.ts", status: "in_progress" },
  { id: "2", content: "Create job handler", status: "in_progress" },
  { id: "3", content: "Register handler in job-queue.ts", status: "in_progress" },
  { id: "4", content: "Add API endpoint", status: "in_progress" },
  { id: "5", content: "Update Angular service", status: "in_progress" },
  { id: "6", content: "Update Angular component", status: "in_progress" },
  { id: "7", content: "Synthesize and verify", status: "pending" }
]

// After TaskOutput for each
todos = [
  { id: "1", content: "...", status: "completed" },
  { id: "2", content: "...", status: "completed" },
  // ... all completed ...
  { id: "7", content: "Synthesize and verify", status: "in_progress" }
]
```

## When NOT to Parallelize

- Tasks modifying the SAME file (merge conflicts)
- Tasks with REAL dependencies (B needs A's output)
- Sequential workflows (commit → push → PR)
- Order matters for correctness

## Performance Comparison

| 5 tasks @ 30s each | Sequential | Parallel |
|---------------------|-----------|---------|
| Total time | ~150s | ~30s |

## Subagent Prompt Template

```
You are a [SPECIALIST] working on the PeerTube codebase.

Task: [CLEAR DESCRIPTION OF WHAT TO ACCOMPLISH]

Context: PeerTube is a federated video platform with:
- Server: Node.js + Express + TypeORM + PostgreSQL
- Client: Angular + TypeScript + SCSS
- Job Queue: BullMQ for async operations
- Storage: S3-compatible object storage

Files to work with:
- [SPECIFIC FILES OR PATTERNS]

Output format:
- Finding: [file:line] — [description]
- Severity: [Critical/Warning/Suggestion]
- Fix: [specific fix recommendation]

Focus areas:
- [PRIORITY 1]
- [PRIORITY 2]
```
