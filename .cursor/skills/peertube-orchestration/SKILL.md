# PeerTube Orchestration Skill

## When to Load

This skill activates for any multi-step, complex, or cross-module task.
Triggers: "implement", "add", "build", "create", "enhance", "improve",
"refactor", "architecture", "design", "system", "multi-step"

## PeerTube Architecture

### Server (Node.js + Express)
```
server/core/
├── controllers/api/     # REST API endpoints
├── lib/
│   ├── job-queue/       # BullMQ job handlers
│   ├── object-storage/  # S3 storage helpers
│   ├── move-storage/    # Migration logic
│   ├── video-file.ts    # Video file processing
│   └── thumbnail.ts     # Thumbnail generation
├── models/              # TypeORM entities
└── initializers/        # Server boot logic
```

### Client (Angular)
```
client/src/app/
├── +admin/              # Admin panel
├── +my-library/         # User's videos, channels
├── shared/              # Shared components, services
│   └── shared-main/channel/  # Channel service
└── core/                # Core Angular modules
```

### Shared
```
packages/models/src/server/job.model.ts  # Job type definitions
```

## Orchestration Workflow

### 1. Effort Assessment
Quickly determine complexity:

| Effort | Scope | Approach |
|--------|-------|----------|
| Instant | Typo, 1 line | Just fix it |
| Light | 1 file | Read → fix → verify |
| Deep | 2-5 files, same layer | Read all → plan → implement → review |
| Exhaustive | Cross-layer (server+client), new API | Full plan → parallel agents → verify |

### 2. Module Dependency Map

When changing PeerTube features, always consider ALL affected layers:

```
User-facing change
    ├─ server/core/controllers/api/  (new/modified endpoints)
    ├─ server/core/lib/              (business logic, jobs)
    ├─ server/core/models/            (database schema)
    ├─ packages/models/               (job types, shared types)
    ├─ client/src/app/+my-library/  (Angular components)
    └─ client/src/app/shared/        (Angular services)

Storage change
    ├─ server/core/lib/object-storage/
    ├─ server/core/lib/move-storage/
    ├─ server/core/lib/job-queue/handlers/
    └─ packages/models/src/server/job.model.ts
```

### 3. Parallel Execution Template

For multi-directory reviews or implementations:

```
Launching N parallel agents (ALL in ONE message):

[Task 1] description="Review server/core/lib/job-queue"
prompt="Analyze the job queue handlers in server/core/lib/job-queue/handlers/
for the PeerTube project. Focus on:
- Correct job handler registration
- Error handling and retry logic
- Object storage integration
- Type consistency with job.model.ts
Report findings with file:line references."
run_in_background: true

[Task 2] description="Review Angular channel components"
prompt="Analyze the video channel Angular code in:
- client/src/app/+my-library/+my-video-channels/
- client/src/app/shared/shared-main/channel/video-channel.service.ts
Focus on:
- Component/service patterns
- HTTP error handling
- Unsubscribe patterns
- Change detection strategy
Report findings with file:line references."
run_in_background: true

# ... more tasks

Collect all via TaskOutput, then synthesize.
```

### 4. Job Queue Pattern

For async operations (video processing, storage migration):

```
When adding a new async job type to PeerTube:

1. Define job type in packages/models/src/server/job.model.ts
2. Create handler in server/core/lib/job-queue/handlers/<name>.ts
3. Register in server/core/lib/job-queue/job-queue.ts
4. Add to admin UI in client/src/app/+admin/system/jobs/
5. Document in CLAUDE.md
```

### 5. Object Storage Pattern

```
When modifying storage logic:

1. Read object-storage-helpers.ts for existing patterns
2. Read move-to-object-storage.ts for migration pattern
3. Never store files in database
4. Always use signed/expiring URLs for private content
5. Test with actual S3-compatible storage (LocalMinIO for dev)
```

## Anti-Patterns to Avoid

- Modifying only one layer (e.g., server without client update)
- Blocking the request thread for video processing
- Adding new database queries without indexes
- Hardcoding storage paths instead of using config
- Forgetting to unsubscribe Angular observables
