---
description: Create a persistent implementation plan for PeerTube tasks. Wait for user confirmation before writing any code.
---

# PeerTube Plan

Create a step-by-step implementation plan for PeerTube tasks.

## What It Does

1. **Understand** → Read requirements, explore codebase
2. **Restate** → Clarify what needs to be built
3. **Identify Risks** → Surface blockers and dependencies
4. **Plan Phases** → Break into ordered steps
5. **Wait for Confirmation** → Do NOT code until approved

## Arguments

```
$ARGUMENTS: Description of the task
```

## Examples

```
/peertube:plan "Add video import from remote URLs"
/peertube:plan "Implement video chapter markers"
/peertube:plan "Add multi-instance federation caching"
```

## PeerTube Planning Considerations

### Cross-Layer Changes
Always identify ALL affected layers:
- server/core/controllers/api/ — API endpoints
- server/core/lib/ — Business logic, job queue
- server/core/models/ — Database schema
- packages/models/ — Shared types
- client/src/app/+my-library/ — Angular pages
- client/src/app/shared/ — Angular services

### Job Queue Planning
- What async operations are needed?
- What job type constants are needed?
- How should failed jobs be retried?
- Does the admin UI need updating?

### Object Storage Planning
- What files need storage?
- Are there migration paths from local storage?
- How should cleanup work?

### Federation Planning
- Does this affect ActivityPub endpoints?
- Are there backward compatibility concerns?
- What other PeerTube instances might be affected?

## Plan Template

```markdown
# Implementation Plan: [Title]

## Requirements Restatement
[Brief restatement of what needs to be built]

## Affected Layers
- [ ] Server: [files/modules]
- [ ] Client: [files/modules]
- [ ] Shared: [files/modules]

## Implementation Phases

### Phase 1: [Name]
- [Step 1]
- [Step 2]

### Phase 2: [Name]
- [Step 1]
- [Step 2]

## Dependencies
- [External: Redis, S3, etc.]
- [Internal: Depends on Phase 1]

## Risks
- HIGH: [Risk description]
- MEDIUM: [Risk description]
- LOW: [Risk description]

## Estimated Complexity
[High/Medium/Low]

**WAITING FOR CONFIRMATION**: Proceed with this plan?
```
