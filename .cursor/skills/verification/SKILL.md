# PeerTube Verification Loop

## When to Load

This skill activates after any significant implementation, before commits,
or when user asks to "verify", "check", "validate", "test", or "lint".

## PeerTube Verification Pipeline

### Phase 1: Parallel Verification Agents

Launch all checks in parallel (single message):

```
Task 1: Type Check
  Run: cd server && npx tsc --noEmit
  Run: cd client && npx tsc --noEmit
  Report: Any TypeScript errors with file:line

Task 2: Server Tests
  Run: cd server && npm test
  Report: Test pass/fail with failing test names

Task 3: Client Tests
  Run: cd client && npm test
  Report: Test pass/fail with failing test names

Task 4: Lint Check
  Run: cd server && npm run lint
  Run: cd client && npm run lint
  Report: Lint violations with file:line

Task 5: Security Scan
  Grep for: hardcoded secrets, console.log, TODO comments
  Check: new API endpoints have authorization
  Check: object storage paths are correct
  Report: Security findings
```

### Phase 2: Adversarial Review

Spawn 3 adversarial agents to filter false positives:

```
Task A: False Positive Filter
  Review each Phase 1 finding
  Determine if it's a real issue or acceptable pattern
  Mark clearly: REAL ISSUE / FALSE POSITIVE / ACCEPTABLE

Task B: Missing Issues Finder
  Look for issues Phase 1 might have missed
  Check edge cases in changed code
  Verify error handling is adequate
  Verify job queue for async operations

Task C: PeerTube-Specific Review
  Check federation compatibility
  Check object storage usage
  Check job queue integration
  Check Angular unsubscribe patterns
```

### Phase 3: Synthesis

```
## Verification Results: [PASS/FAIL]

### Confirmed Issues
1. [Issue] - [Location] - [Why confirmed real]

### Warnings
1. [Warning] - [Location] - [Context]

### All Checks
- [ ] Type checking: PASS/FAIL
- [ ] Server tests: PASS/FAIL
- [ ] Client tests: PASS/FAIL
- [ ] Linting: PASS/FAIL
- [ ] Security: PASS/FAIL

### Summary
- Initial findings: X
- After adversarial: Y confirmed
- False positives: Z filtered
```

## PeerTube-Specific Checks

### Object Storage Verification
- New file types stored in object storage, not DB
- Storage paths use config, not hardcoded
- Cleanup handlers exist for failed uploads
- Migration jobs handle existing files

### Job Queue Verification
- CPU-intensive work uses job queue
- Job handlers handle retries gracefully
- Job types registered in job.model.ts
- Admin UI updated for new job types

### Federation Verification
- API changes maintain backward compatibility
- ActivityPub endpoints follow spec
- Response format matches ActivityPub schema

### Angular Verification
- Services unsubscribe in ngOnDestroy
- HTTP errors handled with user feedback
- OnPush change detection where appropriate
