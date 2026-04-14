---
description: Run comprehensive verification of PeerTube code changes using multi-agent adversarial verification.
---

# PeerTube Verify

Run comprehensive verification after code changes using parallel agents and adversarial review.

## Phase 1: Parallel Checks (All in One Message)

```
Task 1: Type Check
  Run: cd server && npx tsc --noEmit
  Run: cd client && npx tsc --noEmit
  Report: Errors with file:line

Task 2: Server Tests
  Run: cd server && npm test
  Report: Pass/fail with failing test names

Task 3: Client Tests
  Run: cd client && npm test
  Report: Pass/fail with failing test names

Task 4: Lint Check
  Run: cd server && npm run lint
  Run: cd client && npm run lint
  Report: Violations with file:line

Task 5: Security Scan
  Grep: hardcoded secrets, console.log, TODO
  Check: new API endpoints have authorization
  Report: Security findings
```

## Phase 2: Adversarial Review

```
Task A: False Positive Filter
  Review each Phase 1 finding
  Mark: REAL ISSUE / FALSE POSITIVE / ACCEPTABLE

Task B: Missing Issues
  Check edge cases in changed code
  Verify error handling
  Verify job queue integration (if async ops)

Task C: PeerTube-Specific
  Check: object storage usage
  Check: federation compatibility
  Check: Angular unsubscribe patterns
```

## Phase 3: Synthesis

```
## Verification Results: [PASS/FAIL]

### Confirmed Issues
1. [Issue] - [Location] - [Why confirmed]

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
- Confirmed after review: Y
- False positives filtered: Z
```

## Usage

```
/peertube:verify
```

Verifies all staged and unstaged changes in the PeerTube repository.
