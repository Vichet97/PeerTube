# PeerTube Autonomous Loop Patterns

## When to Load

This skill activates when the user wants continuous/automated development
workflows, CI-style pipelines, or persistent agent loops.

Triggers: "continuous", "automate", "loop", "autonomous", "pipeline",
"iterative", "batch", "run overnight", "background"

## Loop Patterns for PeerTube

### Pattern 1: Sequential Pipeline (Simple)

Best for: Daily development steps, scripted workflows.

```bash
#!/bin/bash
# peertube-dev.sh — Sequential pipeline

set -e

# Step 1: Implement feature
claude -p "Read the video channel spec. Implement the banner reset feature
following the existing pattern in server/core/lib/job-queue/handlers/.
Write tests first. Do NOT create documentation files."

# Step 2: Cleanup
claude -p "Review all changed files. Remove any console.log, TODO comments,
or test code that tests framework behavior. Run npm test."

# Step 3: Verify
claude -p "Run the PeerTube verification pipeline:
- cd server && npx tsc --noEmit
- cd server && npm test
- cd client && npx tsc --noEmit
- cd client && npm run lint

Fix any failures."

# Step 4: Commit
claude -p "Create a conventional commit for staged changes.
Use 'feat: add video channel banner reset' as the format."
```

### Pattern 2: Continuous Improvement Loop

Best for: Iterative improvements, test coverage, bug fixes.

```bash
#!/bin/bash
# peertube-improve.sh — Continuous improvement

MAX_RUNS=10
COMPLETION_SIGNAL="PEERTUBE_WORK_COMPLETE"

for i in $(seq 1 $MAX_RUNS); do
  echo "=== Iteration $i ==="

  claude -p "Improve test coverage in the PeerTube codebase.
  Focus on server/core/lib/job-queue/ and server/core/lib/video-*.
  Run tests after each change. Look for untested edge cases."

  # Check for completion signal
  if grep -q "$COMPLETION_SIGNAL" .claude/session.md; then
    echo "Completion signal found. Stopping."
    break
  fi

  # Update progress
  echo "## Progress - Iteration $i" >> .claude/progress.md
  git add -A && git commit -m "chore: iteration $i" || true
done
```

### Pattern 3: Multi-Branch Parallel Development

Best for: Multiple features developed simultaneously.

```bash
#!/bin/bash
# peertube-parallel.sh — Parallel branch development

FEATURES=("banner-reset" "thumbnail-regen" "video-import")

for feature in "${FEATURES[@]}"; do
  git worktree add -b "feat/$feature" ../peertube-$feature HEAD &
done
wait

# Each worktree can now run independently:
# cd ../peertube-banner-reset && claude
# cd ../peertube-thumbnail-regen && claude
# cd ../peertube-video-import && claude

# Merge when done
for feature in "${FEATURES[@]}"; do
  git merge "feat/$feature" || true
  git worktree remove "../peertube-$feature"
done
```

### Pattern 4: PR Loop with Verification

Best for: Review-then-merge workflows.

```bash
#!/bin/bash
# peertube-pr-loop.sh

BRANCH="feat/$1"
git checkout -b "$BRANCH"

# Implement
claude -p "Implement: $2"

# Verify
claude -p "Run PeerTube verification:
- cd server && npx tsc --noEmit && npm test
- cd client && npx tsc --noEmit && npm run lint
Fix all failures."

# Create PR
claude -p "Create PR with conventional commit message.
Title: feat: $2"

# Wait for review
gh pr merge --admin --auto
gh pr checks --wait

# Fix if needed
while gh pr checks status | grep -q "failing"; do
  claude -p "Fix the failing CI checks. Read the logs and address each failure."
  git add -A && git commit --amend --no-edit && git push --force-with-lease
  gh pr checks --wait
done
```

## De-Sloppify Pattern (PeerTube)

After any implementation, add cleanup pass:

```
claude -p "Clean up PeerTube changes:
- Remove console.log statements
- Remove TODO/FIXME comments
- Remove test code that tests framework behavior
- Remove commented-out code
- Ensure job handlers have proper error handling
- Ensure Angular components unsubscribe properly

Run tests to verify nothing broke."
```

## Integration with Claude Code CLI

Since you have Claude Code CLI installed alongside Cursor, you can use
the full autonomous loop patterns. Cursor provides the interactive
intuit intent detection, Claude Code CLI provides the autonomous loop
execution.

### Using Continuous Claude (Claude Code CLI)
```bash
# Install continuous-claude (review code first!)
npx continuous-claude --prompt "Improve test coverage in PeerTube" --max-runs 10
```

### Using dmux (Claude Code CLI)
```bash
# Start dmux for parallel panes
dmux
# n: new pane, m: merge panes
```

## PeerTube-Specific Considerations

- Always run server tests before client tests (client depends on server types)
- Object storage changes need manual verification with MinIO
- Federation changes need integration testing with other PeerTube instances
- Job queue changes should be tested with actual queue backend
- Angular changes benefit from Playwright E2E tests

## Exit Conditions

Always have exit conditions to prevent infinite loops:

| Condition | Example |
|-----------|---------|
| Max runs | `--max-runs 10` |
| Cost limit | `--max-cost $50` |
| Time limit | `--max-duration 2h` |
| Completion signal | `PEERTUBE_WORK_COMPLETE` |
| Test coverage target | Stop when `>80%` |
