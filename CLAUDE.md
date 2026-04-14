# PeerTube Development Guide

## Project Overview

PeerTube is a federated video hosting platform built with Node.js (server) and Angular (client). This is a large monorepo with clear separation between client and server.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Server | Node.js, TypeScript, Express |
| Client | Angular, TypeScript, SCSS |
| Database | PostgreSQL |
| Cache | Redis |
| Object Storage | S3-compatible |
| Job Queue | BullMQ |
| API | REST |
| Testing | Mocha, Playwright |

## Directory Structure

```
client/          # Angular frontend application
server/          # Node.js backend
  core/          # Core server logic
    controllers/  # API controllers
    lib/         # Business logic, services, utilities
    models/      # Database models
    initializers/# Server initialization
    ...
packages/        # Shared TypeScript packages
```

## Key Conventions

### Git Workflow
- Conventional commits: `feat/`, `fix/`, `chore/`, `refactor/`
- Branch naming: `feat/<description>`, `fix/<description>`
- Always run tests before committing

### Code Standards
- TypeScript strict mode
- ESLint + Prettier formatting
- Import ordering enforced
- Max line length: 140

### Testing
- Unit tests: Mocha + chai
- E2E tests: Playwright
- Run: `npm test` (server), `npm test` (client)
- Build check: `npm run build`

### Running Commands

```bash
# Server
cd server
npm install
npm run dev          # Development
npm test             # Tests
npm run lint         # Linting

# Client
cd client
npm install
npm run dev          # Development
npm test             # Tests
npm run lint         # Linting
npm run build        # Production build

# Root (CI)
npm run test:server
npm run test:client
```

### Database
- Migrations managed via TypeORM
- Never run raw SQL in application code
- Use the job queue (BullMQ) for async operations

### Object Storage
- Videos and thumbnails stored in S3-compatible storage
- Use `move-to-object-storage.ts` for migration jobs
- Never store large files in the database

### Job Queue
- All async operations go through BullMQ
- Job types defined in `packages/models/src/server/job.model.ts`
- Handlers in `server/core/lib/job-queue/handlers/`

## Important Notes

- This is a federated system — changes to APIs affect federation
- Video processing is CPU-intensive — use job queue
- Always verify object storage paths when adding new file types
