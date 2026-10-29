# CLAUDE.md

## Project
Implement MoveInSync Case Study 4: Live Trip Updates Fan-Out — Ordered, Resumable, Back-Pressured.

This is a 24-hour backend assessment. Optimize for correctness, clear engineering decisions, demonstrable guarantees, and maintainability rather than unnecessary production complexity.

## Required behavior
1. Topics for cabs, trips, and offices.
2. Authorization on every subscription, with tenant isolation.
3. Per-topic sequence numbers and ordered event delivery.
4. At-least-once delivery semantics with client-side deduplication.
5. Resume from the last processed sequence; snapshot fallback when replay history expires.
6. Bounded per-client queues and location conflation.
7. Status changes and SOS events must not be silently dropped.
8. SOS priority without violating required per-topic ordering.
9. Multi-gateway event distribution and recovery after gateway failure.
10. Metrics, failure handling, automated tests, performance measurements, and documentation.

## Engineering defaults
- Prefer Java 21 and Spring Boot unless the existing project or environment justifies another choice.
- Use Maven and JUnit 5.
- Use Redis Streams for durable event history and Redis Pub/Sub for notifications only if the recovery design is correct.
- Use atomic sequencing and append operations. Document the ordering boundary and failure semantics.
- Keep core domain logic testable without starting Spring or Redis.
- Use constructor injection, small classes, meaningful names, and clear controller/service/repository boundaries.
- Avoid unnecessary frameworks, abstractions, frontend work, and premature optimization.
- Pin compatible dependency versions. Never invent APIs or claim an unverified feature works.

## Mandatory workflow
1. Inspect the repository and the case study before changing code.
2. Create a requirements checklist and a concise architecture/design note.
3. Record important architecture choices in docs/adr/ with context, decision, alternatives, and consequences.
4. Work in small, independently testable milestones.
5. Before implementing a milestone, state the intended changes and acceptance tests.
6. Write or update tests alongside implementation.
7. Run relevant tests after each change. Run the full suite before every commit.
8. Inspect git diff and git status before committing.
9. Make a commit only when the milestone is coherent, tested, and the working tree changes are understood.
10. Never claim tests, benchmarks, startup, or recovery scenarios succeeded unless they were actually run and the result was checked.
11. Do not hide failing tests, weaken assertions to obtain a green build, or silently change the requirements.
12. Do not commit secrets, credentials, .env files, personal data, generated load-test output, or build artifacts.

## Documentation requirements
Maintain README.md as a living implementation record.
- Requirements and acceptance criteria.
- Architecture and component responsibilities.
- Exact setup and run commands.
- API/WebSocket message examples.
- Implemented, partial, and unimplemented requirements.
- Numbered implementation steps with status and links to tests.
- Actual commands and results for tests and benchmarks.
- Architecture decisions, complexity, memory estimates, limitations, and trade-offs.
- Failure-mode and recovery table.
- Known issues and next steps.

Update docs/IMPLEMENTATION_LOG.md after each completed milestone. Include files changed, behavior implemented, tests run, actual outcomes, decisions, and the next step.

Never fabricate test results, benchmark numbers, commit hashes, completion statuses, or implementation details.

## Git discipline
- Use focused, conventional commits, for example:
  feat: implement per-topic event sequencing
  test: cover replay and duplicate delivery
  feat: add bounded client delivery queues
  docs: document gateway recovery trade-offs
- Prefer one logical milestone per commit.
- Do not rewrite published history or use destructive Git commands without explicit approval.
- Never push or publish without user authorization.

## Working style
- Explain consequential design choices briefly before implementing them.
- Prefer the smallest correct implementation that meets the case study.
- If a requirement conflicts with the current design, stop and explain the conflict before proceeding.
- Separate implemented behavior from planned behavior.
- Ask only when a decision genuinely blocks progress; otherwise choose a reasonable, documented default.
- At each milestone, report changed files, tests run and results, remaining risks, documentation updates, and the proposed commit message.