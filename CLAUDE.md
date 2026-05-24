# Contribution Instructions

## Purpose
The sole purpose of this project is to be an in-process emulator of Redis for use in integration tests.  It should mirror the behaviour of Redis as much as is feasibly possible in the context of an in-process emulator.  eg we shouldn't be supporting commands or features that Redis doesn't, we want it to fail when Redis fails and pass when Redis passes.  If tests are passing on the InMemoryEmulator but failing when using a real Redis instance, it's most likely that there is incorrect behaviour in the InMemoryEmulator and the tests are incorrect.

## TDD Workflow

- Always use Test-Driven Development (TDD): write tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.
- For every unit test written, if possible, write the equivalent integration test, testing the same functionality from the entry point.

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Reflection Policy

- **Do not use reflection as a first resort.** Explore all public API options before considering reflection.
- Reflection on internal/private members of external libraries (e.g., SDK backing fields) is fragile — it can break silently on library updates with no compile-time warning.
- If reflection is genuinely the only viable approach after exhausting alternatives, it may be used — but:
  - **The PR description must explicitly state in bold that reflection is used**, what it targets, and why no public API alternative exists.
  - Add a code comment at the reflection site explaining the dependency and what would break if the internal member is renamed or removed.
  - Prefer a graceful fallback (e.g., leave the value as null) over a hard failure if the reflected member is missing.

## Behavioral Source Requirements

Every piece of behavioral logic in the source code — status codes, validation rules, error conditions, side-effect semantics — **must** be backed by a verified source. This prevents accidental divergence from real Redis behavior.

### Rules

1. **Before implementing any behavioral logic**, find and verify the expected behavior from one of the approved sources listed below.
2. **Add a code comment** at the implementation site citing the source (a short URL or description is sufficient). Example:
   ```csharp
   // Ref: https://redis.io/docs/latest/commands/set/
   //   "Set key to hold the string value. If key already holds a value, it is overwritten."
   ```
3. **If sources conflict**, prefer the official Redis documentation over observed behavior. Document the discrepancy in a code comment and mark the relevant integration test with the appropriate trait.
4. **If no source can be found**, do not guess. Ask for guidance or raise a discussion in the PR.

### Approved Behavioral Sources (in priority order)

| Priority | Source | URL / Location |
|----------|--------|----------------|
| 1 | Redis official command reference | https://redis.io/docs/latest/commands/ |
| 2 | Redis official documentation | https://redis.io/docs/latest/ |
| 3 | Redis data types documentation | https://redis.io/docs/latest/develop/data-types/ |
| 4 | StackExchange.Redis API reference | https://stackexchange.github.io/StackExchange.Redis/ |
| 5 | StackExchange.Redis source code | https://github.com/StackExchange/StackExchange.Redis |
| 6 | Redis source code | https://github.com/redis/redis |
| 7 | Observed behavior on a real Redis instance | (testing against a live Redis server) |

> **Note:** Source 7 (observed behavior on a real instance) is the weakest evidence. Always cross-reference with sources 1–6 when possible.

## Versioning & Release

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in `src/Directory.Build.props` (the single `<Version>` property shared by all packages).
- **On `main`:** Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
- **On any other branch:** Commit and push the code changes and version bump only. Do not create or push a tag.

## Test Classification Rules

Tests are split into two projects. When creating or moving tests, follow these rules:

### Tests.Integration
- Uses the test fixture factory to obtain a Redis connection, where session context is managed via xUnit collection fixtures
- Goes through the real StackExchange.Redis client pipeline via the in-process test server
- Must **not** use internal store classes, fault injectors, or any `internal` API
- Can run against in-memory or real Redis via a test target environment variable
- **This is the primary test project** — every test should be an integration test unless it requires internal API access

### Tests.Unit
- Uses internal store or client classes directly
- Tests that use the service but also touch internal APIs (e.g., fault injection internals) belong here
- Only runs in-memory — never against a real instance

### Tests.Shared
- Class library (not a test project) — shared infrastructure, fixtures, traits, and models
- Referenced by both Unit and Integration projects

### Key constraint
The Integration project does **not** have `InternalsVisibleTo` access. If a test needs internal APIs, it belongs in Unit.

## Documentation

After any changes are made that might affect the public API or functionality, documentation must be updated to reflect those changes. The documentation should be clear and comprehensive, covering all new features, changes to existing features, and any deprecations or removals. This includes updating the README file (if relevant), but mainly the wiki which can be found in a sister folder to the main repository — `../InMemoryEmulator.Redis.wiki`.

## Other stuff
Execute all plans in full, do not skip anything

Fix behavioral bugs in the InMemoryEmulator codebase, ensuring all fixes are backed by verified sources and accompanied by appropriate test coverage.  Don't just change the test to match the buggy behavior — find the root cause in the InMemoryEmulator code and fix it to match real Redis behavior. Then add or update tests to verify the fix and prevent regressions.

Assume first and foremost that failures represent missing features/bugs in the InMemoryEmulator.  The tests for InMemoryEmulator could be wrong, so don't assume that because the tests pass on InMemoryEmulator that the tests are correct.  Only assume they are unsupported features if you have a documented official source proving that.  Fix the bugs/missing features, don't skip the tests to mask lack of parity with real Redis.  The goal is complete parity with real Redis.

Remember the source of truth is the official Redis docs, not the tests.
