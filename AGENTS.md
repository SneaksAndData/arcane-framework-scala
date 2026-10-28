# Agent Guidelines

## Project
- Scala 3 (`idePackagePrefix := com.sneaksanddata.arcane.framework`), ZIO-based streaming framework that writes to Iceberg. Single sbt module (`root`), not a monorepo.
- `src/main/scala/plugins/*` contains source-specific plugins (mssql, synapse, parquets3, jsons3, pullstream); `src/main/scala/services/*` and `models/*` hold the shared framework. This repo is a library consumed by downstream plugin apps (see README "Creating plugins" for the pattern), not a standalone runnable service.

## Test Writing

### Test Framework & Architecture
- Use `zio.test.ZIOSpecDefault` for unit and integration test suites.
- Apply timeouts and live clock aspects to suites (e.g. `@@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock`).

### Test Execution & Formatting
- **Always use `just` commands** rather than raw `sbt` invocations to ensure required environment variables (`test-resources/unit-tests.env`) are loaded:
  - Run all tests: `just test`
  - Run specific test: `just test-only <TestName>` (matches on substring; use a full `ClassName` or glob with `*`/spaces for an exact `testOnly` expression)
  - Check formatting: `just fmt-check`
  - Reformat code: `just fmt`
- Most suites need live backing services (MSSQL, MinIO/S3, Lakekeeper Iceberg catalog) defined in `test-resources/docker-compose.yaml`. Check `just ps` first — only run `just up --quiet-pull` if the test environment is offline (it's expensive to start and may already be running); use `just logs` / `just stop` as needed.
- CI (`.github/workflows/build.yaml`) runs: `just up` -> `just test "clean; coverage; test; coverageReport"` -> `just fmt-check`. Mirror this order locally before pushing.

### High-Value vs. Zero-Value Tests
- **Do not write zero-value tests**: Avoid tests that merely assert static constants or metadata.
- **Test real behaviors and state transitions**: Test actual effects against real catalog/table states.
- **Use real domain models for test data**: Use appropriate domain representations (e.g., `TimestampOnlyWatermark` for watermark values) rather than arbitrary placeholder strings.
- Cover both positive flows (e.g., legacy property is detected and migrated) and negative/noop flows (e.g., legacy property is absent and no unintended modifications occur).

### Provisioning Tables
- **Do not use mock or in-memory stubs** when component behavior interacts with catalog or table state.
- **Provision actual tables** in the Iceberg test catalog using `IcebergUtil` (`getSinkEntityManager`, `getSinkTablePropertyManager`).
- **Use collision-free table names**: Generate names using random UUIDs (e.g., `s"test_${UUID.randomUUID().toString.replace("-", "")}"`) to avoid collisions across test runs.
- **Always ensure cleanup**: Wrap table creation and deletion using `ZIO.acquireReleaseWith` so tables are removed even when assertions fail or timeout:
  ```scala
  ZIO.acquireReleaseWith(
    entityManager.createTable(CreateTableRequest(tableName, tableSchema, true))
  )(_ => entityManager.delete(tableName).orDie) { _ =>
    testLogic(entityManager, propertyManager, context)
  }
  ```

### Assertions
- Make assertions as strict and assertive as possible.
- When asserting that a table property exists after an operation, prefer `getRequiredProperty` (which fails if missing) over `getProperty` and assert exact value equality.
