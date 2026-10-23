# Agent Guidelines

## Test Writing

### Test Framework & Architecture
- Use `zio.test.ZIOSpecDefault` for unit and integration test suites.
- Apply timeouts and live clock aspects to suites (e.g. `@@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock`).

### Test Execution & Formatting
- **Always use `just` commands** rather than raw `sbt` invocations to ensure required environment variables (`test-resources/unit-tests.env`) are loaded:
  - Run all tests: `just test`
  - Run specific test: `just test-only <TestName>`
  - Check formatting: `just fmt-check`
  - Reformat code: `just fmt`

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
