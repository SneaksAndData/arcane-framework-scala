# DefaultBackfillMergeGraphBuilder

## Overview

The `DefaultBackfillMergeGraphBuilder` constructs the streaming graph similar to `DefaultStreamingGraphBuilder`. However, unlike `DefaultStreamingGraphBuilder`, it doesn't generate version pairs infinitely, but rather emits a single changeset between `backfillStartDate` and current latest source version.

Backfill MERGE:
1. Reads watermark value that matches `backfillStartDate` from stream context as `start`.
2. Read current watermark from source as `current`.
3. Emits `changes(start, current)` and processes them similar to `DefaultStreamingGraphBuilder`
  - Maintenance does not run for this changeset
  - Batch disposal does not run for this changeset
