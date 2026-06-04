# DefaultBackfillMergeGraphBuilder

## Overview

The `DefaultBackfillMergeGraphBuilder` constructs the streaming graph similar to `DefaultStreamingGraphBuilder`. However, unlike `DefaultStreamingGraphBuilder`, this graph will terminate on the first successful watermark commit.

Backfill MERGE:
1. Reads data from a sharded source, applies field filtering
2. Stages data for each shard
3. Inserts data from a staged shard into a combined backfill table
4. Once all shards have been combined, backfill combine table is swapped to target.
