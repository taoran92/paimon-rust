<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Fallback Branch Reading

Use `scan.fallback-branch` to read missing partitions from another branch of the
same table. This is supported for batch reads of append, primary-key, and Data
Evolution tables through the Rust API, Python binding, and SQL integration.

## Read with a Fallback Branch

For a loaded `Table`, select a fallback branch and use the same read builder for
planning and reading:

```rust
use futures::TryStreamExt;
use std::collections::HashMap;

let fallback = table.copy_with_options(HashMap::from([(
    "scan.fallback-branch".to_string(),
    "backup".to_string(),
)]));
let read_builder = fallback.new_read_builder();
let plan = read_builder.new_scan().plan().await?;
let read = read_builder.new_read()?;
let batches = read
    .to_arrow(plan.splits())?
    .try_collect::<Vec<_>>()
    .await?;
```

A blank value, the current branch name, or a nonexistent fallback branch leaves
ordinary reads unchanged. An existing fallback branch without snapshots adds
no rows to an ordinary read. Setting `scan.tag-name` requires that tag on both
branches, even when the fallback branch has no snapshots.

See the [Python example](python-binding.md#batch-reads-with-a-fallback-branch)
and [SQL configuration](sql.md#reading-with-a-fallback-branch) for other entry
points.

## Partition Precedence

The current branch supplies every partition present in its selected snapshot.
The fallback branch supplies only partitions absent from that snapshot:

```text
Current branch:  pt=10 -> [id=1]
Backup branch:   pt=10 -> [id=2]    pt=20 -> [id=3]
Batch result:    pt=10 -> [id=1]    pt=20 -> [id=3]
```

Filtering for `id=2` in this example returns no rows. Row filters, file pruning,
and `LIMIT` do not change which branch supplies a partition. Removing a partition
from the current branch can make its fallback data visible in later reads.

## Schema Compatibility

Both branches must have the same column names, order, and types, and identical
partition keys and primary keys. Schema IDs, top-level field IDs, and top-level
nullability may differ. Nested field definitions, including field IDs and
nullability, must match. Each branch uses its own schema history and bucket
layout. REST reads apply each branch's own authorization rules.

Reuse the same table handle for planning and reading to retain the fallback
read schema if the fallback branch's schema changes after planning. Serialized
or pickled splits preserve branch selection, but do not include either branch's
read schema. Loading a new table creates a new read view.

## Snapshot Selection

Snapshot IDs are local to each branch. With `scan.snapshot-id`, the fallback
branch uses its latest snapshot at or before the selected current-branch
snapshot's commit time. The same time mapping applies when the current branch
interprets `scan.version` as a snapshot ID.

!!! warning "Fallback Snapshot Availability"
    If the fallback branch has snapshots but none at or before that time, the
    read fails. Set `scan.fallback-branch.allow-earliest-snapshot` to `true` to
    allow its earliest snapshot, which may contain newer data.

As in Java, when `scan.version` selects a tag on the current branch, the
fallback branch resolves the original version value independently. For example,
suppose tag `1` points to snapshot 2 on main. Backup has snapshot 1, committed
later, and has no tag named `1`. With `scan.version=1`, the scan uses main
snapshot 2 and backup snapshot 1. With `scan.tag-name=1`, the scan fails
because the tag is missing on backup.

## Supported Operations

Fallback applies to ordinary batch scans. Streaming and incremental scans,
vector searches, and explicit or index-derived row-ID selections read only the
current branch.

Row-position slices and position shards span the combined input and require
both branches to use the same Data Evolution mode. Chunk shuffle can combine
different layouts. Both respect each branch's `row-tracking.enabled` setting.

Writers and `plan_all()` operate on the current branch. SQL `UPDATE`, `DELETE`,
and `MERGE INTO` reject any nonblank fallback branch name different from the
target's current branch, even when that branch does not exist. Clear the setting
before these operations with `SET 'paimon.scan.fallback-branch' = ''`.
Ordinary `INSERT` remains supported and writes to the current branch. See
[DML](sql.md#dml) for supported SQL mutations.
