// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Snapshot resolution for time travel, mirroring Java `TimeTravelUtil`.

use crate::api::RestError;
use crate::spec::{CoreOptions, Snapshot, TimeTravelSelector};
use crate::table::{SnapshotManager, Table, TagManager};
use crate::Error;
use std::borrow::Cow;
use std::collections::HashMap;

const WATERMARK_PREFIX: &str = "watermark-";

/// Resolve the snapshot selected by the time-travel options, if any.
///
/// Returns `Ok(None)` when no time-travel selector is configured. Returns an
/// error for invalid or conflicting selectors, and when the selector does not
/// match any snapshot — callers that need Java `tryTravelToSnapshot`'s silent
/// fallback (keep the current schema on failure) handle the `Err` themselves.
pub(crate) async fn travel_to_snapshot(
    snapshot_manager: &SnapshotManager,
    tag_manager: &TagManager,
    options: &HashMap<String, String>,
) -> crate::Result<Option<Snapshot>> {
    let (options, version_tag) = adapt_version(tag_manager, options).await?;
    let core_options = CoreOptions::new(&options);

    match core_options.try_time_travel_selector()? {
        Some(TimeTravelSelector::TimestampMillis(ts)) => {
            snapshot_at_timestamp(snapshot_manager, ts, false).await
        }
        Some(TimeTravelSelector::Watermark(w)) => {
            resolve_watermark(snapshot_manager, w).await.map(Some)
        }
        Some(TimeTravelSelector::Version { .. }) => unreachable!("scan.version was adapted above"),
        Some(TimeTravelSelector::SnapshotId {
            value: v,
            option_name,
        }) => {
            // An explicit snapshot id: parse strictly, never resolve a tag.
            match v.parse::<i64>() {
                Ok(id) => snapshot_manager.get_snapshot(id).await.map(Some),
                Err(_) => Err(Error::DataInvalid {
                    message: format!("{option_name} '{v}' is not a valid snapshot id."),
                    source: None,
                }),
            }
        }
        Some(TimeTravelSelector::TagName {
            value: v,
            option_name: _,
        }) => {
            // Tag selectors never fall back to snapshot ids. Reuse the tag
            // fetched while adapting scan.version when it selected this tag.
            match version_tag {
                Some(snapshot) => Ok(Some(snapshot)),
                None => resolve_tag(tag_manager, v).await.map(Some),
            }
        }
        None => Ok(None),
    }
}

pub(super) async fn snapshot_at_timestamp(
    manager: &SnapshotManager,
    ts: i64,
    allow_empty_branch: bool,
) -> crate::Result<Option<Snapshot>> {
    match manager.earlier_or_equal_time_millis(ts).await? {
        None if !allow_empty_branch || manager.get_latest_snapshot_id().await?.is_some() => {
            Err(Error::DataInvalid {
                message: format!("No snapshot found with timestamp <= {ts}"),
                source: None,
            })
        }
        snapshot => Ok(snapshot),
    }
}

pub(super) async fn adapt_version<'a>(
    tag_manager: &TagManager,
    options: &'a HashMap<String, String>,
) -> crate::Result<(Cow<'a, HashMap<String, String>>, Option<Snapshot>)> {
    // Java adapts scan.version before checking mutually exclusive selectors.
    // It overwrites the same selector kind, but preserves conflicts with others.
    let Some(version) = options.get("scan.version") else {
        return Ok((Cow::Borrowed(options), None));
    };
    let mut adapted = options.clone();
    adapted.remove("scan.version");
    let version_tag = resolve_version_tag(tag_manager, version).await?;
    let (key, value) = if version_tag.is_some() {
        ("scan.tag-name", version.clone())
    } else if let Some(watermark) = version.strip_prefix(WATERMARK_PREFIX) {
        let value = watermark.parse::<i64>().map_err(|e| Error::DataInvalid {
            message: format!("scan.version '{version}' has an invalid watermark value."),
            source: Some(Box::new(e)),
        })?;
        ("scan.watermark", value.to_string())
    } else if is_snapshot_id_version(version) {
        ("scan.snapshot-id", version.clone())
    } else {
        return Err(Error::DataInvalid {
            message: format!("scan.version '{version}' is not a valid tag name or snapshot id."),
            source: None,
        });
    };
    adapted.insert(key.to_string(), value);
    Ok((Cow::Owned(adapted), version_tag))
}

/// Resolve a `scan.version` tag before trying a snapshot id or watermark of
/// the same spelling. Reuse its snapshot rather than fetching the tag twice.
///
/// A numeric snapshot id or watermark can use table storage when the tag endpoint
/// returns HTTP 501. Other catalog errors and tag-only versions propagate.
async fn resolve_version_tag(
    tag_manager: &TagManager,
    version: &str,
) -> crate::Result<Option<Snapshot>> {
    match tag_manager.get(version).await {
        Err(Error::RestApi {
            source: RestError::NotImplemented { .. },
        }) if is_snapshot_or_watermark_version(version) => Ok(None),
        result => result,
    }
}

fn is_snapshot_or_watermark_version(version: &str) -> bool {
    match version.strip_prefix(WATERMARK_PREFIX) {
        Some(watermark) => watermark.parse::<i64>().is_ok(),
        None => is_snapshot_id_version(version),
    }
}

fn is_snapshot_id_version(version: &str) -> bool {
    !version.is_empty() && version.bytes().all(|b| b.is_ascii_digit())
}

async fn resolve_watermark(
    snapshot_manager: &SnapshotManager,
    watermark: i64,
) -> crate::Result<Snapshot> {
    match snapshot_manager.later_or_equal_watermark(watermark).await? {
        Some(snapshot) => Ok(snapshot),
        // Mirrors Java StaticFromWatermarkStartingScanner's error.
        None => Err(Error::DataInvalid {
            message: format!(
                "There is currently no snapshot later than or equal to watermark[{watermark}]"
            ),
            source: None,
        }),
    }
}

/// Resolve the snapshot a read should use, including the latest-snapshot fallback.
///
/// Reuses a snapshot cached by [`Table::copy_with_time_travel`] so every read path
/// observes the same snapshot/schema pair. A historical schema whose selector was
/// subsequently changed is rejected instead of mixing that stale schema with a
/// different snapshot.
pub(crate) async fn resolve_snapshot(table: &Table) -> crate::Result<Option<Snapshot>> {
    if let Some(view) = &table.travel_snapshot {
        return Ok(view.snapshot().cloned());
    }
    table.ensure_read_snapshot_current()?;

    match travel_to_snapshot(
        &table.snapshot_manager(),
        &table.tag_manager(),
        table.schema().options(),
    )
    .await?
    {
        Some(snapshot) => Ok(Some(snapshot)),
        None => table.snapshot_manager().get_latest_snapshot().await,
    }
}

/// Fetch a tag's snapshot, failing when the tag does not exist.
async fn resolve_tag(tag_manager: &TagManager, name: &str) -> crate::Result<Snapshot> {
    match tag_manager.get(name).await? {
        Some(s) => Ok(s),
        None => Err(Error::DataInvalid {
            message: format!("Tag '{name}' doesn't exist."),
            source: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{CommitKind, DataType, IntType, Schema, Snapshot, TableSchema};
    use crate::table::{SnapshotManager, Table, TagManager};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_snapshot_or_watermark_version_shape() {
        for (version, expected) in [
            ("1", true),
            ("0042", true),
            ("watermark-1000", true),
            ("watermark--5", true),
            ("", false),
            ("-1", false),
            ("release", false),
            ("watermark-", false),
            ("watermark-soon", false),
        ] {
            assert_eq!(
                super::is_snapshot_or_watermark_version(version),
                expected,
                "{version}"
            );
        }
    }

    fn schema_v0() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn schema_v1() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("age", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(1, &schema)
    }

    fn make_table(file_io: &FileIO, table_path: &str, schema: TableSchema) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "evolved"),
            table_path.to_string(),
            schema,
            None,
        )
    }

    async fn write_schema_file(file_io: &FileIO, table_path: &str, schema: &TableSchema) {
        file_io
            .mkdirs(&format!("{table_path}/schema/"))
            .await
            .unwrap();
        let path = format!("{table_path}/schema/schema-{}", schema.id());
        let content = serde_json::to_string(schema).unwrap();
        file_io
            .new_output(&path)
            .unwrap()
            .write(content.into())
            .await
            .unwrap();
    }

    fn batch_v0(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn batch_v1(ids: Vec<i32>, values: Vec<i32>, ages: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("age", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
                Arc::new(Int32Array::from(ages)),
            ],
        )
        .unwrap()
    }

    async fn write_and_commit(table: &Table, batch: &RecordBatch) {
        let builder = table
            .new_write_builder()
            .with_commit_user("test-user")
            .unwrap();
        let mut write = builder.new_write().unwrap();
        write.write_arrow_batch(batch).await.unwrap();
        let messages = write.prepare_commit().await.unwrap();
        builder
            .try_new_commit()
            .unwrap()
            .commit(messages)
            .await
            .unwrap();
    }

    async fn read_ids(table: &Table) -> Vec<i32> {
        let builder = table.new_read_builder();
        read_builder_ids(&builder).await
    }

    async fn read_builder_ids(builder: &crate::table::ReadBuilder<'_>) -> Vec<i32> {
        use futures::TryStreamExt;
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn test_fallback_branch_scan_merges_partitions() {
        check_fallback_branch("append").await;
    }

    #[tokio::test]
    async fn test_fallback_branch_primary_key() {
        check_fallback_branch("pk").await;
    }

    #[tokio::test]
    async fn test_fallback_branch_data_evolution() {
        check_fallback_branch("evolution").await;
    }

    #[tokio::test]
    async fn test_fallback_preserves_persisted_blob_descriptor_option() {
        use crate::spec::{BlobDescriptor, BlobType};
        use arrow_array::LargeBinaryArray;
        use futures::TryStreamExt;

        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-blob-descriptor";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .partition_keys(["id"])
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&io, path, &schema).await;
        crate::table::BranchManager::new(io.clone(), path.into())
            .create_branch("backup")
            .await
            .unwrap();
        let main = make_table(&io, path, schema.clone());
        let backup = main.copy_with_branch("backup").await.unwrap();
        let payload = b"actual-payload".as_slice();
        for (table, id) in [(&main, 1), (&backup, 2)] {
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(vec![id])) as arrow_array::ArrayRef,
                ),
                ("payload", Arc::new(LargeBinaryArray::from(vec![payload]))),
            ])
            .unwrap();
            write_and_commit(table, &batch).await;
        }

        let default = main.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        // Reload a persisted read option, so it is not an applied dynamic override.
        let schema = schema.copy_with_options(options(&[("blob-as-descriptor", "true")]));
        write_schema_file(&io, path, &schema).await;
        let main = make_table(&io, path, schema.clone());
        let fallback = main.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        for (table, descriptors) in [
            (fallback.clone(), true),
            (
                fallback.copy_with_options(options(&[("blob-as-descriptor", "false")])),
                false,
            ),
        ] {
            let builder = table.new_read_builder();
            let plan = builder.new_scan().plan().await.unwrap();
            assert!(plan.splits().iter().any(crate::DataSplit::is_fallback));
            let batches = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut ids = Vec::new();
            for batch in batches {
                let batch_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    ids.push(batch_ids.value(row));
                    if descriptors {
                        let descriptor = BlobDescriptor::deserialize(values.value(row)).unwrap();
                        assert_eq!(descriptor.length(), payload.len() as i64);
                    } else {
                        assert_eq!(values.value(row), payload);
                    }
                }
            }
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2]);
        }

        // Reverse the stored defaults: a payload filter must match both branches.
        write_schema_file(&io, &format!("{path}/branch/branch-backup"), &schema).await;
        let mut builder = default.new_read_builder();
        builder.with_projection(&["id"]).unwrap().with_filter(
            crate::spec::PredicateBuilder::new(default.schema().fields())
                .equal("payload", crate::spec::Datum::Bytes(payload.to_vec()))
                .unwrap(),
        );
        assert_eq!(read_builder_ids(&builder).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_fallback_branch_row_ids_remain_branch_local() {
        use crate::spec::{row_id_leaf, Datum, PredicateOperator};
        use crate::table::RowRange;
        use crate::vector_search::GlobalIndexResult;

        let table = fallback_with_layout("evolution", "evolution").await;
        assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5]);
        let builder = table.new_read_builder();
        let plans = [
            builder
                .new_scan()
                .with_row_ranges(vec![RowRange::new(0, 0)])
                .plan()
                .await
                .unwrap(),
            builder
                .new_scan()
                .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![RowRange::new(
                    0, 0,
                )]))
                .unwrap()
                .plan()
                .await
                .unwrap(),
        ];
        for plan in plans {
            assert_eq!(read_transported_ids(&table, plan.splits()).await, vec![1]);
        }
        let mut filtered = table.new_read_builder();
        for (op, value, expected) in [
            (PredicateOperator::Eq, 0, vec![1, 3]),
            (PredicateOperator::GtEq, 1, vec![2, 4, 5]),
            (PredicateOperator::Lt, 0, vec![]),
        ] {
            filtered.with_filter(row_id_leaf(op, vec![Datum::Long(value)]));
            assert_eq!(read_builder_ids(&filtered).await, expected);
        }
        filtered.with_filter(row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(1)]));
        let explicit = filtered
            .new_scan()
            .with_row_ranges(vec![RowRange::new(1, 1)])
            .plan()
            .await
            .unwrap();
        assert_eq!(
            read_transported_ids(&table, explicit.splits()).await,
            vec![2]
        );
        filtered.with_row_ranges(vec![RowRange::new(0, 1)]);
        assert_eq!(read_builder_ids(&filtered).await, vec![2]);
        filtered.with_row_ranges(vec![]);
        assert!(read_builder_ids(&filtered).await.is_empty());
        // Backup's id=3 also has row ID 0, but must not admit main's id=1.
        let filter = crate::spec::PredicateBuilder::new(table.schema().fields())
            .equal("id", crate::spec::Datum::Int(3))
            .unwrap();
        let prepared = table.prepare_vector_search_filter(filter).await.unwrap();
        assert!(prepared.include_row_ids().is_empty());
    }

    async fn fallback_with_layout(main_mode: &str, backup_mode: &str) -> Table {
        let layout_options = |mode: &str| {
            HashMap::from([
                (
                    "row-tracking.enabled".into(),
                    (mode != "append").to_string(),
                ),
                (
                    "data-evolution.enabled".into(),
                    (mode == "evolution").to_string(),
                ),
            ])
        };
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-row-tracking";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .option("row-tracking.enabled", (main_mode != "append").to_string())
            .option(
                "data-evolution.enabled",
                (main_mode == "evolution").to_string(),
            )
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let main = make_table(&file_io, path, schema.clone());
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch("backup")
            .await
            .unwrap();
        // An empty branch can choose its layout before writing its first snapshot.
        let backup_schema = schema.copy_with_options(layout_options(backup_mode));
        write_schema_file(
            &file_io,
            &format!("{path}/branch/branch-backup"),
            &backup_schema,
        )
        .await;
        let backup = main.copy_with_branch("backup").await.unwrap();
        write_and_commit(&backup, &batch_v0(vec![3], vec![20])).await;
        // This file starts at row ID 1 on a tracked branch, rather than local offset 0.
        write_and_commit(&backup, &batch_v0(vec![4, 5], vec![30, 30])).await;
        write_and_commit(&main, &batch_v0(vec![1, 2], vec![10, 10])).await;
        main.copy_with_options(options(&[("scan.fallback-branch", "backup")]))
    }

    async fn read_transported_ids(table: &Table, splits: &[crate::DataSplit]) -> Vec<i32> {
        use futures::TryStreamExt;

        let splits: Vec<_> = splits
            .iter()
            .map(|split| {
                crate::DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap())
                    .unwrap()
            })
            .collect();
        let batches: Vec<RecordBatch> = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(&splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ids: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn test_fallback_row_id_filter_uses_branch_layout() {
        use crate::spec::{row_id_leaf, Datum, PredicateOperator};
        for (main, backup, expected) in [
            ("evolution", "append", vec![1]),
            ("append", "evolution", vec![3]),
        ] {
            let table = fallback_with_layout(main, backup).await;
            let mut builder = table.new_read_builder();
            builder.with_filter(row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(0)]));
            assert_eq!(read_builder_ids(&builder).await, expected);
        }
    }

    #[tokio::test]
    async fn test_fallback_row_tracking_slices_and_position_shards() {
        for (main_mode, backup_mode) in [("append", "tracked"), ("tracked", "append")] {
            let table = fallback_with_layout(main_mode, backup_mode).await;
            assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5]);
            let builder = table.new_read_builder();
            for (start, end, expected) in [
                (0, 1, vec![1]),
                (1, 4, vec![2, 3, 4]),
                (4, 5, vec![5]),
                (5, 6, vec![]),
            ] {
                let plan = builder
                    .new_scan()
                    .with_row_position_slice(start, end)
                    .unwrap()
                    .plan()
                    .await
                    .unwrap();
                assert_eq!(read_transported_ids(&table, plan.splits()).await, expected);
            }
            for (index, expected) in [vec![1, 2], vec![3, 4], vec![5]].into_iter().enumerate() {
                let plan = builder
                    .new_scan()
                    .with_row_position_shard(index as u64, 3)
                    .unwrap()
                    .plan()
                    .await
                    .unwrap();
                assert_eq!(read_transported_ids(&table, plan.splits()).await, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_fallback_row_tracking_chunk_shuffle() {
        for chunk_size in [1, 2] {
            let mut reference = None;
            for (main_mode, backup_mode) in [
                ("append", "append"),
                ("append", "tracked"),
                ("tracked", "append"),
                ("append", "evolution"),
                ("evolution", "append"),
            ] {
                let table = fallback_with_layout(main_mode, backup_mode).await;
                let builder = table.new_read_builder();
                let scan = builder
                    .new_scan()
                    .with_chunk_shuffle(42, chunk_size)
                    .unwrap();
                let plan = scan.plan().await.unwrap();
                let mut chunks = Vec::new();
                for split in plan.splits() {
                    chunks.push(read_transported_ids(&table, std::slice::from_ref(split)).await);
                }
                let mut ids: Vec<_> = chunks.iter().flatten().copied().collect();
                ids.sort_unstable();
                assert_eq!(ids, vec![1, 2, 3, 4, 5]);
                // Branch layout must not change the one global shuffle's chunk order.
                if let Some(reference) = &reference {
                    assert_eq!(&chunks, reference);
                } else {
                    reference = Some(chunks.clone());
                }
                let mut sharded_chunks = Vec::new();
                for index in 0..3 {
                    let plan = scan
                        .clone()
                        .with_shard(index, 3)
                        .unwrap()
                        .plan()
                        .await
                        .unwrap();
                    for split in plan.splits() {
                        sharded_chunks
                            .push(read_transported_ids(&table, std::slice::from_ref(split)).await);
                    }
                }
                assert_eq!(sharded_chunks, chunks);
            }
        }
    }

    #[tokio::test]
    async fn test_fallback_row_tracking_mixed_evolution_positions_are_rejected() {
        for (main_mode, backup_mode) in [("append", "evolution"), ("evolution", "append")] {
            let table = fallback_with_layout(main_mode, backup_mode).await;
            // Ordinary reads remain valid; only incompatible position domains are refused.
            assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5]);
            for scan in [
                table
                    .new_read_builder()
                    .new_scan()
                    .with_row_position_slice(1, 4)
                    .unwrap(),
                table
                    .new_read_builder()
                    .new_scan()
                    .with_row_position_shard(1, 3)
                    .unwrap(),
            ] {
                assert!(matches!(
                    scan.plan().await,
                    Err(crate::Error::Unsupported { message })
                        if message.contains("same data-evolution mode")
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_fallback_plan_reads_after_snapshot_metadata_expires() {
        use futures::TryStreamExt;

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-expired-snapshot";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema);
        crate::table::BranchManager::new(file_io, path.into())
            .create_branch("backup")
            .await
            .unwrap();
        let backup = table.copy_with_branch("backup").await.unwrap();
        write_and_commit(&table, &batch_v0(vec![1], vec![10])).await;
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;

        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        let builder = fallback.new_read_builder();
        let reader = builder.new_read().unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let transported: Vec<_> = plan
            .splits()
            .iter()
            .map(|split| {
                crate::DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap())
                    .unwrap()
            })
            .collect();

        // The next append keeps the old data file live. No tag retains snapshot 1.
        write_and_commit(&backup, &batch_v0(vec![3], vec![30])).await;
        backup.snapshot_manager().delete_snapshot(1).await.unwrap();
        backup
            .snapshot_manager()
            .write_earliest_hint(2)
            .await
            .unwrap();
        assert_eq!(read_ids(&backup).await, vec![2, 3]);

        for splits in [plan.splits(), transported.as_slice()] {
            let batches: Vec<RecordBatch> = reader
                .to_arrow(splits)
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut ids: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2]);
        }
        assert_eq!(read_ids(&fallback).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_fallback_scan_stats_include_both_branches() {
        for evolution in [false, true] {
            check_fallback_scan_stats(evolution).await;
        }
    }

    async fn check_fallback_scan_stats(evolution: bool) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-stats";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .option(
                "data-evolution.enabled",
                if evolution { "true" } else { "false" },
            )
            .option(
                "row-tracking.enabled",
                if evolution { "true" } else { "false" },
            )
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let main = make_table(&file_io, path, schema);
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch("backup")
            .await
            .unwrap();
        let backup = main.copy_with_branch("backup").await.unwrap();
        write_and_commit(&main, &batch_v0(vec![1], vec![10])).await;
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        let fallback = main.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        for filter_column in [None, Some("value"), Some("id")] {
            let mut builder = fallback.new_read_builder();
            if let Some(column) = filter_column {
                builder.with_filter(
                    crate::spec::PredicateBuilder::new(fallback.schema().fields())
                        .equal(
                            column,
                            crate::spec::Datum::Int(if column == "value" { 10 } else { 1 }),
                        )
                        .unwrap(),
                );
            }
            let plan = builder.new_scan().plan().await.unwrap();
            let stats = plan.stats();
            let partition_filtered = filter_column == Some("value");
            let data_filtered = filter_column == Some("id");
            let files = if filter_column.is_some() { 1 } else { 2 };
            assert_eq!(stats.base_manifest_files, 0, "{stats:?}");
            // A partition present only on main requires no second backup planning pass.
            let scanned = if partition_filtered { 1 } else { 2 };
            assert_eq!(stats.delta_manifest_files, scanned, "{stats:?}");
            assert_eq!(
                stats.manifest_files_before_partition_pruning, scanned,
                "{stats:?}"
            );
            assert_eq!(
                stats.manifest_files_after_partition_pruning, scanned,
                "{stats:?}"
            );
            assert_eq!(stats.manifest_entries_read, scanned, "{stats:?}");
            assert_eq!(stats.manifest_entries_pruned_by_partition, 0, "{stats:?}");
            assert_eq!(
                stats.manifest_entries_pruned_by_data_stats,
                usize::from(data_filtered && !evolution),
                "{stats:?}"
            );
            let entries = if partition_filtered { 1 } else { 2 };
            assert_eq!(
                stats.manifest_entries_after_entry_pruning, entries,
                "{stats:?}"
            );
            let after_stats = if evolution { entries } else { files };
            assert_eq!(
                stats.manifest_entries_after_manifest_filters, after_stats,
                "{stats:?}"
            );
            assert_eq!(stats.manifest_entries_after_merge, after_stats, "{stats:?}");
            assert_eq!(
                stats.manifest_entries_after_cross_schema_stats, after_stats,
                "{stats:?}"
            );
            assert_eq!(
                stats.data_evolution_groups_before_stats,
                if evolution { entries } else { 0 },
                "{stats:?}"
            );
            assert_eq!(
                stats.data_evolution_groups_pruned_by_stats,
                usize::from(data_filtered && evolution),
                "{stats:?}"
            );
            assert_eq!(stats.split_candidates_built, files, "{stats:?}");
            assert_eq!(stats.final_files, files, "{stats:?}");
            assert_eq!(stats.final_splits, plan.splits().len());
            assert_eq!(
                stats.planned_data_file_bytes,
                plan.splits()
                    .iter()
                    .flat_map(|split| split.data_files())
                    .map(|file| file.file_size as u64)
                    .sum::<u64>()
            );
        }
        let sliced = fallback
            .new_read_builder()
            .new_scan()
            .with_row_position_slice(1, 2)
            .unwrap()
            .plan()
            .await
            .unwrap();
        assert_eq!(sliced.stats().final_splits, 1);
        assert_eq!(sliced.stats().splits_after_limit, 1);
        assert_eq!(sliced.stats().final_files, 1);
        assert!(sliced.splits()[0].is_fallback());
        assert_eq!(
            sliced.stats().planned_data_file_bytes,
            sliced.splits()[0].data_files()[0].file_size as u64
        );
    }

    async fn check_fallback_branch(mode: &str) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-scan";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"]);
        let schema = match mode {
            "pk" => schema.primary_key(["id", "value"]).option("bucket", "1"),
            "evolution" => schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("bucket", "-1"),
            _ => schema,
        }
        .build()
        .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema);

        // Create backup from a tag to retain partition 20 before overwriting main.
        write_and_commit(&table, &batch_v0(vec![2, 3], vec![20, 10])).await;
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        table
            .tag_manager()
            .create("before", &snapshot)
            .await
            .unwrap();
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch_from_tag("backup", "before")
            .await
            .unwrap();
        let writer = table.new_write_builder().with_overwrite();
        let mut write = writer.new_write().unwrap();
        write
            .write_arrow_batch(&batch_v0(vec![1], vec![10]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        writer
            .new_commit()
            .overwrite(messages, Some(HashMap::new()))
            .await
            .unwrap();
        let backup = table.copy_with_branch("backup").await.unwrap();
        assert_eq!(read_ids(&table).await, vec![1]);
        assert_eq!(read_ids(&backup).await, vec![2, 3]);

        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        assert_eq!(read_ids(&fallback).await, vec![1, 2]);
        let neutral = fallback
            .new_read_builder()
            .new_scan()
            .with_file_filters(false, false)
            .unwrap()
            .plan()
            .await
            .unwrap();
        assert_eq!(
            read_transported_ids(&fallback, neutral.splits()).await,
            vec![1, 2]
        );
        let dynamic = table
            .copy_with_time_travel(options(&[("scan.fallback-branch", "backup")]))
            .await
            .unwrap();
        assert_eq!(read_ids(&dynamic).await, vec![1, 2]);

        // A data predicate must not revive backup's shadowed partition 10.
        for (id, expected) in [(2, vec![2]), (3, vec![])] {
            let mut builder = fallback.new_read_builder();
            builder.with_filter(
                crate::spec::PredicateBuilder::new(fallback.schema().fields())
                    .equal("id", crate::spec::Datum::Int(id))
                    .unwrap(),
            );
            builder.with_projection(&["id"]).unwrap();
            assert_eq!(read_builder_ids(&builder).await, expected);
        }
        let mut limited = fallback.new_read_builder();
        limited.with_limit(1);
        assert_eq!(read_builder_ids(&limited).await.len(), 1);

        let builder = fallback.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        assert!(plan.splits().iter().any(crate::DataSplit::is_fallback));
        let splits: Vec<_> = plan
            .splits()
            .iter()
            .map(|split| {
                let bytes = split.serialize_split_v1().unwrap();
                let restored = crate::DataSplit::deserialize_split_v1(&bytes).unwrap();
                assert_eq!(&restored, split);
                restored
            })
            .collect();
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(&splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        // Read-side partition pruning may retain only main, only backup, or
        // neither branch of an already planned/transported split set.
        for (partition, expected) in [(10, 1), (20, 1), (99, 0)] {
            let predicate = crate::spec::PredicateBuilder::new(fallback.schema().fields())
                .equal("value", crate::spec::Datum::Int(partition))
                .unwrap();
            let rows: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .with_filter(predicate)
                .to_arrow(&splits)
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected
            );
        }

        if mode != "pk" {
            let slice = builder
                .new_scan()
                .with_row_position_slice(1, 2)
                .unwrap()
                .plan()
                .await
                .unwrap();
            let rows: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(slice.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert!(slice.splits().iter().all(crate::DataSplit::is_fallback));
            let chunks = builder
                .new_scan()
                .with_chunk_shuffle(42, 1)
                .unwrap()
                .plan()
                .await
                .unwrap();
            assert_eq!(chunks.splits().len(), 2);
            // Rebuilding chunks must retain branch routing even with identical schemas.
            assert_eq!(
                chunks
                    .splits()
                    .iter()
                    .filter(|split| split.is_fallback())
                    .count(),
                1
            );
            let rows: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(chunks.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        }

        if mode == "evolution" {
            let mut filtered = fallback.new_read_builder();
            filtered.with_filter(
                crate::spec::PredicateBuilder::new(fallback.schema().fields())
                    .equal("id", crate::spec::Datum::Int(2))
                    .unwrap(),
            );
            let slice = filtered
                .new_scan()
                .with_row_position_slice(1, 2)
                .unwrap()
                .plan()
                .await
                .unwrap();
            let rows: Vec<RecordBatch> = filtered
                .new_read()
                .unwrap()
                .to_arrow(slice.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        }

        // File/mutation scans must remain scoped to the writable branch.
        let all = builder.new_scan().plan_all().await.unwrap();
        assert!(all.splits().iter().all(|split| !split.is_fallback()));
        assert_eq!(
            all.splits()
                .iter()
                .map(|split| split
                    .data_files()
                    .iter()
                    .map(|file| file.row_count)
                    .sum::<i64>())
                .sum::<i64>(),
            1
        );

        // An empty current branch still reads every fallback partition.
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch("empty")
            .await
            .unwrap();
        let empty = table
            .copy_with_branch("empty")
            .await
            .unwrap()
            .copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        assert_eq!(read_ids(&empty).await, vec![2, 3]);
        let empty_fallback =
            fallback.copy_with_options(options(&[("scan.fallback-branch", "empty")]));
        assert_eq!(read_ids(&empty_fallback).await, vec![1]);
        let reverse = backup.copy_with_options(options(&[("scan.fallback-branch", "main")]));
        assert_eq!(read_ids(&reverse).await, vec![2, 3]);
        let missing = table.copy_with_options(options(&[("scan.fallback-branch", "missing")]));
        assert_eq!(read_ids(&missing).await, vec![1]);

        // A fallback equal to the resolved branch does not require another scan.
        let same_branch = backup.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        assert_eq!(read_ids(&same_branch).await, vec![2, 3]);
        for value in ["", " \t ", "main"] {
            let disabled = fallback
                .copy_with_time_travel(options(&[("scan.fallback-branch", value)]))
                .await
                .unwrap();
            assert_eq!(read_ids(&disabled).await, vec![1]);
        }

        // A transported plan remains readable if its tag is removed while the
        // snapshot exists, or if the snapshot expires while its tag is retained.
        let latest = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        table
            .tag_manager()
            .create("retained", &latest)
            .await
            .unwrap();
        backup
            .tag_manager()
            .create("retained", &snapshot)
            .await
            .unwrap();
        assert!(backup
            .snapshot_manager()
            .commit_snapshot(&latest)
            .await
            .unwrap());
        let opts = options(&[
            ("scan.fallback-branch", "backup"),
            ("scan.tag-name", "retained"),
        ]);
        let tagged = table
            .copy_with_time_travel_strict(opts.clone())
            .await
            .unwrap();
        let plan = tagged.new_read_builder().new_scan().plan().await.unwrap();
        let splits: Vec<_> = plan
            .splits()
            .iter()
            .map(|split| {
                crate::DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap())
                    .unwrap()
            })
            .collect();
        for expire_snapshot in [false, true] {
            if expire_snapshot {
                backup
                    .tag_manager()
                    .create("retained", &snapshot)
                    .await
                    .unwrap();
                backup
                    .snapshot_manager()
                    .delete_snapshot(snapshot.id())
                    .await
                    .unwrap();
                backup
                    .snapshot_manager()
                    .write_earliest_hint(latest.id())
                    .await
                    .unwrap();
            } else {
                backup.tag_manager().delete("retained").await.unwrap();
            }
            let fresh = table
                .copy_with_time_travel_strict(opts.clone())
                .await
                .unwrap();
            let rows: Vec<RecordBatch> = fresh
                .new_read_builder()
                .new_read()
                .unwrap()
                .to_arrow(&splits)
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        }
        assert_eq!(read_ids(&tagged).await, vec![1, 2]);
    }

    async fn setup_fallback_sequence_table() -> (Table, Table) {
        use arrow_array::{ArrayRef, Int64Array};
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-sequence";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("pt", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .option("bucket", "-1")
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema);
        let batch = |id, value, pt| {
            RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
                ("pt", Arc::new(Int32Array::from(vec![pt])) as ArrayRef),
            ])
            .unwrap()
        };
        write_and_commit(&table, &batch(2, 10, 20)).await;
        // The newer file updates only value. Reading id still needs that file
        // when _SEQUENCE_NUMBER is projected or used only by a predicate.
        let update = RecordBatch::try_from_iter(vec![
            ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("value", Arc::new(Int32Array::from(vec![20])) as ArrayRef),
        ])
        .unwrap();
        let writer = table.new_write_builder();
        let messages = writer
            .new_update()
            .unwrap()
            .update_by_arrow_with_row_id(vec![update])
            .await
            .unwrap();
        writer.new_commit().commit(messages).await.unwrap();
        let latest = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.id(), 2);
        table
            .tag_manager()
            .create("updated", &latest)
            .await
            .unwrap();
        crate::table::BranchManager::new(file_io, path.into())
            .create_branch_from_tag("backup", "updated")
            .await
            .unwrap();
        let backup = table.copy_with_branch("backup").await.unwrap();
        let overwrite = table.new_write_builder().with_overwrite();
        let mut write = overwrite.new_write().unwrap();
        write.write_arrow_batch(&batch(1, 30, 10)).await.unwrap();
        overwrite
            .new_commit()
            .overwrite(write.prepare_commit().await.unwrap(), Some(HashMap::new()))
            .await
            .unwrap();
        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        (backup, fallback)
    }

    async fn read_fallback_sequence_rows(
        table: &Table,
        predicate_only: bool,
    ) -> Vec<(i32, Option<i64>)> {
        use arrow_array::Int64Array;
        use futures::TryStreamExt;
        let mut builder = table.new_read_builder();
        if predicate_only {
            builder.with_projection(&["id"]).unwrap();
            builder.with_filter(
                crate::spec::PredicateBuilder::new(&table.schema().fields_with_read_metadata())
                    .equal("_SEQUENCE_NUMBER", crate::spec::Datum::Long(2))
                    .unwrap(),
            );
        } else {
            builder
                .with_projection(&["id", "_SEQUENCE_NUMBER"])
                .unwrap();
        }
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let seq = batch
                .column_by_name("_SEQUENCE_NUMBER")
                .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap());
            rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), seq.map(|seq| seq.value(i)))));
        }
        rows.sort_unstable();
        rows
    }

    #[tokio::test]
    async fn test_fallback_data_evolution_sequence_projection() {
        let (backup, fallback) = setup_fallback_sequence_table().await;
        let direct = read_fallback_sequence_rows(&backup, false).await;
        let actual = read_fallback_sequence_rows(&fallback, false).await;
        assert_eq!(direct, vec![(2, Some(2))]);
        assert_eq!(actual, vec![(1, Some(3)), (2, Some(2))]);
    }

    #[tokio::test]
    async fn test_fallback_data_evolution_sequence_predicate_only() {
        let (backup, fallback) = setup_fallback_sequence_table().await;
        let direct = read_fallback_sequence_rows(&backup, true).await;
        let actual = read_fallback_sequence_rows(&fallback, true).await;
        assert_eq!(direct, vec![(2, None)]);
        assert_eq!(actual, direct);
    }

    #[tokio::test]
    async fn test_fallback_plan_preserves_schema_after_readding_column() {
        check_fallback_plan_schema_change(true).await;
    }

    #[tokio::test]
    async fn test_fallback_plan_preserves_schema_after_adding_column() {
        check_fallback_plan_schema_change(false).await;
    }

    async fn setup_fallback_schema_evolution() -> (Table, Snapshot) {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-pinned-schema";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .build()
            .unwrap();
        write_schema_file(&io, path, &TableSchema::new(0, &schema)).await;
        let table = make_table(&io, path, TableSchema::new(0, &schema));
        crate::table::BranchManager::new(io.clone(), path.into())
            .create_branch("backup")
            .await
            .unwrap();
        let backup = table.copy_with_branch("backup").await.unwrap();
        // Snapshot-id fallback selects a backup snapshot no later than main's.
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        write_and_commit(&table, &batch_v0(vec![1], vec![10])).await;
        let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();

        // Both branches gain a nullable column without a new data snapshot.
        let evolved = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("extra", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .build()
            .unwrap();
        let evolved = TableSchema::new(1, &evolved);
        write_schema_file(&io, path, &evolved).await;
        write_schema_file(&io, &format!("{path}/branch/branch-backup"), &evolved).await;
        (make_table(&io, path, evolved), snapshot)
    }

    async fn check_fallback_schema_rows(
        table: &Table,
        splits: &[crate::DataSplit],
        expected_rows: &[(i32, i32)],
        with_extra: bool,
    ) {
        use arrow_array::Array;
        use futures::TryStreamExt;

        let batches: Vec<RecordBatch> = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(batch.num_columns(), if with_extra { 3 } else { 2 });
            if with_extra {
                let extra = batch.column_by_name("extra").unwrap();
                assert_eq!(extra.null_count(), batch.num_rows());
            }
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
        }
        rows.sort_unstable();
        assert_eq!(rows, expected_rows);
    }

    #[tokio::test]
    async fn test_fallback_snapshot_options_preserve_read_schema() {
        let (table, snapshot) = setup_fallback_schema_evolution().await;
        let snapshot_id = snapshot.id().to_string();
        for selector in ["scan.snapshot-id", "scan.version"] {
            let fallback = table.copy_with_options(options(&[
                ("scan.fallback-branch", "backup"),
                (selector, &snapshot_id),
            ]));
            // Selecting old data without time travel keeps both branches' current
            // read schemas, including the nullable column added after the snapshot.
            let plan = fallback.new_read_builder().new_scan().plan().await.unwrap();
            check_fallback_schema_rows(&fallback, plan.splits(), &[(1, 10), (2, 20)], true).await;

            // Explicit time travel still resolves historical schemas on both sides.
            let historical = fallback
                .copy_with_time_travel_strict(HashMap::new())
                .await
                .unwrap();
            let plan = historical
                .new_read_builder()
                .new_scan()
                .plan()
                .await
                .unwrap();
            check_fallback_schema_rows(&historical, plan.splits(), &[(1, 10), (2, 20)], false)
                .await;
        }
    }

    #[tokio::test]
    async fn test_fallback_reader_preserves_transported_schema() {
        let (table, snapshot) = setup_fallback_schema_evolution().await;
        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        // Plan before selecting or pinning a snapshot to exercise reader-side
        // schema selection independently of the planner.
        let plan = fallback.new_read_builder().new_scan().plan().await.unwrap();
        let transported: Vec<_> = plan
            .splits()
            .iter()
            .map(|split| {
                crate::DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap())
                    .unwrap()
            })
            .collect();
        for selected in [
            fallback
                .copy_with_options(options(&[("scan.snapshot-id", &snapshot.id().to_string())])),
            fallback.copy_with_pinned_snapshot(Some(&snapshot)),
        ] {
            check_fallback_schema_rows(&selected, &transported, &[(1, 10), (2, 20)], true).await;
        }
    }

    #[tokio::test]
    async fn test_fallback_pinned_snapshot_preserves_schema() {
        let (table, snapshot) = setup_fallback_schema_evolution().await;
        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        let pinned = fallback.copy_with_pinned_snapshot(Some(&snapshot));
        let historical = fallback
            .copy_with_resolved_snapshot(&snapshot)
            .await
            .unwrap();

        for (view, expected_rows, with_extra) in [
            (fallback, vec![(1, 10), (2, 20)], true),
            (
                table.copy_with_pinned_snapshot(Some(&snapshot)),
                vec![(1, 10)],
                true,
            ),
            (historical.clone(), vec![(1, 10), (2, 20)], false),
            // Internal re-pinning must retain an already historical schema policy.
            (
                historical.copy_with_pinned_snapshot(Some(&snapshot)),
                vec![(1, 10), (2, 20)],
                false,
            ),
            // Unrelated option copies must retain the preserve-schema policy.
            (
                pinned.copy_with_options(options(&[("read.batch-size", "1")])),
                vec![(1, 10), (2, 20)],
                true,
            ),
            // Explicit time travel supersedes the earlier preserve-schema pin.
            (
                pinned
                    .copy_with_time_travel_strict(HashMap::new())
                    .await
                    .unwrap(),
                vec![(1, 10), (2, 20)],
                false,
            ),
        ] {
            let plan = view.new_read_builder().new_scan().plan().await.unwrap();
            check_fallback_schema_rows(&view, plan.splits(), &expected_rows, with_extra).await;
        }
    }

    async fn check_fallback_plan_schema_change(readd: bool) {
        use futures::TryStreamExt;
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-schema-change";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema.clone());
        write_and_commit(&table, &batch_v0(vec![2], vec![20])).await;
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        table
            .tag_manager()
            .create("before", &snapshot)
            .await
            .unwrap();
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch_from_tag("backup", "before")
            .await
            .unwrap();
        let writer = table.new_write_builder().with_overwrite();
        let mut write = writer.new_write().unwrap();
        write
            .write_arrow_batch(&batch_v0(vec![1], vec![10]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        writer
            .new_commit()
            .overwrite(messages, Some(HashMap::new()))
            .await
            .unwrap();

        let fallback = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        let builder = fallback.new_read_builder();
        // Construct the reader even before planning: both must retain one read schema.
        let reader = builder.new_read().unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let serialized: Vec<_> = plan
            .splits()
            .iter()
            .map(|split| {
                crate::DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap())
                    .unwrap()
            })
            .collect();
        let backup = table.copy_with_branch("backup").await.unwrap();
        let backup_builder = backup.new_read_builder();
        let backup_plan = backup_builder.new_scan().plan().await.unwrap();
        let backup_reader = backup_builder.new_read().unwrap();

        let mut changed = serde_json::to_value(&schema).unwrap();
        changed["id"] = serde_json::json!(1);
        changed["highestFieldId"] = serde_json::json!(2);
        if readd {
            changed["fields"][0]["id"] = serde_json::json!(2);
        } else {
            let mut field = changed["fields"][0].clone();
            field["id"] = serde_json::json!(2);
            field["name"] = serde_json::json!("extra");
            changed["fields"].as_array_mut().unwrap().push(field);
        }
        let changed: TableSchema = serde_json::from_value(changed).unwrap();
        write_schema_file(&file_io, &format!("{path}/branch/branch-backup"), &changed).await;
        assert_eq!(
            table
                .copy_with_branch("backup")
                .await
                .unwrap()
                .schema()
                .id(),
            1
        );

        let direct: Vec<RecordBatch> = backup_reader
            .to_arrow(backup_plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            direct[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        for splits in [plan.splits(), serialized.as_slice()] {
            for read in [&reader, &builder.new_read().unwrap()] {
                let batches: Vec<RecordBatch> =
                    read.to_arrow(splits).unwrap().try_collect().await.unwrap();
                let mut ids: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                            .collect::<Vec<_>>()
                    })
                    .collect();
                ids.sort_unstable();
                assert_eq!(ids, vec![Some(1), Some(2)]);
            }
        }
        // Ordinary option copies preserve the captured schema, while a newly
        // loaded read view sees DDL even without a new data snapshot.
        let copied = fallback.copy_with_options(options(&[("read.batch-size", "1")]));
        assert_eq!(read_ids(&copied).await, vec![1, 2]);
        let fresh = table.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        if readd {
            let fresh_builder = fresh.new_read_builder();
            let plan = fresh_builder.new_scan().plan().await.unwrap();
            let batches: Vec<RecordBatch> = fresh_builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut ids: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>()
                })
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, vec![None, Some(1)]);
        } else {
            assert!(fresh.new_read_builder().new_scan().plan().await.is_err());
        }
    }

    #[tokio::test]
    async fn test_fallback_branch_uses_its_own_schema_ids() {
        use futures::TryStreamExt;
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-schema-ids";
        let mut json = serde_json::to_value(schema_v1()).unwrap();
        json["partitionKeys"] = serde_json::json!(["value"]);
        let schema: TableSchema = serde_json::from_value(json.clone()).unwrap();
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema);
        write_and_commit(&table, &batch_v1(vec![2], vec![20], vec![222])).await;
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        table
            .tag_manager()
            .create("before", &snapshot)
            .await
            .unwrap();
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch_from_tag("backup", "before")
            .await
            .unwrap();

        // Both branches have schema-1, but age was re-added independently on
        // main and has a different field ID. Schema IDs are branch-local.
        json["fields"][2]["id"] = serde_json::json!(3);
        json["highestFieldId"] = serde_json::json!(3);
        let schema: TableSchema = serde_json::from_value(json).unwrap();
        write_schema_file(&file_io, path, &schema).await;
        let main = make_table(&file_io, path, schema);
        let writer = main.new_write_builder().with_overwrite();
        let mut write = writer.new_write().unwrap();
        write
            .write_arrow_batch(&batch_v1(vec![1], vec![10], vec![111]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        writer
            .new_commit()
            .overwrite(messages, Some(HashMap::new()))
            .await
            .unwrap();
        let fallback = main.copy_with_options(options(&[("scan.fallback-branch", "backup")]));
        let mut builder = fallback.new_read_builder();
        builder.with_projection(&["age"]).unwrap();
        builder.with_filter(
            crate::spec::PredicateBuilder::new(fallback.schema().fields())
                .equal("age", crate::spec::Datum::Int(222))
                .unwrap(),
        );
        let plan = builder.new_scan().plan().await.unwrap();
        assert_eq!(plan.splits().len(), 1);
        assert!(plan.splits()[0].is_fallback());
        let split =
            crate::DataSplit::deserialize_split_v1(&plan.splits()[0].serialize_split_v1().unwrap())
                .unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(&[split])
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ages: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ages, vec![Some(222)]);
    }

    async fn empty_fallback_tables() -> (Table, Table) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/fallback-times";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["value"])
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        write_schema_file(&file_io, path, &schema).await;
        let table = make_table(&file_io, path, schema);
        crate::table::BranchManager::new(file_io.clone(), path.into())
            .create_branch("backup")
            .await
            .unwrap();
        let backup = table.copy_with_branch("backup").await.unwrap();
        (
            table.copy_with_options(options(&[("scan.fallback-branch", "backup")])),
            backup,
        )
    }

    async fn fallback_time_tables() -> (Table, Table) {
        let (table, backup) = empty_fallback_tables().await;
        write_and_commit(&table, &batch_v0(vec![1], vec![10])).await;
        (table, backup)
    }

    #[tokio::test]
    async fn test_fallback_explicit_empty_pin_before_first_plan() {
        let (latest, backup) = empty_fallback_tables().await;
        let planned_early = latest.copy_with_pinned_snapshot(None);
        let planned_late = latest.copy_with_pinned_snapshot(None);
        assert!(read_ids(&planned_early.clone()).await.is_empty());
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;

        assert_eq!(read_ids(&latest).await, vec![2]);
        for view in [
            planned_early,
            planned_late.copy_with_options(options(&[("read.batch-size", "1")])),
            planned_late,
        ] {
            assert_eq!(read_ids(&view).await, Vec::<i32>::new());
        }
    }

    #[tokio::test]
    async fn test_fallback_historical_view_captures_empty_backup_before_first_plan() {
        let (table, backup) = fallback_time_tables().await;
        let mut views = Vec::new();
        for allow in ["false", "true"] {
            views.push(
                table
                    .copy_with_time_travel_strict(options(&[
                        ("scan.snapshot-id", "1"),
                        ("scan.fallback-branch.allow-earliest-snapshot", allow),
                    ]))
                    .await
                    .unwrap(),
            );
        }
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        for view in views {
            assert_eq!(read_ids(&view).await, vec![1]);
        }
        assert_eq!(read_ids(&table).await, vec![1, 2]);
    }

    async fn set_snapshot_time(branch: &Table, id: i64, time: i64) {
        let manager = branch.snapshot_manager();
        let snapshot = manager.get_snapshot(id).await.unwrap();
        let mut json = serde_json::to_value(&snapshot).unwrap();
        json["timeMillis"] = serde_json::json!(time);
        branch
            .file_io()
            .new_output(&manager.snapshot_path(id))
            .unwrap()
            .write(serde_json::to_vec(&json).unwrap().into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fallback_repin_keeps_timestamp_selected_backup() {
        let (table, backup) = fallback_time_tables().await;
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        write_and_commit(&backup, &batch_v0(vec![3], vec![30])).await;
        set_snapshot_time(&backup, 1, 100).await;
        set_snapshot_time(&table, 1, 200).await;
        set_snapshot_time(&backup, 2, 300).await;
        let view = table
            .copy_with_time_travel_strict(options(&[("scan.timestamp-millis", "400")]))
            .await
            .unwrap();
        assert_eq!(read_ids(&view).await, vec![1, 2, 3]);
        let repinned = view.copy_with_pinned_snapshot(view.travel_snapshot());
        assert_eq!(read_ids(&repinned).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_fallback_pins_empty_and_nonempty_snapshots() {
        let (latest, backup) = fallback_time_tables().await;
        let snapshot = latest.resolve_read_snapshot().await.unwrap().unwrap();
        let pinned = latest.copy_with_pinned_snapshot(Some(&snapshot));
        let by_id = latest.copy_with_options(options(&[("scan.snapshot-id", "1")]));
        let by_version = latest.copy_with_options(options(&[("scan.version", "1")]));
        let timestamp = options(&[("scan.timestamp-millis", &i64::MAX.to_string())]);
        let by_time = latest
            .copy_with_time_travel_strict(timestamp.clone())
            .await
            .unwrap();
        let views = [&pinned, &by_id, &by_version, &by_time];
        for view in views {
            let (first, concurrent) = tokio::join!(read_ids(view), read_ids(view));
            assert_eq!((first, concurrent), (vec![1], vec![1]));
        }
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        for view in views {
            let copy = view.copy_with_options(options(&[("read.batch-size", "128")]));
            let (original, copied) = tokio::join!(read_ids(view), read_ids(&copy));
            assert_eq!((original, copied), (vec![1], vec![1]));
        }
        assert_eq!(read_ids(&latest).await, vec![1, 2]);
        assert_eq!(
            read_ids(&pinned.copy_with_pinned_snapshot(Some(&snapshot))).await,
            vec![1]
        );
        let later = latest
            .copy_with_time_travel_strict(timestamp)
            .await
            .unwrap();
        assert_eq!(read_ids(&later).await, vec![1, 2]);
        write_and_commit(&backup, &batch_v0(vec![3], vec![30])).await;
        assert_eq!(read_ids(&later).await, vec![1, 2]);
        assert_eq!(read_ids(&latest).await, vec![1, 2, 3]);

        // A different fallback branch has an independent snapshot namespace.
        crate::table::BranchManager::new(latest.file_io().clone(), latest.location().into())
            .create_branch("third")
            .await
            .unwrap();
        let third = latest.copy_with_branch("third").await.unwrap();
        write_and_commit(&third, &batch_v0(vec![4], vec![40])).await;
        let switched = by_time.copy_with_options(options(&[("scan.fallback-branch", "third")]));
        assert_eq!(read_ids(&switched).await, vec![1, 4]);
    }

    #[tokio::test]
    async fn test_fallback_branch_snapshot_time_is_not_a_shared_id() {
        let (table, backup) = fallback_time_tables().await;
        write_and_commit(&backup, &batch_v0(vec![2], vec![20])).await;
        write_and_commit(&backup, &batch_v0(vec![3], vec![30])).await;
        write_and_commit(&table, &batch_v0(vec![4], vec![10])).await;

        // Pin times so main snapshot 1 predates backup, while main snapshot 2
        // maps to backup snapshot 1 rather than backup's latest snapshot 2.
        for (branch, id, time) in [
            (&table, 1, 100),
            (&table, 2, 300),
            (&backup, 1, 200),
            (&backup, 2, 400),
        ] {
            set_snapshot_time(branch, id, time).await;
        }
        let fallback = table
            .copy_with_time_travel_strict(options(&[
                ("scan.fallback-branch", "backup"),
                ("scan.snapshot-id", "1"),
            ]))
            .await
            .unwrap();
        let error = fallback
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("allow-earliest-snapshot"),
            "{error}"
        );
        let allowed = fallback.copy_with_options(options(&[(
            "scan.fallback-branch.allow-earliest-snapshot",
            "true",
        )]));
        assert_eq!(read_ids(&allowed).await, vec![1, 2]);

        let strict = allowed.copy_with_options(options(&[(
            "scan.fallback-branch.allow-earliest-snapshot",
            "false",
        )]));
        assert!(strict
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err()
            .to_string()
            .contains("allow-earliest-snapshot"));
        let too_early = table.copy_with_options(options(&[("scan.timestamp-millis", "150")]));
        assert!(too_early
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err()
            .to_string()
            .contains("No snapshot found with timestamp <= 150"));

        // Without a tag, both ID selectors map main's time to backup snapshot 1.
        // The fallback-only row 2 must be present, but later row 3 must be absent.
        for selectors in [
            options(&[("scan.snapshot-id", "2")]),
            options(&[("scan.version", "2"), ("scan.snapshot-id", "overwritten")]),
        ] {
            let mapped = table.copy_with_time_travel_strict(selectors).await.unwrap();
            assert_eq!(read_ids(&mapped).await, vec![1, 2, 4]);
        }

        // Tag "2" selects each branch's latest snapshot. Unlike time mapping,
        // backup's tag includes row 3 from snapshot 2, committed at time 400.
        table
            .tag_manager()
            .create("2", &table.resolve_read_snapshot().await.unwrap().unwrap())
            .await
            .unwrap();
        let tagged = table
            .copy_with_time_travel_strict(options(&[
                ("scan.fallback-branch", "backup"),
                ("scan.tag-name", "overwritten"),
                ("scan.version", "2"),
            ]))
            .await
            .unwrap();
        assert!(tagged.new_read_builder().new_scan().plan().await.is_err());
        backup
            .tag_manager()
            .create("2", &backup.resolve_read_snapshot().await.unwrap().unwrap())
            .await
            .unwrap();
        assert_eq!(read_ids(&tagged).await, vec![1, 2, 3, 4]);
        let conflict =
            table.copy_with_options(options(&[("scan.version", "2"), ("scan.snapshot-id", "2")]));
        assert!(conflict
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err()
            .to_string()
            .contains("Only one time-travel selector"));
    }

    /// Table with two schema versions and one snapshot per version:
    /// snapshot 1 (schema 0: id, value) and snapshot 2 (schema 1: + age).
    async fn setup_evolved_table() -> (FileIO, String) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/evolved_table";
        for dir in ["snapshot", "manifest"] {
            file_io
                .mkdirs(&format!("{table_path}/{dir}/"))
                .await
                .unwrap();
        }
        write_schema_file(&file_io, table_path, &schema_v0()).await;
        let table_v0 = make_table(&file_io, table_path, schema_v0());
        write_and_commit(&table_v0, &batch_v0(vec![1, 2, 3], vec![10, 20, 30])).await;

        write_schema_file(&file_io, table_path, &schema_v1()).await;
        let table_v1 = make_table(&file_io, table_path, schema_v1());
        write_and_commit(&table_v1, &batch_v1(vec![4, 5], vec![40, 50], vec![14, 15])).await;

        (file_io, table_path.to_string())
    }

    #[tokio::test]
    async fn externally_resolved_schema_replaces_cached_time_travel() {
        let (io, path) = setup_evolved_table().await;
        let latest = latest_table(&io, &path);
        let historical = latest
            .copy_with_time_travel(options(&[("scan.snapshot-id", "1")]))
            .await
            .unwrap();
        assert_eq!(historical.travel_snapshot().unwrap().id(), 1);
        assert_eq!(historical.schema().id(), 0);
        // The external caller deliberately keeps the old fields while removing
        // the snapshot selector, as Java copyWithoutTimeTravel can do.
        let resolved = historical
            .copy_with_resolved_schema(schema_v0(), "main")
            .unwrap();
        assert_eq!(
            super::resolve_snapshot(&resolved)
                .await
                .unwrap()
                .unwrap()
                .id(),
            2
        );
        assert_eq!(resolved.schema().id(), 0);
        assert_eq!(historical.travel_snapshot().unwrap().id(), 1);
        let selected = resolved
            .copy_with_resolved_schema(
                schema_v0().copy_with_options(options(&[("scan.snapshot-id", "1")])),
                "main",
            )
            .unwrap();
        assert_eq!(
            super::resolve_snapshot(&selected)
                .await
                .unwrap()
                .unwrap()
                .id(),
            1
        );
        assert!(resolved
            .copy_with_resolved_schema(schema_v0(), "../invalid")
            .is_err());
    }

    fn latest_table(file_io: &FileIO, table_path: &str) -> Table {
        make_table(file_io, table_path, schema_v1())
    }

    /// Table whose snapshots carry watermarks, committed directly through
    /// `SnapshotManager` (the Rust commit path never writes watermarks):
    /// snapshot 1 (watermark 100), snapshot 2 (no watermark), snapshot 3
    /// (watermark 300), all on schema 0.
    async fn setup_watermark_table() -> (FileIO, String) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/watermark_table";
        for dir in ["snapshot", "manifest"] {
            file_io
                .mkdirs(&format!("{table_path}/{dir}/"))
                .await
                .unwrap();
        }
        write_schema_file(&file_io, table_path, &schema_v0()).await;
        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        for (id, watermark) in [(1, Some(100)), (2, None), (3, Some(300))] {
            let snapshot = Snapshot::builder()
                .version(3)
                .id(id)
                .schema_id(0)
                .base_manifest_list(format!("base-list-{id}"))
                .delta_manifest_list(format!("delta-list-{id}"))
                .commit_user("test-user".to_string())
                .commit_identifier(0)
                .commit_kind(CommitKind::APPEND)
                .time_millis(1000 * id as u64)
                .watermark(watermark)
                .build();
            sm.commit_snapshot(&snapshot).await.unwrap();
        }
        (file_io, table_path.to_string())
    }

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn test_scan_timestamp_reads_historical_schema_and_rows() {
        use chrono::{Local, TimeZone};
        use futures::TryStreamExt;

        let (io, path) = setup_evolved_table().await;
        let table = latest_table(&io, &path);
        let sm = table.snapshot_manager();
        let base = Local
            .with_ymd_and_hms(2024, 1, 2, 12, 0, 0)
            .unwrap()
            .timestamp_millis();
        // Fix commit times so the exact/between/before boundaries never depend
        // on how quickly the fixture commits complete.
        for (id, millis) in [(1, base + 123), (2, base + 1000)] {
            let mut snapshot = serde_json::to_value(sm.get_snapshot(id).await.unwrap()).unwrap();
            snapshot["timeMillis"] = serde_json::json!(millis);
            io.new_output(&sm.snapshot_path(id))
                .unwrap()
                .write(serde_json::to_vec(&snapshot).unwrap().into())
                .await
                .unwrap();
        }

        for timestamp in ["2024-01-02 12:00:00.123", "2024-01-02T12:00:00.999999999"] {
            let opts = options(&[
                ("scan.timestamp", timestamp),
                ("scan.mode", "from-timestamp"),
            ]);
            let traveled = table
                .copy_with_time_travel_strict(opts.clone())
                .await
                .unwrap();
            assert_eq!(traveled.schema().id(), 0);
            assert_eq!(traveled.travel_snapshot().unwrap().id(), 1);
            let builder = traveled.new_read_builder();
            let plan = builder.new_scan().plan().await.unwrap();
            let batches: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let ids: Vec<i32> = batches
                .iter()
                .flat_map(|batch| {
                    assert_eq!(batch.num_columns(), 2);
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(ids, vec![1, 2, 3]);

            let changed =
                traveled.copy_with_options(options(&[("scan.timestamp", "2024-01-02 12:00:01")]));
            assert!(changed.travel_snapshot().is_none());
            let resolved = changed
                .copy_with_time_travel_strict(HashMap::new())
                .await
                .unwrap();
            assert_eq!(resolved.travel_snapshot().unwrap().id(), 2);
            assert_eq!(resolved.schema().id(), 1);

            let pinned = traveled.copy_with_pinned_snapshot(traveled.travel_snapshot());
            assert!(!pinned.schema().options().contains_key("scan.timestamp"));
        }
        for timestamp in ["2024-01-02 12:00:01", "2024-01-02 12:00:02"] {
            let selected = super::resolve_snapshot(
                &table.copy_with_options(options(&[("scan.timestamp", timestamp)])),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(selected.id(), 2);
        }
        for timestamp in ["2024-01-02 12:00:00.122", "bad timestamp"] {
            let selected = table.copy_with_options(options(&[("scan.timestamp", timestamp)]));
            assert!(selected.new_read_builder().new_scan().plan().await.is_err());
        }
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_switches_to_snapshot_schema() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        assert_eq!(traveled.schema().id(), 0);
        let names: Vec<&str> = traveled
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(names, vec!["id", "value"]);
        assert!(traveled.is_time_traveled());
        // Options stay the merged ones, not the historical schema's options.
        assert_eq!(
            traveled.schema().options().get("scan.version"),
            Some(&"1".to_string())
        );
        // The resolved snapshot is cached for scans, and invalidated when the
        // selector changes.
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));
        let recopied = traveled.copy_with_options(options(&[("scan.version", "2")]));
        assert!(recopied.travel_snapshot().is_none());
    }

    #[tokio::test]
    async fn test_pinned_snapshot_preserves_read_schema_without_rereading() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path)
            .copy_with_options(options(&[("scan.version", "2"), ("custom", "value")]));
        let manager = table.snapshot_manager();
        let snapshot = manager.get_snapshot(1).await.unwrap();
        // Pinning and later resolution must reuse the supplied snapshot, not
        // read it again or switch the table's schema to schema 0.
        file_io
            .delete_file(&manager.snapshot_path(1))
            .await
            .unwrap();
        let pinned = table.copy_with_pinned_snapshot(Some(&snapshot));
        assert_eq!(pinned.schema().id(), table.schema().id());
        assert_eq!(pinned.schema().fields(), table.schema().fields());
        assert_eq!(pinned.schema().options().get("custom").unwrap(), "value");
        assert!(!pinned.schema().options().contains_key("scan.version"));
        assert_eq!(
            pinned.schema().options().get("scan.snapshot-id").unwrap(),
            "1"
        );
        assert_eq!(
            super::resolve_snapshot(&pinned).await.unwrap().unwrap(),
            snapshot
        );
        assert!(table.travel_snapshot().is_none());
    }

    #[tokio::test]
    async fn test_pinning_replaces_prior_supported_scan_modes() {
        use futures::TryStreamExt;
        let (io, path) = setup_evolved_table().await;
        let table = latest_table(&io, &path);
        let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
        for extra in [
            options(&[("scan.mode", "from-snapshot"), ("scan.snapshot-id", "1")]),
            options(&[
                ("scan.mode", "from-timestamp"),
                ("scan.timestamp-millis", "0"),
            ]),
            options(&[
                ("scan.mode", "incremental"),
                ("incremental-between-timestamp", "0,9999999999999"),
            ]),
        ] {
            let configured = table.copy_with_options(extra);
            for source in [None, Some(&snapshot)] {
                let view = configured.copy_with_pinned_snapshot(source);
                let plan = view.new_read_builder().new_scan().plan().await.unwrap();
                let builder = view.new_read_builder();
                let batches: Vec<RecordBatch> = builder
                    .new_read()
                    .unwrap()
                    .to_arrow(plan.splits())
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(rows, if source.is_some() { 5 } else { 0 });
                assert!(!view
                    .schema()
                    .options()
                    .contains_key("incremental-between-timestamp"));
            }
        }
    }

    #[tokio::test]
    async fn test_pinned_empty_view_survives_commits_and_option_copies() {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/pinned_empty";
        write_schema_file(&io, path, &schema_v0()).await;
        let table = make_table(&io, path, schema_v0());
        let pinned = table.copy_with_pinned_snapshot(None);
        write_and_commit(&table, &batch_v0(vec![1, 2], vec![10, 20])).await;

        assert_eq!(
            table.resolve_read_snapshot().await.unwrap().unwrap().id(),
            1
        );
        for view in [
            pinned.clone(),
            pinned.copy_with_options(options(&[("read.batch-size", "1")])),
        ] {
            assert!(view.resolve_read_snapshot().await.unwrap().is_none());
            assert!(view
                .new_read_builder()
                .new_scan()
                .plan()
                .await
                .unwrap()
                .splits()
                .is_empty());
            assert!(view.is_time_traveled());
            assert!(!view.has_resolved_travel_snapshot());
            assert!(crate::catalog::list_partitions_from_file_system(&view)
                .await
                .unwrap()
                .is_empty());
            assert!(view.partition_row_counts().await.unwrap().is_empty());
        }
        for extra in [
            options(&[("scan.mode", "default")]),
            options(&[("incremental-between-timestamp", "0,9999999999999")]),
        ] {
            let changed = pinned.copy_with_options(extra);
            assert!(changed.resolve_read_snapshot().await.is_err());
            assert!(changed.new_read_builder().new_scan().plan().await.is_err());
        }
        // Changing the selector must not silently reuse the empty view or the
        // schema captured by that view. Explicitly re-resolve the pair.
        let changed = pinned.copy_with_options(options(&[("scan.snapshot-id", "1")]));
        assert!(changed.resolve_read_snapshot().await.is_err());
        let selected = changed.copy_with_time_travel(HashMap::new()).await.unwrap();
        assert_eq!(
            selected
                .resolve_read_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            1
        );
        let reset = pinned
            .copy_with_resolved_schema(schema_v0(), "main")
            .unwrap();
        assert_eq!(
            reset.resolve_read_snapshot().await.unwrap().unwrap().id(),
            1
        );
        assert!(!reset.is_time_traveled());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_same_schema_can_append_and_commit() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        // Snapshot 2 carries the current schema, so the schema is not
        // switched. Its read snapshot does not limit append publication.
        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "2")]))
            .await
            .unwrap();

        assert_eq!(traveled.schema().id(), 1);
        assert!(!traveled.is_time_traveled());
        write_and_commit(&traveled, &batch_v1(vec![6], vec![60], vec![16])).await;
        assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(read_ids(&traveled).await, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .schema_id(),
            1
        );
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_without_selector_is_noop() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let copied = table.copy_with_time_travel(HashMap::new()).await.unwrap();

        assert_eq!(copied.schema(), table.schema());
        assert!(!copied.is_time_traveled());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_invalid_selector_falls_back_silently() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        // Not a tag and not a snapshot id: kept latest, error deferred to scan.
        let copied = table
            .copy_with_time_travel(options(&[("scan.version", "no-such-version")]))
            .await
            .unwrap();
        assert_eq!(copied.schema().id(), 1);
        assert!(!copied.is_time_traveled());

        // Conflicting selectors behave the same.
        let copied = table
            .copy_with_time_travel(options(&[
                ("scan.version", "1"),
                ("scan.timestamp-millis", "123"),
            ]))
            .await
            .unwrap();
        assert_eq!(copied.schema().id(), 1);
        assert!(!copied.is_time_traveled());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_by_timestamp_and_tag() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.clone());
        let snapshot1 = snapshot_manager.get_snapshot(1).await.unwrap();

        let traveled = table
            .copy_with_time_travel(options(&[(
                "scan.timestamp-millis",
                &snapshot1.time_millis().to_string(),
            )]))
            .await
            .unwrap();
        assert_eq!(traveled.schema().id(), 0);

        let tag_manager = TagManager::new(file_io.clone(), table_path.clone());
        tag_manager.create("v1-tag", &snapshot1).await.unwrap();
        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "v1-tag")]))
            .await
            .unwrap();
        assert_eq!(traveled.schema().id(), 0);
        assert!(traveled.is_time_traveled());
    }

    #[tokio::test]
    async fn test_time_traveled_table_appends_with_its_schema_and_publishes_latest() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        write_and_commit(&traveled, &batch_v0(vec![6], vec![60])).await;
        let latest_snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_snapshot.id(), 3);
        assert_eq!(latest_snapshot.schema_id(), 1);
        assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(read_ids(&traveled).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_changing_selector_after_travel_fails_scan() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        // Merging unrelated options keeps the resolved snapshot/schema pair.
        let recopied = traveled.copy_with_options(options(&[("k", "v")]));
        assert_eq!(recopied.travel_snapshot().map(|s| s.id()), Some(1));

        // Changing the selector without re-resolving leaves a historical
        // schema with no matching snapshot; scanning such a copy must fail
        // instead of evolving another snapshot's files to the stale schema.
        let stale = traveled.copy_with_options(options(&[("scan.version", "2")]));
        assert!(stale.travel_snapshot().is_none());
        let err = stale
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .expect_err("scan after selector change must fail");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("copy_with_time_travel")),
            "expected stale time-travel state error, got {err:?}"
        );

        // Re-resolving through copy_with_time_travel is the supported path.
        let retraveled = traveled
            .copy_with_time_travel(options(&[("scan.version", "2")]))
            .await
            .unwrap();
        assert_eq!(retraveled.schema().id(), 1);
        assert_eq!(retraveled.travel_snapshot().map(|s| s.id()), Some(2));
    }

    #[tokio::test]
    async fn test_invalid_snapshot_id_error_names_original_option() {
        let (file_io, table_path) = setup_evolved_table().await;
        let opts = options(&[("scan.snapshot-id", "abc")]);
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        let err = super::travel_to_snapshot(&sm, &tm, &opts)
            .await
            .expect_err("non-numeric snapshot-id must fail");
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains("scan.snapshot-id"), "got: {message}");
                assert!(!message.contains("scan.version"), "got: {message}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_time_travel_read_uses_snapshot_schema() {
        use futures::TryStreamExt;

        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();
        let builder = traveled.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let names: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(names, vec!["id", "value"]);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);

        // Latest table sees both snapshots' data and the evolved schema.
        let builder = table.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches[0].schema().fields().len(), 3);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_resolves_watermark() {
        let (file_io, table_path) = setup_watermark_table().await;
        let table = make_table(&file_io, &table_path, schema_v0());

        // Exact match on snapshot 1; snapshot 2 carries no watermark and is skipped.
        let traveled = table
            .copy_with_time_travel(options(&[("scan.watermark", "100")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));

        // Between watermarks: the earliest snapshot with watermark >= the value.
        let traveled = table
            .copy_with_time_travel(options(&[("scan.watermark", "150")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(3));
        assert!(traveled.has_resolved_travel_snapshot());
    }

    #[tokio::test]
    async fn test_scan_version_resolves_java_watermark_prefix_after_tag() {
        let (file_io, table_path) = setup_watermark_table().await;
        let table = make_table(&file_io, &table_path, schema_v0());

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "watermark-150")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(3));

        // Java resolves an existing tag before interpreting the watermark prefix.
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let snapshot1 = sm.get_snapshot(1).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        tm.create("watermark-150", &snapshot1).await.unwrap();
        let tagged = table
            .copy_with_time_travel(options(&[("scan.version", "watermark-150")]))
            .await
            .unwrap();
        assert_eq!(tagged.travel_snapshot().map(|s| s.id()), Some(1));

        let err =
            super::travel_to_snapshot(&sm, &tm, &options(&[("scan.version", "watermark-invalid")]))
                .await
                .expect_err("invalid watermark version must fail");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("invalid watermark value")),
            "expected watermark parse error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn scan_version_overwrites_same_selector_before_validation() {
        let (io, path) = setup_watermark_table().await;
        let table = make_table(&io, &path, schema_v0());
        let sm = table.snapshot_manager();
        let tm = table.tag_manager();
        tm.create("before", &sm.get_snapshot(1).await.unwrap())
            .await
            .unwrap();
        for (version, key, expected) in [
            ("1", "scan.snapshot-id", 1),
            ("before", "scan.tag-name", 1),
            ("watermark-150", "scan.watermark", 3),
        ] {
            let opts = options(&[("scan.version", version), (key, "invalid-overridden-value")]);
            let snapshot = super::travel_to_snapshot(&sm, &tm, &opts)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.id(), expected);
            assert_eq!(opts[key], "invalid-overridden-value");
            let traveled = table.copy_with_time_travel(opts).await.unwrap();
            assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(expected));
        }
        for opts in [
            options(&[("scan.version", "1"), ("scan.tag-name", "before")]),
            options(&[("scan.version", "before"), ("scan.snapshot-id", "1")]),
        ] {
            assert!(super::travel_to_snapshot(&sm, &tm, &opts).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_watermark_without_matching_snapshot_fails_at_scan() {
        let (file_io, table_path) = setup_watermark_table().await;
        let table = make_table(&file_io, &table_path, schema_v0());

        // Like Java tryTravelToSnapshot, resolution failure falls back silently...
        let unresolved = table
            .copy_with_time_travel(options(&[("scan.watermark", "301")]))
            .await
            .unwrap();
        assert!(!unresolved.has_resolved_travel_snapshot());

        // ...and the error surfaces at scan planning, naming the watermark.
        let err = unresolved
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .expect_err("scan with unresolvable watermark must fail");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("watermark[301]")),
            "expected watermark error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_watermark_selector_change_invalidates_resolved_snapshot() {
        let (file_io, table_path) = setup_watermark_table().await;
        let table = make_table(&file_io, &table_path, schema_v0());

        let traveled = table
            .copy_with_time_travel(options(&[("scan.watermark", "100")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));

        // Merging unrelated options keeps the resolved snapshot.
        let recopied = traveled.copy_with_options(options(&[("k", "v")]));
        assert_eq!(recopied.travel_snapshot().map(|s| s.id()), Some(1));

        // Changing the watermark invalidates the cached resolution.
        let changed = traveled.copy_with_options(options(&[("scan.watermark", "150")]));
        assert!(changed.travel_snapshot().is_none());
    }

    #[tokio::test]
    async fn test_has_resolved_travel_snapshot_reflects_resolution() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let resolved = table
            .copy_with_time_travel(options(&[("scan.snapshot-id", "1")]))
            .await
            .unwrap();
        assert!(resolved.has_resolved_travel_snapshot());

        // Nonexistent snapshot id: core silently falls back, helper reports false.
        let unresolved = table
            .copy_with_time_travel(options(&[("scan.snapshot-id", "999")]))
            .await
            .unwrap();
        assert!(!unresolved.has_resolved_travel_snapshot());

        // No selector at all: false.
        let none = table.copy_with_time_travel(HashMap::new()).await.unwrap();
        assert!(!none.has_resolved_travel_snapshot());
    }

    #[tokio::test]
    async fn test_changing_snapshot_id_or_tag_selector_after_travel_invalidates_cache() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));

        // scan.snapshot-id and scan.tag-name are first-class time-travel
        // selectors, so re-selecting through either on an already-travelled
        // copy must invalidate the cached snapshot — otherwise the stale
        // snapshot would be reused and the new selector silently ignored.
        for selector in ["scan.snapshot-id", "scan.tag-name"] {
            let stale = traveled.copy_with_options(options(&[(selector, "2")]));
            assert!(
                stale.travel_snapshot().is_none(),
                "{selector} must invalidate the cached travel snapshot"
            );
        }
    }

    #[tokio::test]
    async fn test_snapshot_id_selector_rejects_tag_name() {
        let (file_io, table_path) = setup_evolved_table().await;
        // A tag named "v1-tag" exists, but it is not a valid snapshot id.
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let snapshot1 = sm.get_snapshot(1).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        tm.create("v1-tag", &snapshot1).await.unwrap();

        // scan.snapshot-id must only accept a numeric snapshot id; it must not
        // fall back to resolving a tag of the same name.
        let err = super::travel_to_snapshot(&sm, &tm, &options(&[("scan.snapshot-id", "v1-tag")]))
            .await
            .expect_err("scan.snapshot-id must not resolve a tag name");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("scan.snapshot-id")),
            "expected snapshot-id parse error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_tag_name_selector_rejects_snapshot_id() {
        let (file_io, table_path) = setup_evolved_table().await;
        // No tag named "1" exists, but snapshot 1 does.
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        let err = super::travel_to_snapshot(&sm, &tm, &options(&[("scan.tag-name", "1")]))
            .await
            .expect_err("scan.tag-name must not resolve a snapshot id");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("Tag '1'")),
            "expected missing-tag error, got {err:?}"
        );
    }
}
