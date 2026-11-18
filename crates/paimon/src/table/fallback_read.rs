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

//! Partition fallback branch resolution shared by batch scans and readers.

use super::{SchemaManager, Table};
use crate::spec::{DataField, Predicate, Snapshot, TimeTravelSelector};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

pub(super) type PinnedFallbackSnapshot = Arc<OnceCell<Option<Box<Snapshot>>>>;

fn allow_earliest(options: &HashMap<String, String>) -> bool {
    options
        .get("scan.fallback-branch.allow-earliest-snapshot")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

const SELECTORS: &[&str] = &[
    "scan.snapshot-id",
    "scan.version",
    "scan.tag-name",
    "scan.timestamp",
    "scan.timestamp-millis",
    "scan.watermark",
    "scan.mode",
];

impl Table {
    pub(super) fn fallback_pin_with_options(
        &self,
        options: &HashMap<String, String>,
    ) -> PinnedFallbackSnapshot {
        let previous = self.schema().options();
        if SELECTORS
            .iter()
            .copied()
            .chain(["scan.fallback-branch"])
            .all(|key| previous.get(key) == options.get(key))
            && allow_earliest(previous) == allow_earliest(options)
        {
            self.pinned_fallback_snapshot.clone()
        } else {
            Arc::default()
        }
    }

    pub(super) fn fallback_uses_snapshot_schema(&self) -> bool {
        // Only resolved time travel selects historical schemas. Option/schema
        // copies and preserve-schema pins retain each branch's read schema,
        // even when a selector chooses data written before those fields existed.
        self.has_resolved_travel_snapshot() && !self.preserve_read_schema
    }

    pub(crate) async fn fallback_table(&self) -> Result<Option<Table>> {
        let Some(branch) = self.schema().options().get("scan.fallback-branch") else {
            return Ok(None);
        };
        if branch.trim().is_empty() || branch == self.branch() {
            return Ok(None);
        }
        let other = self
            .fallback_table
            .get_or_try_init(|| self.load_fallback_table(branch))
            .await?;
        let Some(other) = other else {
            return Ok(None);
        };
        // Retain the other branch's physical layout (notably bucket count and
        // schema history). Only the caller's read options cross the boundary.
        let mut options: HashMap<_, _> = self
            .schema()
            .options()
            .iter()
            .filter(|(key, _)| {
                (key.starts_with("scan.")
                    || key.starts_with("read.")
                    || key.starts_with("source.")
                    || key.starts_with("parquet.")
                    || key.starts_with("mosaic.read.")
                    || key.starts_with("table-read.")
                    || key.as_str() == "blob-as-descriptor"
                    || self.applied_dynamic_option_keys.contains(*key))
                    && !SELECTORS.contains(&key.as_str())
                    && !matches!(key.as_str(), "branch" | "bucket")
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        options.insert("scan.fallback-branch".into(), String::new());
        let mut base = other.schema().options().clone();
        // An absent caller option selects payloads, even if backup stores true.
        base.remove("blob-as-descriptor");
        for selector in SELECTORS {
            base.remove(*selector);
        }
        let mut other = other.clone();
        other.schema = other.schema().copy_with_replaced_options(base);
        Ok(Some(other.copy_with_options(options)))
    }

    // SplitSerializer's existing branch wrappers carry no read schema. Keep
    // that schema on the query table, like Java's FallbackReadFileStoreTable,
    // rather than reloading it when a split is executed. Snapshot schemas
    // cannot substitute for it: DDL need not produce a new data snapshot.
    async fn load_fallback_table(&self, branch: &str) -> Result<Option<Table>> {
        super::validate_branch_name(branch)?;
        // Like Java's table factory, a not-yet-created fallback branch is a no-op.
        // REST branches need not have schema files in object storage.
        if self.rest_env().is_none() {
            let manager = SchemaManager::new(self.file_io().clone(), self.location().to_string());
            let manager = if branch == "main" {
                manager
            } else {
                manager.with_branch(branch)
            };
            if manager.latest().await?.is_none() {
                return Ok(None);
            }
        }
        match self.load_branch(branch, true).await {
            Ok(table) => Ok(Some(table)),
            Err(Error::BranchNotExist { branch_name, .. }) if branch_name == branch => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn fallback_read_table(
        &self,
        snapshot_id: i64,
        query_auth_required: bool,
    ) -> Result<Table> {
        let other = self
            .fallback_table()
            .await?
            .ok_or_else(|| Error::ConfigInvalid {
                message: "Fallback splits require an existing scan.fallback-branch".into(),
            })?;
        // Ordinary splits already identify their files. Reuse the captured branch
        // schema even if snapshot metadata has expired; time travel and query
        // authorization still require snapshot resolution below.
        if !self.schema().core_options().has_time_travel_selector()
            && !query_auth_required
            && !other.schema().core_options().query_auth_enabled()
        {
            validate_schema(self, &other)?;
            return Ok(other);
        }
        let snapshot = match self
            .pinned_fallback_snapshot
            .get()
            .and_then(Option::as_deref)
        {
            Some(snapshot) if snapshot.id() == snapshot_id => snapshot.clone(),
            _ => match other.snapshot_manager().get_snapshot(snapshot_id).await {
                // Tags retain full snapshot metadata after the snapshot file expires.
                Err(error @ Error::SnapshotNotExist { .. }) => other
                    .tag_manager()
                    .list_all()
                    .await?
                    .into_iter()
                    .map(|(_, snapshot)| snapshot)
                    .find(|snapshot| snapshot.id() == snapshot_id)
                    .ok_or(error)?,
                result => result?,
            },
        };
        let other = if self.fallback_uses_snapshot_schema() {
            other.copy_with_resolved_snapshot(&snapshot).await?
        } else {
            other.copy_with_pinned_snapshot(Some(&snapshot))
        };
        validate_schema(self, &other)?;
        Ok(other)
    }

    // An explicitly resolved historical view captures both branches, including an empty backup.
    pub(super) async fn pin_resolved_fallback(&self) -> Result<()> {
        if let Some(other) = self.fallback_table().await? {
            match self.fallback_snapshot(&other, self.travel_snapshot()).await {
                // An unresolved backup selector is reported by planning. A transported
                // split may still address a valid snapshot after its tag disappears.
                Ok(_)
                | Err(Error::ConfigInvalid { .. } | Error::DataInvalid { source: None, .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(crate) async fn fallback_snapshot(
        &self,
        other: &Table,
        current: Option<&Snapshot>,
    ) -> Result<Option<Snapshot>> {
        if matches!(self.travel_snapshot, Some(super::ReadSnapshot::Empty)) {
            // The empty pin captures the whole read view, including fallback.
            return Ok(None);
        }
        if self.travel_snapshot.is_some() {
            if let Some(pinned) = self.pinned_fallback_snapshot.get() {
                return Ok(pinned.as_deref().cloned());
            }
        }
        let (options, _) =
            super::time_travel::adapt_version(&self.tag_manager(), self.schema().options()).await?;
        let selector = crate::spec::CoreOptions::new(&options).try_time_travel_selector()?;
        let fixed = self.travel_snapshot.is_some()
            || matches!(selector, Some(TimeTravelSelector::SnapshotId { .. }));
        let resolve = self.resolve_fallback_snapshot(other, current, selector);
        // A resolved view or an explicit snapshot ID has one backup selection
        // for the lifetime of this view.
        if fixed {
            let pinned = self
                .pinned_fallback_snapshot
                .get_or_try_init(|| async { Ok::<_, Error>(resolve.await?.map(Box::new)) })
                .await?;
            return Ok(pinned.as_deref().cloned());
        }
        resolve.await
    }

    async fn resolve_fallback_snapshot(
        &self,
        other: &Table,
        current: Option<&Snapshot>,
        selector: Option<TimeTravelSelector<'_>>,
    ) -> Result<Option<Snapshot>> {
        let manager = other.snapshot_manager();
        if matches!(selector, Some(TimeTravelSelector::SnapshotId { .. })) {
            let Some(current) = current else {
                return Ok(None);
            };
            if let Some(snapshot) = manager
                .earlier_or_equal_time_millis(current.time_millis() as i64)
                .await?
            {
                return Ok(Some(snapshot));
            }
            let Some(id) = manager.earliest_snapshot_id().await? else {
                return Ok(None);
            };
            if !allow_earliest(self.schema().options()) {
                return Err(Error::ConfigInvalid {
                    message: format!("No snapshot of fallback branch '{}' exists at or before the requested snapshot; set scan.fallback-branch.allow-earliest-snapshot=true to allow its earliest snapshot", other.branch()),
                });
            }
            return manager.get_snapshot(id).await.map(Some);
        }
        if let Some(TimeTravelSelector::TimestampMillis(ts)) = selector {
            return super::time_travel::snapshot_at_timestamp(&manager, ts, true).await;
        }
        if selector.is_some() {
            return super::time_travel::travel_to_snapshot(
                &manager,
                &other.tag_manager(),
                self.schema().options(),
            )
            .await;
        }
        manager.get_latest_snapshot().await
    }
}

pub(super) fn validate_schema(table: &Table, other: &Table) -> Result<()> {
    let fields = table.schema().fields();
    let other_fields = other.schema().fields();
    if fields.len() != other_fields.len()
        || fields.iter().zip(other_fields).any(|(a, b)| {
            a.name() != b.name() || !a.data_type().equals_ignore_nullable(b.data_type())
        })
        || table.schema().partition_keys() != other.schema().partition_keys()
        || table.schema().primary_keys() != other.schema().primary_keys()
    {
        return Err(Error::ConfigInvalid {
            message: format!("Branch '{}' and fallback branch '{}' must have the same row type, partition keys and primary keys", table.branch(), other.branch()),
        });
    }
    Ok(())
}

/// The compatible schemas have the same order and nested types (including
/// child IDs). Only top-level IDs, descriptions and nullability may differ.
pub(super) fn bind_predicate(
    predicate: &mut Predicate,
    source: &[DataField],
    target: &[DataField],
) -> Result<()> {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                bind_predicate(child, source, target)?;
            }
        }
        Predicate::Not(child) => bind_predicate(child, source, target)?,
        Predicate::Leaf { column, .. } if crate::spec::is_reserved_system_field_name(column) => {}
        Predicate::Leaf { .. } | Predicate::NestedLeaf { .. } => {
            super::query_auth::reject_noncanonical_leaves(std::slice::from_ref(predicate), source)?;
            match predicate {
                Predicate::Leaf {
                    index, data_type, ..
                } => {
                    *data_type = target[*index].data_type().clone();
                }
                Predicate::NestedLeaf { index, root, .. } => {
                    **root = target[*index].clone();
                }
                _ => unreachable!(),
            }
        }
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
    Ok(())
}

pub(super) fn read_fields(
    fields: &[DataField],
    table: &Table,
    other: &Table,
) -> Result<Vec<DataField>> {
    if other.schema().core_options().query_auth_enabled() {
        super::query_auth::reject_noncanonical_fields(fields, table.schema().fields())?;
    }
    fields
        .iter()
        .map(|field| {
            if crate::spec::is_reserved_system_field_name(field.name()) {
                return Ok(field.clone());
            }
            let index = table
                .schema()
                .fields()
                .iter()
                .position(|source| source.id() == field.id() && source.name() == field.name())
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!(
                        "Fallback projection '{}' does not match the query schema",
                        field.name()
                    ),
                })?;
            let local = &other.schema().fields()[index];
            // Keep projected ROW children and connector extraction metadata.
            let data_type = field
                .data_type()
                .copy_with_nullable(local.data_type().is_nullable())?;
            Ok(DataField::new(local.id(), field.name().into(), data_type)
                .with_description(field.description().map(str::to_owned))
                .with_default_value(field.default_value().map(str::to_owned)))
        })
        .collect()
}

/// Readers produce the nullable runtime Arrow type on both branches. Publish
/// query field IDs after branch-local projection, retaining any reader metadata
/// (for example resolved BLOBs) and the row count of zero-column projections.
pub(super) fn query_batch(
    batch: arrow_array::RecordBatch,
    query: &arrow_schema::SchemaRef,
) -> Result<arrow_array::RecordBatch> {
    let schema = batch.schema();
    if schema.fields() == query.fields() {
        return Ok(batch);
    }
    if schema.fields().len() != query.fields().len() {
        return Err(Error::DataInvalid {
            message: "Fallback batch has a different projection".into(),
            source: None,
        });
    }
    let fields = schema
        .fields()
        .iter()
        .zip(query.fields())
        .map(|(local, query)| {
            if local.name() != query.name() {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Expected fallback column '{}', got '{}'",
                        query.name(),
                        local.name()
                    ),
                    source: None,
                });
            }
            let mut metadata = local.metadata().clone();
            metadata.extend(query.metadata().clone());
            Ok(query.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = arrow_schema::Schema::new_with_metadata(fields, schema.metadata().clone());
    let (_, arrays, rows) = batch.into_parts();
    arrow_array::RecordBatch::try_new_with_options(
        Arc::new(schema),
        arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|error| Error::DataInvalid {
        message: format!("Fallback batch does not match query schema: {error}"),
        source: Some(Box::new(error)),
    })
}
