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
use crate::spec::{DataField, Snapshot, TimeTravelSelector};
use crate::{Error, Result};
use std::collections::HashMap;

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
    pub(crate) async fn fallback_table(&self) -> Result<Option<Table>> {
        let Some(branch) = self.schema().options().get("scan.fallback-branch") else {
            return Ok(None);
        };
        if branch.trim().is_empty() || branch == self.branch() {
            return Ok(None);
        }
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
        let other = match self.copy_with_branch(branch).await {
            Ok(table) => table,
            Err(Error::RestApi {
                source:
                    crate::api::RestError::NoSuchResource {
                        resource_type: Some(kind),
                        resource_name: Some(name),
                        ..
                    },
            }) if kind.eq_ignore_ascii_case("BRANCH") && name == *branch => return Ok(None),
            Err(error) => return Err(error),
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
                    || key.as_str() == "blob.as-descriptor"
                    || self.applied_dynamic_option_keys.contains(*key))
                    && !SELECTORS.contains(&key.as_str())
                    && !matches!(key.as_str(), "branch" | "bucket")
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        options.insert("scan.fallback-branch".into(), String::new());
        let mut base = other.schema().options().clone();
        for selector in SELECTORS {
            base.remove(*selector);
        }
        let mut other = other;
        other.schema = other.schema().copy_with_replaced_options(base);
        Ok(Some(other.copy_with_options(options)))
    }

    pub(crate) async fn fallback_read_table(&self, snapshot_id: i64) -> Result<Table> {
        let other = self
            .fallback_table()
            .await?
            .ok_or_else(|| Error::ConfigInvalid {
                message: "Fallback splits require an existing scan.fallback-branch".into(),
            })?;
        let snapshot = other.snapshot_manager().get_snapshot(snapshot_id).await?;
        let other = if self.schema().core_options().has_time_travel_selector() {
            other.copy_with_resolved_snapshot(&snapshot).await?
        } else {
            other.copy_with_pinned_snapshot(Some(&snapshot))
        };
        validate_schema(self, &other)?;
        Ok(other)
    }

    pub(crate) async fn fallback_snapshot(
        &self,
        other: &Table,
        current: Option<&Snapshot>,
    ) -> Result<Option<Snapshot>> {
        let selector = self.schema().core_options().try_time_travel_selector()?;
        let map_snapshot = match selector {
            Some(TimeTravelSelector::SnapshotId { .. }) => true,
            Some(TimeTravelSelector::Version { value, .. }) if value.parse::<i64>().is_ok() => {
                super::time_travel::resolve_version_tag(&self.tag_manager(), value)
                    .await?
                    .is_none()
            }
            _ => false,
        };
        if map_snapshot {
            let Some(current) = current else {
                return Ok(None);
            };
            let manager = other.snapshot_manager();
            if let Some(snapshot) = manager
                .earlier_or_equal_time_millis(current.time_millis() as i64)
                .await?
            {
                return Ok(Some(snapshot));
            }
            let Some(id) = manager.earliest_snapshot_id().await? else {
                return Ok(None);
            };
            let allow = self
                .schema()
                .options()
                .get("scan.fallback-branch.allow-earliest-snapshot")
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            if !allow {
                return Err(Error::ConfigInvalid {
                    message: format!("No snapshot of fallback branch '{}' exists at or before the requested snapshot; set scan.fallback-branch.allow-earliest-snapshot=true to allow its earliest snapshot", other.branch()),
                });
            }
            return manager.get_snapshot(id).await.map(Some);
        }
        if selector.is_some() {
            return super::time_travel::travel_to_snapshot(
                &other.snapshot_manager(),
                &other.tag_manager(),
                self.schema().options(),
            )
            .await;
        }
        other.snapshot_manager().get_latest_snapshot().await
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

pub(super) fn read_fields(fields: &[DataField], other: &Table) -> Vec<DataField> {
    fields
        .iter()
        .map(|field| {
            match other
                .schema()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field.name())
            {
                // Projection order, nullability and nested projection belong to the
                // query; field IDs belong to this branch's schema history.
                Some(other) => field.clone().with_id(other.id()),
                None => field.clone(), // system column
            }
        })
        .collect()
}
