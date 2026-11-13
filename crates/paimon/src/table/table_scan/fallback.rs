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

//! Batch partition fallback. Ownership is established before data pruning.

use super::*;

impl PaimonTableScan<'_> {
    pub(super) async fn plan_fallback(&self, stats: &mut ScanStats) -> crate::Result<Option<Plan>> {
        let Some(other) = self.table.fallback_table().await? else {
            return Ok(None);
        };
        let current = super::super::time_travel::resolve_snapshot(&self.table).await?;
        let other_snapshot = self
            .table
            .fallback_snapshot(&other, current.as_ref())
            .await?;
        let Some(other_snapshot) = other_snapshot else {
            return Ok(None);
        };
        let other = if self
            .table
            .schema()
            .core_options()
            .has_time_travel_selector()
        {
            other.copy_with_resolved_snapshot(&other_snapshot).await?
        } else {
            other.copy_with_pinned_snapshot(Some(&other_snapshot))
        };
        super::super::fallback_read::validate_schema(&self.table, &other)?;

        // List complete current-branch partitions at exactly the snapshot we
        // will read. Data predicates, indexes, buckets and LIMIT cannot make a
        // partition absent: otherwise fallback would resurrect shadowed rows.
        let mut listing = self.clone();
        listing.data_predicates.clear();
        listing.bucket_predicate = None;
        listing.row_ranges = None;
        listing.split_selection = None;
        listing.scan_all_files = true;
        let main_entries = match &current {
            Some(snapshot) => {
                listing
                    .read_filtered_manifest_entries(
                        ManifestListSource::Snapshot(snapshot),
                        None,
                        &mut ScanStats::default(),
                    )
                    .await?
            }
            None => Vec::new(),
        };
        let owned: HashSet<Vec<u8>> = main_entries
            .iter()
            .map(|entry| entry.partition().to_vec())
            .collect();

        // Position slices and chunk distribution apply once to the combined
        // input. Applying a slice separately would restart its offset in each
        // branch and omit rows; LIMIT must not truncate either input first.
        let mut main_scan = self.clone();
        if self.row_position_selection().is_some() || self.chunk_shuffle().is_some() {
            main_scan.split_selection.as_mut().unwrap().mode = None;
            main_scan.limit = None;
            if self.chunk_shuffle().is_some() {
                main_scan.split_selection.as_mut().unwrap().shard = None;
            }
        }
        let mut fallback = main_scan.clone();
        fallback.table = ScanTable::Owned(Arc::new(other));
        fallback.query_auth_grant = None;
        if let Some(ids) = &self.projected_read_field_ids {
            fallback.projected_read_field_ids = Some(
                self.table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|field| ids.contains(&field.id()))
                    .filter_map(|field| {
                        fallback
                            .table
                            .schema()
                            .fields()
                            .iter()
                            .find(|candidate| candidate.name() == field.name())
                            .map(DataField::id)
                    })
                    .collect(),
            );
        }
        let mut fallback = fallback.authorized_scan().await?.into_owned();
        // Build the absent-partition set using partition predicates only.
        listing.table = fallback.table.clone();
        let fallback_entries: Vec<_> = listing
            .read_filtered_manifest_entries(
                ManifestListSource::Snapshot(&other_snapshot),
                None,
                &mut ScanStats::default(),
            )
            .await?
            .into_iter()
            .filter(|entry| !owned.contains(entry.partition()))
            .collect();
        let partitions = fallback_entries
            .iter()
            .map(|entry| entry.partition().to_vec())
            .collect();
        fallback.partition_filter = Some(PartitionFilter::PartitionSet {
            partitions,
            bounds: Vec::new(),
        });
        if self.table.schema().core_options().data_evolution_enabled() {
            if let Some(selection) = self.row_position_selection() {
                let counts = [&main_entries, &fallback_entries].map(|entries| {
                    RowPositionSelection::Slice {
                        start: 0,
                        end: u64::MAX,
                    }
                    .select(entries, None)
                    .map(|ranges| {
                        ranges
                            .iter()
                            .map(|range| range.to() as u64 - range.from() as u64 + 1)
                            .sum::<u64>()
                    })
                });
                let [main_count, fallback_count] = counts;
                let main_count = main_count?;
                let total = main_count.checked_add(fallback_count?).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: "Fallback row-position count overflow".into(),
                        source: None,
                    }
                })?;
                let (start, end) = selection.bounds(total);
                // DE positions count complete row-ID domains before group stats,
                // projection and DVs. Each branch has an independent ID namespace.
                main_scan.row_ranges = Some(
                    RowPositionSelection::Slice { start, end }
                        .select(&main_entries, main_scan.row_ranges.as_deref())?,
                );
                fallback.row_ranges = Some(
                    RowPositionSelection::Slice {
                        start: start.saturating_sub(main_count),
                        end: end.saturating_sub(main_count),
                    }
                    .select(&fallback_entries, fallback.row_ranges.as_deref())?,
                );
            }
        }

        let fields = self.projected_read_field_ids()?;
        let main = match current {
            Some(snapshot) => {
                main_scan
                    .plan_snapshot(snapshot, fields.as_ref(), stats)
                    .await?
            }
            None => Plan::new(Vec::new()),
        };
        let main = self.finish_plan(main).await?;
        let main_candidates = stats.split_candidates_built;
        let main_stopped = stats.limit_early_stopped;
        let main_base = stats.base_manifest_files;
        let main_delta = stats.delta_manifest_files;
        let fields = fallback.projected_read_field_ids()?;
        let backup = fallback
            .plan_snapshot(other_snapshot, fields.as_ref(), stats)
            .await?;
        let backup = fallback.finish_plan(backup).await?;
        stats.record_split_planning(
            main_candidates + stats.split_candidates_built,
            main_stopped || stats.limit_early_stopped,
        );
        stats.record_manifest_lists(
            main_base + stats.base_manifest_files,
            main_delta + stats.delta_manifest_files,
        );
        let snapshot_id = main.snapshot_id();
        let mut splits = main.into_splits();
        splits.extend(
            backup
                .into_splits()
                .into_iter()
                .map(|split| split.with_fallback(true)),
        );
        if !self.table.schema().core_options().data_evolution_enabled() {
            if let Some(selection) = self.row_position_selection() {
                splits = selection.select_append_splits(
                    splits,
                    self.table.schema().core_options().row_tracking_enabled(),
                )?;
            }
        }
        if let Some(config) = self.chunk_shuffle() {
            splits = chunk_shuffle_splits(&self.table, splits, config, self.shard()).await?;
        }
        Ok(Some(Plan::from_splits(snapshot_id, splits)))
    }
}
