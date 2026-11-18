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

use super::super::{fallback_read::bind_predicate, ReadSnapshot};
use super::*;

impl PaimonTableScan<'_> {
    pub(super) async fn plan_fallback(
        &self,
        requested: &Self,
        stats: &mut ScanStats,
    ) -> crate::Result<Option<Plan>> {
        let Some(other) = self.table.fallback_table().await? else {
            return Ok(None);
        };
        let current = super::super::time_travel::resolve_snapshot(&self.table).await?;
        let other_snapshot = self
            .table
            .fallback_snapshot(&other, current.as_ref())
            .await?;
        let other = match other_snapshot.as_ref() {
            Some(snapshot) if self.table.fallback_uses_snapshot_schema() => {
                other.copy_with_resolved_snapshot(snapshot).await?
            }
            None if !matches!(self.table.travel_snapshot, Some(ReadSnapshot::Empty)) => {
                return Ok(None);
            }
            snapshot => other.copy_with_pinned_snapshot(snapshot),
        };
        let fallback_query_auth = other.schema().core_options().query_auth_enabled();
        // An empty pin has no files to read, but backup authorization still
        // maps the requested projection and partition predicates across schemas.
        if other_snapshot.is_some() || fallback_query_auth {
            super::super::fallback_read::validate_schema(&self.table, &other)?;
        }
        // DE positions count complete row-ID domains before pruning, whereas
        // ordinary append positions count the final stats-pruned physical rows.
        // Neither rule can be applied to both layouts in one positional scan.
        if other_snapshot.is_some()
            && self.row_position_selection().is_some()
            && self.table.schema().core_options().data_evolution_enabled()
                != other.schema().core_options().data_evolution_enabled()
        {
            return Err(crate::Error::Unsupported {
                message: "Fallback row-position selection requires both branches to use the same data-evolution mode".into(),
            });
        }

        // Main's authorization may add rule-only columns and remove unsafe
        // pruning predicates. Authorize backup from the user's original scan,
        // so each branch expands only its own rule dependencies.
        let mut fallback = requested.clone();
        fallback.table = ScanTable::Owned(Arc::new(other));
        fallback.query_auth_grant = None;
        if other_snapshot.is_some() || fallback_query_auth {
            let source = self.table.schema();
            let target = fallback.table.schema();
            for predicate in fallback
                .data_predicates
                .iter_mut()
                .chain(&mut fallback.bucket_predicate)
            {
                bind_predicate(predicate, source.fields(), target.fields())?;
            }
            let partition = fallback
                .split_selection
                .as_mut()
                .and_then(|s| s.logical_partition_predicate.as_mut());
            let physical = match &mut fallback.partition_filter {
                Some(PartitionFilter::Predicate(predicate)) => Some(predicate),
                _ => None,
            };
            for predicate in partition.into_iter().chain(physical) {
                bind_predicate(
                    predicate,
                    &source.partition_fields(),
                    &target.partition_fields(),
                )?;
            }
        }
        if fallback_query_auth {
            if let Some(predicate) = fallback
                .split_selection
                .as_mut()
                .and_then(|s| s.logical_partition_predicate.take())
            {
                fallback.partition_filter = None;
                fallback.data_predicates.push(
                    super::super::read_builder::partition_predicate_on_table(
                        &fallback.table,
                        &predicate,
                    ),
                );
            }
        }
        if let Some(ids) = &requested.projected_read_field_ids {
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
                    // System field IDs are the same across branches; preserve them unchanged.
                    // When _SEQUENCE_NUMBER is needed, Data Evolution must retain the file
                    // carrying the latest sequence number, even if the query does not read
                    // any user columns written by that file.
                    .chain(ids.iter().copied().filter(|id| is_system_field_id(*id)))
                    .collect(),
            );
        }
        let mut fallback = fallback.authorized_scan().await?.into_owned();
        let other_snapshot = match other_snapshot {
            Some(snapshot) if self.limit != Some(0) => snapshot,
            selection => {
                if selection.is_some() {
                    // LIMIT 0 skips ownership I/O, but still validates the
                    // manifest options of each branch we would have scanned.
                    if current.is_some() {
                        self.table
                            .schema()
                            .core_options()
                            .scan_manifest_parallelism()?;
                    }
                    fallback
                        .table
                        .schema()
                        .core_options()
                        .scan_manifest_parallelism()?;
                    stats.record_split_planning(0, true);
                }
                self.check_planned_files().await?;
                fallback.check_planned_files().await?;
                return Ok(Some(Plan::from_splits(
                    current.as_ref().map(Snapshot::id),
                    Vec::new(),
                )));
            }
        };
        let tracks_ids = |table: &Table| {
            let options = table.schema().core_options();
            options.row_tracking_enabled() || options.data_evolution_enabled()
        };
        // The read builder can extract ranges only against main's layout.
        // With an untracked main, recover the logical ranges for a tracked backup.
        let ranges = if requested.row_ranges_from_filter {
            requested.row_ranges.clone()
        } else if !tracks_ids(&self.table) && !requested.data_predicates.is_empty() {
            super::super::row_id_predicate::extract_row_id_ranges(&Predicate::and(
                requested.data_predicates.clone(),
            ))
        } else {
            None
        };
        let skip_main = ranges.is_some() && !tracks_ids(&self.table);
        let skip_backup = ranges.is_some() && !tracks_ids(&fallback.table);
        if let Some(ranges) = ranges {
            // Comparisons represented by these ranges cannot match a NULL row ID.
            fallback.row_ranges = (!skip_backup).then_some(ranges);
        }
        // Position slices and chunk distribution apply once to the combined
        // input. Applying a slice separately would restart its offset in each
        // branch and omit rows; LIMIT must not truncate either input first.
        let mut main_scan = self.clone();
        if self.row_position_selection().is_some() || self.chunk_shuffle().is_some() {
            for scan in [&mut main_scan, &mut fallback] {
                scan.split_selection.as_mut().unwrap().mode = None;
                scan.limit = None;
                if self.chunk_shuffle().is_some() {
                    scan.split_selection.as_mut().unwrap().shard = None;
                }
            }
        }
        // List complete current-branch partitions at exactly the snapshot we
        // will read. Data predicates, indexes, buckets and LIMIT cannot make a
        // partition absent: otherwise fallback would resurrect shadowed rows.
        let mut listing = self.clone();
        // A shared authorized predicate is safe for ownership. If masking makes
        // the two predicates differ, retain only the explicit physical selector.
        listing.partition_filter = if self.partition_filter == fallback.partition_filter {
            self.partition_filter.clone()
        } else if requested.logical_partition_predicate().is_none() {
            requested.partition_filter.clone()
        } else {
            None
        };
        listing.data_predicates.clear();
        listing.bucket_predicate = None;
        listing.row_ranges = None;
        listing.split_selection = None;
        listing.scan_all_files = true;
        listing.file_scan_options = None;
        let main_entries = match &current {
            Some(snapshot) => listing.read_ownership_entries(snapshot, stats).await?,
            None => Vec::new(),
        };
        let owned: HashSet<Vec<u8>> = main_entries
            .iter()
            .map(|entry| entry.partition().to_vec())
            .collect();

        // Build the absent-partition set using partition predicates only.
        listing.table = fallback.table.clone();
        // A predicate safe on main may prune rows that match after backup's masking.
        // Use backup's independently authorized pruning before subtracting owned partitions.
        listing.partition_filter = fallback.partition_filter.clone();
        let fallback_entries: Vec<_> = listing
            .read_ownership_entries(&other_snapshot, stats)
            .await?
            .into_iter()
            .filter(|entry| !owned.contains(entry.partition()))
            .collect();
        if self.table.schema().core_options().data_evolution_enabled() {
            if let Some(selection) = self.row_position_selection() {
                // Ownership used the wider, shared physical selector. DE
                // positions still count only main's own selected partitions.
                let mut main_position_entries = Vec::new();
                for entry in main_entries {
                    // Nonmatching owned partitions must not occupy positions ahead of backup.
                    if self
                        .partition_filter
                        .as_ref()
                        .map_or(Ok(true), |filter| filter.matches_entry(entry.partition()))?
                    {
                        main_position_entries.push(entry);
                    }
                }
                // Count the row-ID unions in the same domains used below for selecting ranges.
                let counts = [&main_position_entries, &fallback_entries].map(|entries| {
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
                // Select from the counted main domain, not the wider ownership domain.
                main_scan.row_ranges = Some(
                    RowPositionSelection::Slice { start, end }
                        .select(&main_position_entries, main_scan.row_ranges.as_deref())?,
                );
                fallback.row_ranges = Some(
                    RowPositionSelection::Slice {
                        // If main_count is zero, global [0,1) stays backup-local [0,1).
                        start: start.saturating_sub(main_count),
                        end: end.saturating_sub(main_count),
                    }
                    .select(&fallback_entries, fallback.row_ranges.as_deref())?,
                );
            }
        }

        let fields = self.projected_read_field_ids()?;
        let mut main_stats = ScanStats::default();
        let main = match current {
            Some(snapshot) if skip_main => Plan::from_splits(Some(snapshot.id()), Vec::new()),
            Some(snapshot) => {
                main_scan
                    .plan_snapshot(snapshot, fields.as_ref(), &mut main_stats)
                    .await?
            }
            None => Plan::new(Vec::new()),
        };
        let main = self.finish_plan(main).await?;
        stats.accumulate_planning(&main_stats);
        let snapshot_id = main.snapshot_id();
        let mut splits = main.into_splits();
        let mut fallback_stats = ScanStats::default();
        let backup = if skip_backup || fallback_entries.is_empty() {
            Plan::new(Vec::new())
        } else {
            fallback.partition_filter = Some(PartitionFilter::from_partition_set(
                fallback_entries
                    .iter()
                    .map(|entry| entry.partition().to_vec())
                    .collect(),
                &fallback.table.schema().partition_fields(),
            )?);
            let fields = fallback.projected_read_field_ids()?;
            fallback
                .plan_snapshot(other_snapshot, fields.as_ref(), &mut fallback_stats)
                .await?
        };
        let backup = fallback.finish_plan(backup).await?;
        stats.accumulate_planning(&fallback_stats);
        splits.extend(
            backup
                .into_splits()
                .into_iter()
                .map(|split| split.with_fallback(true)),
        );
        if !self.table.schema().core_options().data_evolution_enabled() {
            if let Some(selection) = self.row_position_selection() {
                splits = selection.select_append_splits(splits, |split| {
                    let table = if split.is_fallback() {
                        &fallback.table
                    } else {
                        &self.table
                    };
                    table.schema().core_options().row_tracking_enabled()
                })?;
            }
        }
        if let Some(config) = self.chunk_shuffle() {
            splits = chunk_shuffle_splits(
                &self.table,
                Some(&fallback.table),
                splits,
                config,
                self.shard(),
            )
            .await?;
        }
        Ok(Some(Plan::from_splits(snapshot_id, splits)))
    }

    async fn read_ownership_entries(
        &self,
        snapshot: &Snapshot,
        stats: &mut ScanStats,
    ) -> crate::Result<Vec<ManifestEntry>> {
        let mut pass = ScanStats::default();
        let entries = self
            .read_filtered_manifest_entries(ManifestListSource::Snapshot(snapshot), None, &mut pass)
            .await?;
        stats.accumulate_planning(&pass);
        Ok(entries)
    }
}
