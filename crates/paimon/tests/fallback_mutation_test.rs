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

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{BranchManager, CopyOnWriteMergeWriter, RowIdPlan};
use paimon::Table;
use std::{collections::HashMap, sync::Arc};

async fn setup(name: &str, evolution: bool) -> (Table, Table, Table) {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let path = format!("memory:/{name}");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("data-evolution.enabled", evolution.to_string())
        .option("row-tracking.enabled", evolution.to_string())
        .build()
        .unwrap();
    let schema = TableSchema::new(0, &schema);
    io.mkdirs(&format!("{path}/schema")).await.unwrap();
    io.new_output(&format!("{path}/schema/schema-0"))
        .unwrap()
        .write(serde_json::to_vec(&schema).unwrap().into())
        .await
        .unwrap();
    let main = Table::new(
        io.clone(),
        Identifier::new("db", name),
        path.clone(),
        schema,
        None,
    );
    BranchManager::new(io, path)
        .create_branch("backup")
        .await
        .unwrap();
    let backup = main.copy_with_branch("backup").await.unwrap();
    write(&main, 1, 10).await;
    write(&backup, 2, 20).await;
    let fallback = main.copy_with_options(HashMap::from([(
        "scan.fallback-branch".into(),
        "backup".into(),
    )]));
    (main, backup, fallback)
}
async fn write(table: &Table, id: i32, pt: i32) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("pt", Arc::new(Int32Array::from(vec![pt])) as ArrayRef),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn fallback_cow_targets_only_current_branch_partitions() {
    let (main, backup, fallback) = setup("cow", false).await;
    let main_plan = main.new_read_builder().new_scan().plan().await.unwrap();
    let backup_plan = backup.new_read_builder().new_scan().plan().await.unwrap();
    for partition in [
        None,
        Some(main_plan.splits()[0].partition().to_serialized_bytes()),
        Some(backup_plan.splits()[0].partition().to_serialized_bytes()),
    ] {
        let expected = if partition.as_ref()
            == Some(&backup_plan.splits()[0].partition().to_serialized_bytes())
        {
            0
        } else {
            1
        };
        let writer = CopyOnWriteMergeWriter::new(
            &fallback,
            vec!["id".into()],
            partition.map(|p| std::collections::HashSet::from([p])),
        )
        .await
        .unwrap();
        assert_eq!(writer.file_index().len(), expected);
        for file in writer.file_index() {
            assert_eq!(
                file.file_meta.file_name,
                main_plan.splits()[0].data_files()[0].file_name
            );
        }
    }
}

#[tokio::test]
async fn fallback_row_id_mutation_plan_rejects_backup_worker_splits() {
    let (main, backup, fallback) = setup("row_id_mutation", true).await;
    let plan = fallback.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(
        main.snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        backup
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
    );
    let worker_plan = paimon::table::Plan::from_splits(
        plan.snapshot_id(),
        plan.splits()
            .iter()
            .filter(|s| s.is_fallback())
            .cloned()
            .collect(),
    );
    assert!(!worker_plan.splits().is_empty());
    assert!(RowIdPlan::from_plan(worker_plan.clone()).is_err());
    let builder = fallback.new_write_builder();
    assert!(builder
        .new_update()
        .unwrap()
        .with_plan(worker_plan)
        .is_err());
    let local = main.new_read_builder().new_scan().plan().await.unwrap();
    assert!(RowIdPlan::from_plan(local).is_ok());
}
