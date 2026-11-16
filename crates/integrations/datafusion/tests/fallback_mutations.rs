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

mod common;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, UInt64Array};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::spec::SchemaChange;
use paimon::Catalog;
use std::sync::Arc;

async fn assert_disabled_fallback_mutation(update: bool) {
    for evolution in [false, true] {
        for noop in ["", " ", "main"] {
            let (_tmp, catalog) = common::create_test_env();
            let ctx = common::create_sql_context(catalog.clone()).await;
            let options = if evolution {
                " WITH ('data-evolution.enabled'='true', 'row-tracking.enabled'='true', 'deletion-vectors.enabled'='true')"
            } else {
                ""
            };
            for sql in [
                "CREATE SCHEMA paimon.test_db".to_string(),
                format!(
                    "CREATE TABLE paimon.test_db.t (id INT, pt INT) PARTITIONED BY (pt){options}"
                ),
                "INSERT INTO paimon.test_db.t (id, pt) VALUES (1, 10)".into(),
                "CALL sys.create_branch(table => 'test_db.t', branch => 'backup')".into(),
            ] {
                ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            }
            let main = catalog
                .get_table(&Identifier::new("test_db", "t"))
                .await
                .unwrap();
            let backup = main.copy_with_branch("backup").await.unwrap();
            let wb = backup.new_write_builder();
            let mut w = wb.new_write().unwrap();
            w.write_arrow_batch(
                &RecordBatch::try_from_iter([
                    ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
                    ("pt", Arc::new(Int32Array::from(vec![20])) as ArrayRef),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
            wb.new_commit()
                .commit(w.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let schema = main
                .schema()
                .apply_changes(vec![SchemaChange::set_option(
                    "scan.fallback-branch".into(),
                    "backup".into(),
                )])
                .unwrap();
            main.file_io()
                .new_output(&main.schema_manager().schema_path(schema.id()))
                .unwrap()
                .write(serde_json::to_vec(&schema).unwrap().into())
                .await
                .unwrap();
            ctx.sql(&format!("SET 'paimon.scan.fallback-branch' = '{noop}'"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            for (id, expected) in [(2, 0), (1, 1)] {
                let sql = if update {
                    format!("UPDATE paimon.test_db.t SET id = 99 WHERE id = {id}")
                } else {
                    format!("DELETE FROM paimon.test_db.t WHERE id = {id}")
                };
                let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                assert_eq!(
                    result[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                    expected,
                    "{sql}, evolution={evolution}, noop={noop:?}"
                );
            }
            let rows = ctx
                .sql("SELECT id FROM paimon.test_db.t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let ids: Vec<_> = rows
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(ids, if update { vec![99] } else { vec![] });
            let builder = backup.new_read_builder();
            let plan = builder.new_scan().plan().await.unwrap();
            let rows: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[2]
            );
        }
    }
}

#[tokio::test]
async fn disabled_fallback_update_uses_current_branch() {
    assert_disabled_fallback_mutation(true).await;
}

#[tokio::test]
async fn disabled_fallback_delete_uses_current_branch() {
    assert_disabled_fallback_mutation(false).await;
}
