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

//! Tests for COUNT(*) pushdown optimization via statistics.
//!
//! DataFusion's `aggregate_statistics` optimizer rule can replace a
//! scan+aggregate subtree with a literal projection when the source
//! reports `Precision::Exact(num_rows)` from `statistics_from_inputs()`.
//!
//! These tests verify that:
//! - COUNT(*) pushdown succeeds when statistics are exact (no PaimonTableScan in plan)
//! - COUNT(*) pushdown fails (falls back to scanning) when statistics are inexact

mod common;

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, SchemaChange};
use paimon::Catalog;
use paimon_datafusion::SQLContext;

/// Creates a test sql_context with a table ready for inserts.
async fn setup_table(schema_sql: &str) -> (tempfile::TempDir, SQLContext) {
    let (tmp, sql_context) = common::setup_sql_context().await;
    sql_context
        .sql(&format!("CREATE TABLE paimon.test_db.t {schema_sql}"))
        .await
        .expect("CREATE TABLE should succeed");
    (tmp, sql_context)
}

/// Creates a test sql_context with a partitioned table ready for inserts.
async fn setup_partitioned_table(
    columns: &str,
    partition_cols: &str,
) -> (tempfile::TempDir, SQLContext) {
    let (tmp, sql_context) = common::setup_sql_context().await;
    sql_context
        .sql(&format!(
            "CREATE TABLE paimon.test_db.t ({columns}) PARTITIONED BY ({partition_cols})"
        ))
        .await
        .expect("CREATE TABLE should succeed");
    (tmp, sql_context)
}

/// Checks if the physical plan contains a PaimonTableScan (meaning data would be scanned).
fn plan_contains_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    plan_str.contains("PaimonTableScan")
}

/// Creates a physical plan for the given SQL and checks if COUNT was pushed down.
/// Returns Ok(plan) if pushdown succeeded (no scan in plan), Err(plan) if it fell back to scanning.
async fn verify_count_pushdown(
    sql_context: &SQLContext,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>> {
    let df = sql_context.sql(sql).await.expect("Query should succeed");
    let plan = df
        .create_physical_plan()
        .await
        .expect("Physical plan should succeed");

    if plan_contains_scan(&plan) {
        Err(plan)
    } else {
        Ok(plan)
    }
}

/// Executes a COUNT(*) query and returns the count value.
async fn run_count_query(sql_context: &SQLContext, sql: &str) -> i64 {
    let batches = sql_context
        .sql(sql)
        .await
        .expect("Query should succeed")
        .collect()
        .await
        .expect("Collect should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "COUNT(*) should return exactly one row");

    let count_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT should return Int64");

    count_array.value(0)
}

// ============================================================================
// Test: COUNT(*) without filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_no_filter_pushes_down() {
    let (_tmp, sql_context) = setup_table("(id INT, value INT)").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) should push down (no filter)");

    let count = run_count_query(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 3, "COUNT(*) should return 3");
}

#[tokio::test]
async fn test_fallback_count_column_after_drop_and_add() {
    let (_tmp, catalog) = common::create_test_env();
    let sql_context = common::create_sql_context(catalog.clone()).await;
    for sql in [
        "CREATE SCHEMA paimon.test_db",
        "CREATE TABLE paimon.test_db.t (id INT, pt INT, age INT) PARTITIONED BY (pt)",
        "INSERT INTO paimon.test_db.t VALUES (1, 10, 111)",
        "CALL sys.create_branch(table => 'test_db.t', branch => 'backup')",
    ] {
        common::exec(&sql_context, sql).await;
    }
    let main = catalog
        .get_table(&Identifier::new("test_db", "t"))
        .await
        .unwrap();
    let backup = main.copy_with_branch("backup").await.unwrap();
    let builder = backup.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
        ("pt", Arc::new(Int32Array::from(vec![20]))),
        ("age", Arc::new(Int32Array::from(vec![222]))),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Evolve only backup's schema, leaving its files at schema 0, which has
    // the same numeric ID as main's current schema but a different read view.
    let mut schema = backup.schema().clone();
    for change in [
        SchemaChange::drop_column("age".into()),
        SchemaChange::add_column("age".into(), DataType::Int(IntType::new())),
    ] {
        schema = schema.apply_changes(vec![change]).unwrap();
        backup
            .file_io()
            .new_output(&backup.schema_manager().schema_path(schema.id()))
            .unwrap()
            .write(serde_json::to_vec(&schema).unwrap().into())
            .await
            .unwrap();
    }
    assert_eq!(main.schema().id(), 0);
    assert_eq!(schema.id(), 2);
    assert_ne!(main.schema().fields()[2].id(), schema.fields()[2].id());

    common::exec(&sql_context, "SET 'paimon.scan.fallback-branch' = 'backup'").await;
    let batches = sql_context
        .sql("SELECT age FROM paimon.test_db.t ORDER BY id")
        .await
        .unwrap()
        .collect()
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
        })
        .collect();
    assert_eq!(ages, vec![Some(111), None]);
    let sql = "SELECT COUNT(age) FROM paimon.test_db.t";
    assert_eq!(run_count_query(&sql_context, sql).await, 1);
    assert!(
        verify_count_pushdown(&sql_context, sql).await.is_err(),
        "fallback column counts must not use main's manifest column statistics"
    );
    let sql = "SELECT COUNT(*) FROM paimon.test_db.t";
    assert_eq!(run_count_query(&sql_context, sql).await, 2);
    verify_count_pushdown(&sql_context, sql)
        .await
        .expect("fallback split row counts remain exact");
}

// ============================================================================
// Test: COUNT(*) with data filter should NOT push down (must scan)
// ============================================================================

#[tokio::test]
async fn test_count_star_with_data_filter_does_not_push_down() {
    let (_tmp, sql_context) = setup_table("(id INT, value INT)").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id > 1",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with data filter should NOT push down - must scan data"
    );

    let count = run_count_query(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id > 1",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) with equality filter on non-partition column should NOT push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_non_partition_equality_does_not_push_down() {
    let (_tmp, sql_context) = setup_table("(id INT, value INT)").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id = 2",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with non-partition equality filter should NOT push down"
    );

    let count = run_count_query(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id = 2",
    )
    .await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) on table with single row should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_single_row_pushes_down() {
    let (_tmp, sql_context) = setup_table("(id INT, value INT)").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) should push down");

    let count = run_count_query(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) on empty table should push down (return 0)
// ============================================================================

#[tokio::test]
async fn test_count_star_empty_table_pushes_down() {
    let (_tmp, sql_context) = setup_table("(id INT, value INT)").await;

    verify_count_pushdown(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) on empty table should push down");

    let count = run_count_query(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 0, "COUNT(*) should return 0");
}

// ============================================================================
// Test: COUNT(*) with exact partition filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_partition_filter_pushes_down() {
    let (_tmp, sql_context) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01'), (3, 30, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01'",
    )
    .await
    .expect("COUNT(*) with partition filter should push down");

    let count = run_count_query(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01'",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) with mixed partition + data filter should NOT push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_mixed_partition_data_filter_does_not_push_down() {
    let (_tmp, sql_context) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01'), (3, 30, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01' AND value > 15",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with mixed partition + data filter should NOT push down"
    );

    let count = run_count_query(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01' AND value > 15",
    )
    .await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) with partition IN filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_partition_in_filter_pushes_down() {
    let (_tmp, sql_context) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02'), (3, 30, '2024-01-03')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt IN ('2024-01-01', '2024-01-02')",
    )
    .await
    .expect("COUNT(*) with partition IN filter should push down");

    let count = run_count_query(
        &sql_context,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt IN ('2024-01-01', '2024-01-02')",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) on partitioned table without filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_partitioned_no_filter_pushes_down() {
    let (_tmp, sql_context) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) without filter should push down");

    let count = run_count_query(&sql_context, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}
