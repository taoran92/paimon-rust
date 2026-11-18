# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Partition fallback must not mix branch-local mutation or index coordinates."""

import json
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import GlobalIndexResult, PaimonCatalog, SQLContext


def _write(table, ids, vectors, partitions):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.record_batch([ids, vectors, partitions], schema=pa.schema([
            ("id", pa.int32()), ("embedding", pa.list_(pa.float32())), ("pt", pa.int32())])))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _rows(table):
    builder = table.new_read_builder()
    return [row for batch in builder.new_read().read(builder.new_scan().plan().splits())
            for row in batch.to_pylist()]


def _branches(tmp_path, evolution):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    options = (" WITH ('bucket'='-1', 'data-evolution.enabled'='true', 'row-tracking.enabled'='true', "
               "'deletion-vectors.enabled'='true', 'global-index.enabled'='true', "
               "'vector-index.search-mode'='full')") if evolution else ""
    ctx.sql("CREATE TABLE paimon.db.t (id INT, embedding FLOAT[], pt INT) PARTITIONED BY (pt)" + options)
    ctx.sql("CALL sys.create_branch(table => 'db.t', branch => 'backup')")
    main = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("db.t")
    backup = main.copy({"branch": "backup"})
    # Each independent branch allocates row ID 0. Backup precedes main so a
    # snapshot-pinned read also sees backup's conflicting row ID.
    _write(backup, [2], [[0, 1]], [20])
    _write(main, [1], [[1, 0]], [10])
    return ctx, main, backup


@pytest.fixture
def branches(tmp_path):
    return _branches(tmp_path, True)


@pytest.fixture(params=[False, True], ids=["append", "evolution"])
def mutation_branches(tmp_path, request):
    return _branches(tmp_path, request.param)


def _stored_fallback(main):
    path = Path(main.location()) / "schema" / "schema-0"
    schema = json.loads(path.read_text())
    schema["options"]["scan.fallback-branch"] = "backup"
    path.write_text(json.dumps(schema))


def _mutation(operation):
    return {
        "UPDATE": "UPDATE paimon.db.t SET id = 99 WHERE id = 2",
        "DELETE": "DELETE FROM paimon.db.t WHERE id = 2",
        "MERGE": "MERGE INTO paimon.db.t t USING (SELECT 2 AS id) s ON t.id = s.id "
                 "WHEN MATCHED THEN UPDATE SET id = 99",
    }[operation]


@pytest.mark.parametrize("operation", ["UPDATE", "DELETE", "MERGE"])
@pytest.mark.parametrize("source", ["session", "table"])
def test_fallback_mutations_reject_before_writing(mutation_branches, operation, source):
    ctx, main, backup = mutation_branches
    before = _rows(main), _rows(backup)
    if source == "session":
        ctx.sql("SET 'paimon.scan.fallback-branch'='backup'")
    else:
        _stored_fallback(main)
    with pytest.raises(ValueError, match="with scan.fallback-branch is not supported"):
        ctx.sql(_mutation(operation))
    assert (_rows(main), _rows(backup)) == before
    # A failed mutation leaves the session's fallback reads usable.
    assert sorted(row["id"] for batch in ctx.sql("SELECT id FROM paimon.db.t")
                  for row in batch.to_pylist()) == [1, 2]


@pytest.mark.parametrize("operation", ["UPDATE", "DELETE", "MERGE"])
@pytest.mark.parametrize("noop", ["", " ", "main"])
def test_fallback_noop_override_allows_mutations(mutation_branches, operation, noop):
    ctx, main, backup = mutation_branches
    _stored_fallback(main)
    ctx.sql(f"SET 'paimon.scan.fallback-branch'='{noop}'")
    assert [row for batch in ctx.sql(_mutation(operation)) for row in batch.to_pylist()] == [{"count": 0}]
    assert [row["id"] for row in _rows(main)] == [1]
    assert [row["id"] for row in _rows(backup)] == [2]


def test_empty_pin_survives_fallback_branch_ddl(tmp_path):
    options = {"warehouse": str(tmp_path)}
    ctx = SQLContext()
    ctx.register_catalog("paimon", options)
    ctx.sql("CREATE SCHEMA paimon.db")
    ctx.sql("CREATE TABLE paimon.db.t (id INT, pt INT) PARTITIONED BY (pt)")
    ctx.sql("CALL sys.create_branch(table => 'db.t', branch => 'primary')")
    catalog = PaimonCatalog(options)

    def load_primary():
        primary = catalog.get_table("db.t").copy({"branch": "primary"})
        return primary.copy({"scan.fallback-branch": "main"})

    def assert_empty(table):
        plan = table.new_read_builder().new_scan().plan()
        assert plan.snapshot_id() is None
        assert plan.splits() == []

    early = load_primary().copy_with_pinned_snapshot(None)
    late = load_primary().copy_with_pinned_snapshot(None)
    assert_empty(early)
    ctx.sql("ALTER TABLE paimon.db.t ADD COLUMN amount INT")
    assert_empty(early)
    assert_empty(late)

    # A live scan must check the schema once main has a data snapshot, even at LIMIT 0.
    ctx.sql("INSERT INTO paimon.db.t VALUES (1, 2, 3)")
    with pytest.raises(ValueError, match="must have the same row type"):
        load_primary().new_read_builder().with_limit(0).new_scan().plan()


@pytest.mark.parametrize("copied", [False, True])
def test_fallback_resolved_snapshot_preserves_read_schema(mutation_branches, copied):
    from pypaimon_rust.datafusion import Split, Table

    _, main, _ = mutation_branches
    root = Path(main.location())
    schema = json.loads((root / "schema" / "schema-0").read_text())
    schema["id"] += 1
    schema["highestFieldId"] += 1
    schema["fields"].append({"id": schema["highestFieldId"], "name": "extra", "type": "INT"})
    # Both branches gain a nullable column without a new data snapshot.
    for branch in [root, root / "branch" / "branch-backup"]:
        (branch / "schema" / f"schema-{schema['id']}").write_text(json.dumps(schema))
    schema["options"].update({"scan.snapshot-id": "1", "scan.fallback-branch": "backup"})
    selected = (main.copy_with_resolved_schema(json.dumps(schema)) if copied
                else Table.from_resolved_schema(str(root), json.dumps(schema)))
    builder = selected.new_read_builder()
    splits = builder.new_scan().plan().splits()
    for restored in [splits, [Split.deserialize(split.serialize()) for split in splits]]:
        rows = [row for batch in builder.new_read().read(restored) for row in batch.to_pylist()]
        assert sorted((row["id"], row["pt"], row["extra"]) for row in rows) == [
            (1, 10, None), (2, 20, None)]


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("transport", [False, True])
def test_fallback_vector_materialization_keeps_search_branch(branches, batch, transport):
    import pickle

    _, main, _ = branches
    target = main.copy({"scan.fallback-branch": "backup"})
    builder = (target.new_batch_vector_search_builder().with_query_vectors([[1, 0], [0, 1]])
               if batch else target.new_vector_search_builder().with_query_vector([1, 0]))
    builder.with_vector_column("embedding").with_limit(1)
    if transport:
        plan = pickle.loads(pickle.dumps(builder.new_vector_search_scan().scan()))
        reader = builder.new_batch_vector_search_read() if batch else builder.new_vector_search_read()
        results = reader.read_batch_plan(plan) if batch else [reader.read_plan(plan)]
    else:
        results = builder.execute() if batch else [builder.execute()]
    assert len(results) == (2 if batch else 1)
    for index, result in enumerate(results):
        assert set(result.row_ids()) == {0}
        # Default L2 scoring is 1 / (1 + squared_distance).
        assert result.row_ids()[0] == pytest.approx(1.0 if index == 0 else 1.0 / 3.0)
        rows = [row for rb in result.new_read_builder().with_projection(["id", "pt"]).read()
                for row in rb.to_pylist()]
        assert len(rows) == 1 and rows[0]["id"] == 1 and rows[0]["pt"] == 10


@pytest.mark.parametrize("selection", ["ranges", "index"])
def test_fallback_explicit_row_ids_stay_on_current_branch(branches, selection):
    from pypaimon_rust.datafusion import RowIdPlan

    _, main, _ = branches
    builder = main.copy({"scan.fallback-branch": "backup"}).new_read_builder()
    with pytest.raises(ValueError, match="cannot contain fallback splits"):
        RowIdPlan(builder.new_scan().plan())
    if selection == "ranges":
        builder.with_row_ranges([(0, 0)])
        scan = builder.new_scan()
    else:
        scan = builder.new_scan().with_global_index_result(GlobalIndexResult.from_scored_row_ids({0: 1.0}))
    rows = [row for batch in builder.new_read().read(scan.plan().splits()) for row in batch.to_pylist()]
    assert len(rows) == 1 and rows[0]["id"] == 1


def test_fallback_does_not_block_insert(branches):
    ctx, main, backup = branches
    ctx.sql("SET 'paimon.scan.fallback-branch'='backup'")
    ctx.sql("INSERT INTO paimon.db.t (id, embedding, pt) VALUES (3, ARRAY[1.0, 0.0], 30)")
    assert sorted(row["id"] for row in _rows(main)) == [1, 3]
    assert [row["id"] for row in _rows(backup)] == [2]
