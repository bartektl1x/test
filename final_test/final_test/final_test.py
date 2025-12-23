import json
import pytest

from pyspark.sql import SparkSession, Row

from shared.metadata_ingestion import MetadataIngestionPipeline


# ============================================================
# Spark & pipeline fixtures
# ============================================================

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("metadata-ingestion-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def pipeline():
    return MetadataIngestionPipeline(
        base_path="/tmp",
        table_list=["orders"],
    )


# ============================================================
# Bronze – validation & deterministic hashing
# ============================================================

def test_valid_schema_is_accepted_and_hashed(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema=json.dumps({"id": "int", "amount": "double"}),
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is True
    assert row.schema_hash is not None
    assert row.validation_error is None


def test_schema_key_order_does_not_change_hash(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema=json.dumps({"a": "int", "b": "string"}),
        ),
        Row(
            table_name="orders",
            source_system="erp",
            schema=json.dumps({"b": "string", "a": "int"}),
        ),
    ])

    rows = pipeline.bronze_from_df(df).collect()

    assert rows[0].schema_hash == rows[1].schema_hash


def test_invalid_json_is_rejected(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema="{not_json}",
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is False
    assert row.schema_hash is None
    assert row.validation_error is not None


def test_empty_schema_is_rejected(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema=json.dumps({}),
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is False
    assert row.schema_hash is None
    assert "schema violates ingestion contract" in row.validation_error


def test_null_schema_is_rejected(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema=None,
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is False
    assert row.schema_hash is None
    assert row.validation_error == "schema is null"


def test_missing_table_name_is_rejected(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name=None,
            source_system="erp",
            schema=json.dumps({"id": "int"}),
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is False
    assert row.validation_error == "table_name is null"


def test_missing_source_system_is_rejected(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system=None,
            schema=json.dumps({"id": "int"}),
        )
    ])

    row = pipeline.bronze_from_df(df).collect()[0]

    assert row.is_valid is False
    assert row.validation_error == "source_system is null"


# ============================================================
# Schema changes – SCD2 semantics
# ============================================================

def test_schema_changes_emit_only_real_changes(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="A",
            is_valid=True,
            effective_ts="2024-01-01 00:00:00",
            ingestion_ts="2024-01-01 00:00:00",
            file_path="f1",
        ),
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="A",
            is_valid=True,
            effective_ts="2024-01-02 00:00:00",
            ingestion_ts="2024-01-02 00:00:00",
            file_path="f2",
        ),
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="B",
            is_valid=True,
            effective_ts="2024-01-03 00:00:00",
            ingestion_ts="2024-01-03 00:00:00",
            file_path="f3",
        ),
    ])

    result = pipeline.schema_changes(df).collect()

    assert [r.schema_hash for r in result] == ["A", "B"]


def test_schema_changes_support_rollback(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="A",
            is_valid=True,
            effective_ts="2024-01-01",
            ingestion_ts="2024-01-01",
            file_path="f1",
        ),
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="B",
            is_valid=True,
            effective_ts="2024-01-02",
            ingestion_ts="2024-01-02",
            file_path="f2",
        ),
        Row(
            table_name="orders",
            source_system="erp",
            schema_hash="A",
            is_valid=True,
            effective_ts="2024-01-03",
            ingestion_ts="2024-01-03",
            file_path="f3",
        ),
    ])

    result = pipeline.schema_changes(df).collect()

    assert [r.schema_hash for r in result] == ["A", "B", "A"]


# ============================================================
# Quarantine
# ============================================================

def test_quarantine_contains_only_invalid_rows(spark, pipeline):
    df = spark.createDataFrame([
        Row(
            table_name="orders",
            is_valid=True,
            validation_error=None,
            file_path="f1",
            ingestion_ts="2024-01-01",
        ),
        Row(
            table_name="orders",
            is_valid=False,
            validation_error="schema invalid",
            file_path="f2",
            ingestion_ts="2024-01-01",
        ),
    ])

    rows = pipeline.quarantine(df).collect()

    assert len(rows) == 1
    assert rows[0].file_path == "f2"
    assert rows[0].validation_error == "schema invalid"


# ============================================================
# Governance – unconfigured tables
# ============================================================

def test_detect_unconfigured_tables_filters_configured(
    spark,
    pipeline,
    monkeypatch,
):
    input_df = spark.createDataFrame([
        Row(table_name="orders"),
        Row(table_name="customers"),
        Row(table_name="payments"),
    ])

    class FakeReadStream:
        def format(self, *_):
            return self

        def option(self, *_):
            return self

        def load(self, *_):
            return input_df

    monkeypatch.setattr(spark, "readStream", FakeReadStream())

    result = pipeline.detect_unconfigured_tables(spark).collect()
    detected = {r.table_name for r in result}

    assert detected == {"customers", "payments"}


def test_detect_unconfigured_tables_returns_empty_when_all_configured(
    spark,
    pipeline,
    monkeypatch,
):
    input_df = spark.createDataFrame([
        Row(table_name="orders"),
    ])

    class FakeReadStream:
        def format(self, *_):
            return self

        def option(self, *_):
            return self

        def load(self, *_):
            return input_df

    monkeypatch.setattr(spark, "readStream", FakeReadStream())

    result = pipeline.detect_unconfigured_tables(spark).collect()

    assert result == []
