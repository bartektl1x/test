from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    sha2,
    to_json,
    lag,
    coalesce,
)
from pyspark.sql.window import Window


class MetadataIngestionPipeline:
    """
    Shared library for ingesting and governing table metadata JSONs.

    Design principles:
    - Explicit ingestion of configured tables only
    - Controlled wildcard for detecting unconfigured tables
    - Clear separation between:
        * schema change history (SCD2-ready)
        * statistics time series (append-only)
    """

    def __init__(
        self,
        base_path: str,
        table_list: List[str],
        schema_col: str = "schema",
        row_count_col: str = "row_count",
        extraction_start_col: str = "extraction_start_time",
    ):
        self.base_path = base_path.rstrip("/")
        self.table_list = table_list
        self.schema_col = schema_col
        self.row_count_col = row_count_col
        self.extraction_start_col = extraction_start_col

    # ------------------------------------------------------------------
    # Paths (explicit ingestion)
    # ------------------------------------------------------------------

    def metadata_paths(self) -> List[str]:
        """
        Explicit list of metadata paths for configured tables only.
        """
        return [
            f"{self.base_path}/{table}/metadata"
            for table in self.table_list
        ]

    # ------------------------------------------------------------------
    # Bronze: configured tables only
    # ------------------------------------------------------------------

    def bronze(self, spark: SparkSession) -> DataFrame:
        """
        Append-only ingestion of metadata JSONs
        for explicitly configured tables.
        """
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(self.metadata_paths())
            .withColumn("file_path", input_file_name())
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn(
                "effective_ts",
                coalesce(
                    col(self.extraction_start_col).cast("timestamp"),
                    col("ingestion_ts"),
                ),
            )
            .withColumn(
                "schema_hash",
                sha2(to_json(col(self.schema_col)), 256),
            )
        )

    # ------------------------------------------------------------------
    # Silver input: schema change event log (SCD2-ready)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
        """
        Produces an ordered event log of schema changes per table.

        One row = one effective schema change.
        Supports:
        - out-of-order arrival
        - schema rollback (A → B → A)
        """
        w = (
            Window
            .partitionBy("table_name")
            .orderBy("effective_ts")
        )

        return (
            bronze_df
            .withColumn(
                "prev_schema_hash",
                lag("schema_hash").over(w),
            )
            .filter(
                col("prev_schema_hash").isNull()
                | (col("schema_hash") != col("prev_schema_hash"))
            )
            .select(
                "table_name",
                col(self.schema_col).alias("schema"),
                "schema_hash",
                "effective_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Silver: statistics (append-only time series)
    # ------------------------------------------------------------------

    def stats(self, bronze_df: DataFrame) -> DataFrame:
        """
        Append-only statistics snapshots.

        Each row represents a measurement at a point in time.
        No deduplication. No SCD.
        """
        return (
            bronze_df
            .select(
                "table_name",
                col(self.row_count_col).alias("row_count"),
                "effective_ts",
                "ingestion_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Governance: detect unconfigured tables (controlled wildcard)
    # ------------------------------------------------------------------

    def detect_unconfigured_tables(self, spark: SparkSession) -> DataFrame:
        """
        Detects metadata JSONs present on storage
        for tables NOT defined in table_list.

        Intended for:
        - monitoring
        - alerting
        - audit / governance
        """
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{self.base_path}/*/metadata")
            .withColumn("file_path", input_file_name())
            .filter(~col("table_name").isin(self.table_list))
            .select(
                "table_name",
                "file_path",
                current_timestamp().alias("detected_ts"),
            )
        )



import dlt as dp
from pyspark.sql.functions import col
from shared.metadata_ingestion import MetadataIngestionPipeline
pipeline = MetadataIngestionPipeline(
    base_path="abfss://<container>@<account>.dfs.core.windows.net/adls",
    table_list=[
        "table1",
        "table2",
        "table3",
    ],
)


@dp.table(name="metadata_bronze")
def metadata_bronze():
    return pipeline.bronze(spark)


@dp.view(name="metadata_schema_changes")
def metadata_schema_changes():
    return pipeline.schema_changes(
        dp.read("metadata_bronze")
    )


dp.create_streaming_table("metadata_schema_silver")
dp.apply_changes(
    target="metadata_schema_silver",
    source="metadata_schema_changes",
    keys=["table_name"],
    sequence_by=col("effective_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["schema_hash"],
)


@dp.table(name="metadata_stats_silver")
def metadata_stats_silver():
    return pipeline.stats(
        dp.read("metadata_bronze")
    )


@dp.table(name="metadata_unconfigured_tables")
def metadata_unconfigured_tables():
    return pipeline.detect_unconfigured_tables(spark)





import pytest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hypothesis import given, strategies as st

from shared.metadata_ingestion import MetadataIngestionPipeline
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("metadata-pipeline-tests")
        .getOrCreate()
    )


@pytest.fixture
def pipeline():
    return MetadataIngestionPipeline(
        base_path="/tmp/metadata",
        table_list=["table1"],
    )

def bronze_df(spark, rows):
    return spark.createDataFrame(rows)




def test_schema_change_detected(spark, pipeline):
    rows = [
        {
            "table_name": "table1",
            "schema": {"a": "int"},
            "schema_hash": "A",
            "effective_ts": datetime(2024, 1, 1),
            "source_system": "sys",
        },
        {
            "table_name": "table1",
            "schema": {"a": "int", "b": "string"},
            "schema_hash": "B",
            "effective_ts": datetime(2024, 1, 2),
            "source_system": "sys",
        },
    ]

    df = bronze_df(spark, rows)
    result = pipeline.schema_changes(df).collect()

    assert len(result) == 2
    assert result[0]["schema_hash"] == "A"
    assert result[1]["schema_hash"] == "B"


def test_out_of_order_input(spark, pipeline):
    rows = [
        {
            "table_name": "table1",
            "schema": {"a": "int", "b": "string"},
            "schema_hash": "B",
            "effective_ts": datetime(2024, 1, 2),
            "source_system": "sys",
        },
        {
            "table_name": "table1",
            "schema": {"a": "int"},
            "schema_hash": "A",
            "effective_ts": datetime(2024, 1, 1),
            "source_system": "sys",
        },
    ]

    df = bronze_df(spark, rows)
    result = pipeline.schema_changes(df).orderBy("effective_ts").collect()

    assert [r.schema_hash for r in result] == ["A", "B"]



def test_schema_rollback(spark, pipeline):
    rows = [
        {
            "table_name": "table1",
            "schema": {"a": "int"},
            "schema_hash": "A",
            "effective_ts": datetime(2024, 1, 1),
            "source_system": "sys",
        },
        {
            "table_name": "table1",
            "schema": {"a": "int", "b": "string"},
            "schema_hash": "B",
            "effective_ts": datetime(2024, 1, 2),
            "source_system": "sys",
        },
        {
            "table_name": "table1",
            "schema": {"a": "int"},
            "schema_hash": "A",
            "effective_ts": datetime(2024, 1, 3),
            "source_system": "sys",
        },
    ]

    df = bronze_df(spark, rows)
    result = pipeline.schema_changes(df).collect()

    assert [r.schema_hash for r in result] == ["A", "B", "A"]


def test_stats_append_only(spark, pipeline):
    rows = [
        {
            "table_name": "table1",
            "row_count": 100,
            "effective_ts": datetime(2024, 1, 1),
            "ingestion_ts": datetime(2024, 1, 1),
            "source_system": "sys",
        },
        {
            "table_name": "table1",
            "row_count": 120,
            "effective_ts": datetime(2024, 1, 2),
            "ingestion_ts": datetime(2024, 1, 2),
            "source_system": "sys",
        },
    ]

    df = bronze_df(spark, rows)
    result = pipeline.stats(df).collect()

    assert len(result) == 2
    assert result[0]["row_count"] == 100
    assert result[1]["row_count"] == 120


def test_detect_unconfigured_tables(spark):
    pipeline = MetadataIngestionPipeline(
        base_path="/tmp/metadata",
        table_list=["table1"],
    )

    rows = [
        {"table_name": "table1"},
        {"table_name": "table2"},
    ]

    df = spark.createDataFrame(rows)
    unconfigured = df.filter(~col("table_name").isin(pipeline.table_list)).collect()

    assert len(unconfigured) == 1
    assert unconfigured[0]["table_name"] == "table2"


@given(
    st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=5),
            st.integers(min_value=0, max_value=10),
        ),
        min_size=1,
        max_size=20,
    )
)
def test_property_schema_changes_ordering(spark, pipeline, data):
    base = datetime(2024, 1, 1)

    rows = [
        {
            "table_name": "table1",
            "schema": {"v": v},
            "schema_hash": str(v),
            "effective_ts": base + timedelta(days=t),
            "source_system": "sys",
        }
        for t, v in data
    ]

    df = bronze_df(spark, rows)
    result = pipeline.schema_changes(df).orderBy("effective_ts").collect()

    times = [r.effective_ts for r in result]
    assert times == sorted(times)
