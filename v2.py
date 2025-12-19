# shared/metadata_ingestion.py

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    sha2,
    to_json,
    regexp_extract,
    lit,
    sort_array
)


class MetadataIngestionPipeline:
    """
    Shared library for ingesting table-level metadata JSONs.

    Assumptions:
    - Path layout: <base>/<table_name>/metadata/*.json
    - JSON contains:
        - schema (array or struct, canonicalizable)
        - row_count (long)
        - extracted_at (timestamp, optional)
        - source_system (string, optional)
    """

    def __init__(
        self,
        base_path: str,
        schema_col: str = "schema",
        row_count_col: str = "row_count",
        source_system_default: Optional[str] = None,
    ):
        self.base_path = base_path.rstrip("/")
        self.schema_col = schema_col
        self.row_count_col = row_count_col
        self.source_system_default = source_system_default

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _table_name_from_path(self) -> str:
        return regexp_extract(
            input_file_name(),
            r".*/([^/]+)/metadata/.*",
            1
        )

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------

    def bronze(self) -> DataFrame:
        """
        Append-only ingestion of metadata JSONs using Autoloader.
        """
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option(
                "cloudFiles.schemaLocation",
                f"{self.base_path}/_schemas/metadata"
            )
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{self.base_path}/*/metadata")
        )

        df = (
            df
            .withColumn("table_name", self._table_name_from_path())
            .withColumn("file_path", input_file_name())
            .withColumn("ingestion_ts", current_timestamp())
        )

        if self.source_system_default:
            df = df.withColumn(
                "source_system",
                col("source_system").cast("string")
                .otherwise(lit(self.source_system_default))
            )

        return (
            df
            .withColumn(
                "schema_hash",
                sha2(
                    to_json(sort_array(col(self.schema_col))),
                    256
                )
            )
            .withColumn(
                "row_count_hash",
                sha2(col(self.row_count_col).cast("string"), 256)
            )
        )

    # ------------------------------------------------------------------
    # Silver: schema history (SCD2 source)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
        """
        Emits one row per unique (table_name, schema_hash).
        Safe for streaming (no windows, no state explosion).
        """
        return (
            bronze_df
            .select(
                "table_name",
                col(self.schema_col).alias("schema"),
                "schema_hash",
                "source_system",
                "ingestion_ts"
            )
            .dropDuplicates(["table_name", "schema_hash"])
        )

    # ------------------------------------------------------------------
    # Silver: statistics snapshot
    # ------------------------------------------------------------------

    def stats(self, bronze_df: DataFrame) -> DataFrame:
        """
        Append-only statistics time series.
        """
        return (
            bronze_df
            .select(
                "table_name",
                col(self.row_count_col).alias("row_count"),
                "row_count_hash",
                col("extracted_at").cast("timestamp"),
                "ingestion_ts",
                "source_system"
            )
        )




import dlt
from pyspark.sql.functions import col
from shared.metadata_ingestion import MetadataIngestionPipeline

pipeline = MetadataIngestionPipeline(
    base_path="abfss://<container>@<account>.dfs.core.windows.net/adls",
    source_system_default="landing"
)

@dlt.table(name="metadata_bronze")
def metadata_bronze():
    return pipeline.bronze()


@dlt.view(name="metadata_schema_changes")
def metadata_schema_changes():
    return pipeline.schema_changes(
        dlt.read("metadata_bronze")
    )


dlt.apply_changes(
    target="metadata_schema_silver",
    source="metadata_schema_changes",
    keys=["table_name", "schema_hash"],
    sequence_by=col("ingestion_ts"),
    stored_as_scd_type=2
)


@dlt.table(name="metadata_stats_silver")
def metadata_stats_silver():
    return pipeline.stats(
        dlt.read("metadata_bronze")
    )
