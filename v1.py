from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    sha2,
    to_json,
    lag
)
from pyspark.sql.window import Window


class MetadataIngestionPipeline:
    """
    Shared library class for ingesting and processing table metadata JSONs.

    Responsibilities:
    - Read metadata JSONs for multiple tables (Autoloader compatible)
    - Produce Bronze (append-only)
    - Detect schema changes (SCD2 source)
    - Produce data statistics snapshots
    """

    def __init__(
        self,
        base_path: str,
        table_list: List[str],
        schema_col: str = "schema",
        row_count_col: str = "row_count"
    ):
        self.base_path = base_path.rstrip("/")
        self.table_list = table_list
        self.schema_col = schema_col
        self.row_count_col = row_count_col

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def metadata_paths(self) -> List[str]:
        return [
            f"{self.base_path}/{table}/metadata"
            for table in self.table_list
        ]

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------

    def bronze(self) -> DataFrame:
        """
        Append-only ingestion of metadata JSONs.
        """
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(self.metadata_paths())
            .withColumn("file_path", input_file_name())
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn(
                "schema_hash",
                sha2(to_json(col(self.schema_col)), 256)
            )
            .withColumn(
                "row_count_hash",
                sha2(col(self.row_count_col).cast("string"), 256)
            )
        )

    # ------------------------------------------------------------------
    # Silver: schema changes (SCD2 source)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
        """
        Detect schema changes per table.
        New row is emitted only when schema hash changes.
        """
        w = Window.partitionBy("table_name").orderBy("ingestion_ts")

        return (
            bronze_df
            .withColumn(
                "prev_schema_hash",
                lag("schema_hash").over(w)
            )
            .filter(
                col("prev_schema_hash").isNull() |
                (col("schema_hash") != col("prev_schema_hash"))
            )
            .select(
                "table_name",
                col(self.schema_col).alias("schema"),
                "schema_hash",
                "source_system",
                "ingestion_ts"
            )
        )

    # ------------------------------------------------------------------
    # Silver: stats snapshot (append-only)
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
                "extracted_at",
                "ingestion_ts",
                "source_system"
            )
        )


import dlt
from pyspark.sql.functions import col
from shared.metadata_ingestion import MetadataIngestionPipeline

pipeline = MetadataIngestionPipeline(
    base_path="abfss://<container>@<account>.dfs.core.windows.net/adls",
    table_list=["table1", "table2", "table3"]
)

@dlt.table(name="metadata_bronze")
def metadata_bronze():
    return pipeline.bronze()


@dlt.view(name="schema_changes")
def schema_changes():
    return pipeline.schema_changes(dlt.read("metadata_bronze"))


@dlt.table(name="metadata_schema_silver")
def metadata_schema_silver():
    return dlt.read("schema_changes")


dlt.apply_changes(
    target="metadata_schema_silver",
    source="schema_changes",
    keys=["table_name"],
    sequence_by=col("ingestion_ts"),
    stored_as_scd_type=2
)


@dlt.table(name="metadata_stats_silver")
def metadata_stats_silver():
    return pipeline.stats(dlt.read("metadata_bronze"))
