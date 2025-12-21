# shared/metadata_ingestion.py

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    when,
    lit,
    from_json,
    to_json,
    sha2,
    map_entries,
    sort_array,
    map_from_entries,
)
from pyspark.sql.types import (
    MapType,
    StringType,
)
from pyspark.sql.window import Window


class MetadataIngestionPipeline:
    """
    Production-grade metadata ingestion for Databricks (shared_lib).

    CONTRACT:
    - `schema` column MUST be a JSON string
    - JSON must represent a flat map: column_name -> type
    - Ordering of keys is NOT significant (semantic hashing)
    - Invalid rows NEVER crash the stream, they go to quarantine

    DESIGN GOALS:
    - Streaming-safe (Auto Loader)
    - Deterministic schema hashing (SCD2-ready)
    - No Python UDFs
    - Explicit, documented trade-offs
    """

    REQUIRED_COLUMNS = {"table_name", "source_system"}

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

        self._parsed_schema_col = "_parsed_schema"
        self._canonical_schema_col = "_canonical_schema"

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def metadata_paths(self) -> List[str]:
        return [
            f"{self.base_path}/{table}/metadata"
            for table in self.table_list
        ]

    # ------------------------------------------------------------------
    # Bronze – ingestion + validation (stream-safe)
    # ------------------------------------------------------------------

    def bronze(self, spark: SparkSession) -> DataFrame:
        """
        Ingest metadata JSONs using Auto Loader.
        Performs PER-ROW validation and deterministic schema hashing.
        """

        raw = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(self.metadata_paths())
            .withColumn("file_path", input_file_name())
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn(
                "effective_ts",
                when(
                    col(self.extraction_start_col).isNotNull(),
                    col(self.extraction_start_col).cast("timestamp"),
                ).otherwise(col("ingestion_ts")),
            )
        )

        # Parse schema JSON (string -> map)
        parsed = raw.withColumn(
            self._parsed_schema_col,
            from_json(
                col(self.schema_col).cast(StringType()),
                MapType(StringType(), StringType(), True),
            ),
        )

        # Canonicalize schema: sort map keys deterministically
        canonicalized = parsed.withColumn(
            self._canonical_schema_col,
            when(
                col(self._parsed_schema_col).isNotNull(),
                map_from_entries(
                    sort_array(
                        map_entries(col(self._parsed_schema_col))
                    )
                ),
            ),
        )

        validated = (
            canonicalized
            .withColumn(
                "validation_error",
                when(col("table_name").isNull(), lit("table_name is null"))
                .when(col("source_system").isNull(), lit("source_system is null"))
                .when(col(self.schema_col).isNull(), lit("schema is null"))
                .when(col(self._parsed_schema_col).isNull(), lit("schema is not valid JSON"))
                .otherwise(lit(None)),
            )
            .withColumn(
                "is_valid",
                col("validation_error").isNull(),
            )
            .withColumn(
                "schema_hash",
                when(
                    col("is_valid"),
                    sha2(
                        to_json(col(self._canonical_schema_col)),
                        256,
                    ),
                ).otherwise(lit(None)),
            )
        )

        return validated

    # ------------------------------------------------------------------
    # Schema change log (SCD2-ready)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
        """
        Emits only real schema changes (semantic, deterministic).
        """

        w = (
            Window
            .partitionBy("table_name")
            .orderBy(
                col("effective_ts"),
                col("ingestion_ts"),
                col("file_path"),
            )
        )

        return (
            bronze_df
            .filter(col("is_valid"))
            .withColumn(
                "prev_schema_hash",
                col("schema_hash").lag().over(w),
            )
            .filter(
                col("prev_schema_hash").isNull()
                | (col("schema_hash") != col("prev_schema_hash"))
            )
            .select(
                "table_name",
                col(self._canonical_schema_col).alias("schema"),
                "schema_hash",
                "effective_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Statistics (append-only)
    # ------------------------------------------------------------------

    def stats(self, bronze_df: DataFrame) -> DataFrame:
        return (
            bronze_df
            .filter(col("is_valid"))
            .select(
                "table_name",
                col(self.row_count_col).alias("row_count"),
                "effective_ts",
                "ingestion_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Quarantine (invalid metadata)
    # ------------------------------------------------------------------

    def quarantine(self, bronze_df: DataFrame) -> DataFrame:
        return (
            bronze_df
            .filter(~col("is_valid"))
            .select(
                "table_name",
                "file_path",
                "validation_error",
                "ingestion_ts",
            )
        )

    # ------------------------------------------------------------------
    # Governance – unconfigured tables
    # ------------------------------------------------------------------

    def detect_unconfigured_tables(self, spark: SparkSession) -> DataFrame:
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
