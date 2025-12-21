# shared/metadata_ingestion.py

from typing import Any, Dict, List
import json
import hashlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    udf,
    when,
)
from pyspark.sql.types import (
    StringType,
    StructType,
    MapType,
)
from pyspark.sql.window import Window


class MetadataIngestionPipeline:
    """
    Shared library for ingesting and governing table metadata JSONs.

    Guarantees:
    - Deterministic, semantic schema hashing
    - Explicit input data contract with fail-fast validation
    - Single ingestion source (wildcard)
    - Deterministic schema change detection (SCD2-ready)
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

    # ------------------------------------------------------------------
    # Input contract validation (fail-fast)
    # ------------------------------------------------------------------

    def _validate_contract(self, df: DataFrame) -> None:
        """
        Validates minimal input contract.
        Fails fast on structural violations.
        """
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if self.schema_col not in df.columns:
            raise ValueError(f"Missing required column '{self.schema_col}'")

        schema_type = df.schema[self.schema_col].dataType
        if not isinstance(schema_type, (StructType, MapType)):
            raise ValueError(
                f"Column '{self.schema_col}' must be struct or map, "
                f"got {schema_type}"
            )

        if df.filter(col("table_name").isNull()).limit(1).count() > 0:
            raise ValueError("Column 'table_name' contains null values")

        if self.row_count_col in df.columns:
            if df.filter(col(self.row_count_col) < 0).limit(1).count() > 0:
                raise ValueError("Column 'row_count' must be >= 0")

    # ------------------------------------------------------------------
    # Canonical schema hashing
    # ------------------------------------------------------------------

    @staticmethod
    def _canonicalize(obj: Any) -> Any:
        """
        Recursively normalizes schema structures into
        order-insensitive canonical form.
        """
        if isinstance(obj, dict):
            return {
                k: MetadataIngestionPipeline._canonicalize(obj[k])
                for k in sorted(obj.keys())
            }

        if isinstance(obj, list):
            return sorted(
                (MetadataIngestionPipeline._canonicalize(v) for v in obj),
                key=lambda x: json.dumps(x, sort_keys=True),
            )

        return obj

    @staticmethod
    def _schema_hash(schema: Dict[str, Any]) -> str:
        """
        Computes deterministic hash of a schema definition.
        """
        if schema is None:
            return None

        canonical = MetadataIngestionPipeline._canonicalize(schema)
        payload = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Bronze ingestion (single wildcard source)
    # ------------------------------------------------------------------

    def bronze(self, spark: SparkSession) -> DataFrame:
        """
        Append-only ingestion of all metadata JSONs.
        Enforces input contract and enriches technical columns.
        """
        schema_hash_udf = udf(self._schema_hash, StringType())

        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{self.base_path}/*/metadata")
        )

        self._validate_contract(df)

        return (
            df
            .withColumn("file_path", input_file_name())
            .withColumn("ingestion_ts", current_timestamp())
            .withColumn(
                "effective_ts",
                when(
                    col(self.extraction_start_col).isNotNull(),
                    col(self.extraction_start_col).cast("timestamp"),
                ).otherwise(col("ingestion_ts")),
            )
            .withColumn(
                "is_configured",
                col("table_name").isin(self.table_list),
            )
            .withColumn(
                "schema_hash",
                schema_hash_udf(col(self.schema_col)),
            )
        )

    # ------------------------------------------------------------------
    # Schema change event log (SCD2-ready)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
        """
        Deterministic schema change detection.
        One row = one semantic schema change.
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
            .filter(col("is_configured"))
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
                col(self.schema_col).alias("schema"),
                "schema_hash",
                "effective_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Statistics (append-only)
    # ------------------------------------------------------------------

    def stats(self, bronze_df: DataFrame) -> DataFrame:
        """
        Append-only statistics snapshots.
        """
        return (
            bronze_df
            .filter(col("is_configured"))
            .select(
                "table_name",
                col(self.row_count_col).alias("row_count"),
                "effective_ts",
                "ingestion_ts",
                "source_system",
            )
        )

    # ------------------------------------------------------------------
    # Governance: unconfigured tables
    # ------------------------------------------------------------------

    def unconfigured_tables(self, bronze_df: DataFrame) -> DataFrame:
        """
        Metadata for tables not present in configuration.
        """
        return (
            bronze_df
            .filter(~col("is_configured"))
            .select(
                "table_name",
                "file_path",
                current_timestamp().alias("detected_ts"),
            )
        )
