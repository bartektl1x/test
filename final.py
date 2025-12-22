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
    expr,
)
from pyspark.sql.types import (
    MapType,
    StringType,
)
from pyspark.sql.window import Window


class MetadataIngestionPipeline:
    """
    Production-grade metadata schema ingestion.

    CONTRACT:
    - `schema` column MUST be a JSON string
    - JSON must represent a FLAT map<string, string>
    - Key order is irrelevant (semantic hashing)
    - Invalid rows NEVER crash the stream (quarantine)

    RESPONSIBILITY:
    - Ingest logical schemas
    - Validate ingestion contract
    - Canonicalize + hash schemas
    - Track schema changes (SCD2)

    NON-GOALS:
    - No semantic interpretation of types
    - No StructType building (SchemaRegistry responsibility)
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
        self._schema_shape_valid_col = "_schema_shape_valid"

    # ------------------------------------------------------------------
    # IO layer
    # ------------------------------------------------------------------

    def metadata_paths(self) -> List[str]:
        return [
            f"{self.base_path}/{table}/metadata"
            for table in self.table_list
        ]

    def bronze(self, spark: SparkSession) -> DataFrame:
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

        return self.bronze_from_df(raw)

    # ------------------------------------------------------------------
    # Pure transformation (testable)
    # ------------------------------------------------------------------

    def bronze_from_df(self, df: DataFrame) -> DataFrame:
        # --------------------------------------------------------------
        # 1. Parse JSON (syntax validation)
        # --------------------------------------------------------------

        parsed = df.withColumn(
            self._parsed_schema_col,
            from_json(
                col(self.schema_col).cast(StringType()),
                MapType(StringType(), StringType(), True),
            ),
        )

        # --------------------------------------------------------------
        # 2. Shape validation (flat map<string, string>)
        # --------------------------------------------------------------

        with_shape_validation = parsed.withColumn(
            self._schema_shape_valid_col,
            col(self.schema_col).isNotNull()
            & col(self._parsed_schema_col).isNotNull()
            & (expr(f"size({self._parsed_schema_col})") > 0),
        )

        # --------------------------------------------------------------
        # 3. Canonicalization (deterministic ordering)
        # --------------------------------------------------------------

        canonicalized = with_shape_validation.withColumn(
            self._canonical_schema_col,
            when(
                col(self._schema_shape_valid_col),
                map_from_entries(
                    sort_array(
                        map_entries(col(self._parsed_schema_col))
                    )
                ),
            ),
        )

        # --------------------------------------------------------------
        # 4. Final validation + hashing
        # --------------------------------------------------------------

        validated = (
            canonicalized
            .withColumn(
                "validation_error",
                when(col("table_name").isNull(), lit("table_name is null"))
                .when(col("source_system").isNull(), lit("source_system is null"))
                .when(col(self.schema_col).isNull(), lit("schema is null"))
                .when(
                    ~col(self._schema_shape_valid_col),
                    lit("schema violates ingestion contract (expected flat map<string,string>)"),
                )
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
                ),
            )
        )

        return validated

    # ------------------------------------------------------------------
    # Schema change log (SCD2-ready)
    # ------------------------------------------------------------------

    def schema_changes(self, bronze_df: DataFrame) -> DataFrame:
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
    # Quarantine
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


# shared/schema_registry.py

from typing import Dict, List, Tuple

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    DataType,
)


class SchemaRegistry:
    """
    SchemaRegistry v1 – semantic schema interpreter.

    RESPONSIBILITY:
    - Interpret logical schemas (column_name -> type string)
    - Validate semantic correctness
    - Build Spark StructType

    NON-GOALS (v1):
    - No schema storage
    - No versioning
    - No compatibility checks
    - No ingestion logic
    """

    _TYPE_MAPPING: Dict[str, DataType] = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
    }

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------

    def parse(
        self,
        schema: Dict[str, str],
    ) -> StructType:
        """
        Parse logical schema into Spark StructType.

        Raises:
            ValueError: if schema is semantically invalid
        """
        errors = self.validate(schema)

        if errors:
            raise ValueError(
                "Invalid schema:\n" + "\n".join(f"- {e}" for e in errors)
            )

        fields: List[StructField] = []

        for col_name, type_name in schema.items():
            spark_type = self._resolve_type(type_name)
            fields.append(
                StructField(
                    name=col_name,
                    dataType=spark_type,
                    nullable=True,
                )
            )

        return StructType(fields)

    # ---------------------------------------------------------
    # Validation
    # ---------------------------------------------------------

    def validate(
        self,
        schema: Dict[str, str],
    ) -> List[str]:
        """
        Semantic validation of logical schema.

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        if not schema:
            errors.append("schema is empty")
            return errors

        for col_name, type_name in schema.items():
            if not col_name or not col_name.strip():
                errors.append("column name is empty")

            if not type_name or not type_name.strip():
                errors.append(f"column '{col_name}' has empty type")

            if not self._is_supported_type(type_name):
                errors.append(
                    f"unsupported type '{type_name}' for column '{col_name}'"
                )

        return errors

    # ---------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------

    def _is_supported_type(self, type_name: str) -> bool:
        return type_name.lower() in self._TYPE_MAPPING

    def _resolve_type(self, type_name: str) -> DataType:
        try:
            return self._TYPE_MAPPING[type_name.lower()]
        except KeyError:
            raise ValueError(f"Unsupported type: {type_name}")

