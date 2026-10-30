from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    MapType
)

raw_path = (
    "/Volumes/riot/game/raw_files/"
    "champion_16.21.1.json"
)

champion_schema = StructType([
    StructField("type", StringType()),
    StructField("format", StringType()),
    StructField("version", StringType()),
    StructField(
        "data",
        MapType(
            StringType(),
            StructType([
                StructField("id", StringType()),
                StructField("key", StringType()),
                StructField("name", StringType()),
                StructField("title", StringType()),
                StructField("tags", __import__(
                    "pyspark.sql.types",
                    fromlist=["ArrayType"]
                ).ArrayType(StringType()))
            ])
        )
    )
])

raw_df = (
    spark.read
    .option("multiLine", True)
    .schema(champion_schema)
    .json(raw_path)
)

bronze_df = raw_df.withColumn(
    "ingested_at",
    F.current_timestamp()
).withColumn(
    "source",
    F.lit("ddragon_champion")
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("riot.game.bronze_champion_raw")
)

