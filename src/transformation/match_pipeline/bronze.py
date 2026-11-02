
from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/riot/match/raw_matches/matches/"

SCHEMA_PATH = (
    "/Volumes/riot/match/raw_matches/_schema/bronze_matches"
)

CHECKPOINT_PATH = (
    "/Volumes/riot/match/raw_matches/_checkpoint/bronze_matches"
)

TARGET_TABLE = "riot.match.bronze_matches"


def main():
    matches_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(SOURCE_PATH)
    )

    bronze_stream = (
        matches_stream
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )

    query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
