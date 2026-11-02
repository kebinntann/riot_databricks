from pyspark.sql import functions as F

bronze_df = spark.table("riot.game.bronze_champion_raw")

silver_df = (
    bronze_df
    .select(
        "version",
        "ingested_at",
        F.explode("data").alias("champion_id", "champion")
    )
    .select(
        "version",
        "ingested_at",
        "champion_id",
        F.col("champion.key").cast("int").alias("champion_key"),
        F.trim(F.col("champion.name")).alias("name"),
        F.trim(F.col("champion.title")).alias("title"),
        F.col("champion.tags").alias("tags")
    )
)

assert silver_df.count() > 0, "No champions found"

assert (
    silver_df
    .filter(
        F.col("champion_id").isNull()
        | F.col("name").isNull()
        | F.col("champion_key").isNull()
    )
    .count() == 0
), "Required champion fields are missing"

assert (
    silver_df
    .groupBy("version", "champion_id")
    .count()
    .filter(F.col("count") > 1)
    .count() == 0
), "Duplicate champions found"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("riot.game.silver_champions")
)