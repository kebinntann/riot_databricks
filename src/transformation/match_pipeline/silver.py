
from pyspark.sql import functions as F

BRONZE_TABLE = "riot.match.bronze_matches"
SILVER_MATCHES = "riot.match.silver_matches"
SILVER_PARTICIPANTS = "riot.match.silver_participants"


def main():
    bronze = spark.read.table(BRONZE_TABLE)

    # Match-level table
    matches = (
        bronze
        .select(
            F.col("metadata.matchId").alias("match_id"),
            F.col("info.gameCreation").alias("game_creation_ms"),
            F.col("info.gameDuration").alias("game_duration_seconds"),
            F.col("info.gameMode").alias("game_mode"),
            F.col("info.queueId").alias("queue_id"),
            F.col("info.gameVersion").alias("game_version"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["match_id"])
    )

    (
        matches.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_MATCHES)
    )

    # Participant-level table
    participants = (
        bronze
        .select(
            F.col("metadata.matchId").alias("match_id"),
            F.explode("info.participants").alias("p"),
        )
        .select(
            "match_id",
            F.col("p.puuid").alias("puuid"),
            F.col("p.participantId").alias("participant_id"),
            F.col("p.championId").alias("champion_id"),
            F.col("p.championName").alias("champion_name"),
            F.col("p.teamId").alias("team_id"),
            F.col("p.win").alias("win"),
            F.col("p.kills").alias("kills"),
            F.col("p.deaths").alias("deaths"),
            F.col("p.assists").alias("assists"),
            F.col("p.goldEarned").alias("gold_earned"),
            F.col("p.totalDamageDealtToChampions")
                .alias("damage_to_champions"),
        )
        .dropDuplicates(["match_id", "participant_id"])
    )

    (
        participants.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_PARTICIPANTS)
    )


if __name__ == "__main__":
    main()
