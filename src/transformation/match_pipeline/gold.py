spark.sql("""
    REFRESH MATERIALIZED VIEW
    riot.match.champion_win_rate
""")