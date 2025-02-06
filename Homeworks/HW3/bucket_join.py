from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, split, max, lit

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SparkNotebook") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medal_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medal_matches_players.csv")

matches.write.bucketBy(16, "match_id").saveAsTable("matches_bucketed")
match_details.write.bucketBy(16, "match_id").saveAsTable("match_details_bucketed")
medal_matches_players.write.bucketBy(16, "match_id").saveAsTable("medal_matches_players_bucketed")

matches_bucketed = spark.table("matches_bucketed")
match_details_bucketed = spark.table("match_details_bucketed")
medal_matches_players_bucketed = spark.table("medal_matches_players_bucketed")

# Perform the bucket join
joined_data = matches_bucketed.join(match_details_bucketed, "match_id")\
    .join(medal_matches_players_bucketed, "match_id")


# Which player averages the most kills per game?
avg_kills_per_game = joined_data.groupBy("player_gamertag")\
    .agg(avg("player_total_kills").alias("avg_kills"))\
    .orderBy(col("avg_kills").desc())

avg_kills_per_game.show(1)


# Which playlist gets played the most?
playlist_play_count = joined_data.groupBy("playlist_id")\
    .agg(count("match_id").alias("game_count"))\
    .orderBy(col("game_count").desc())

playlist_play_count.show(1)


# Which map gets played the most?
map_play_count = joined_data.groupBy("mapid")\
    .agg(count("match_id").alias("game_count"))\
    .orderBy(col("game_count").desc())

map_play_count.show(1)


# Which map do players get the most Killing Spree medals on?
killing_spree_medals = joined_data.filter(col("medal_id") == "Killing Spree")\
    .groupBy("mapid")\
    .agg(sum("count").alias("killing_spree_count"))\
    .orderBy(col("killing_spree_count").desc())

killing_spree_medals.show(1)

# sortwithinpartitions
sorted_by_playlist = joined_data.sortWithinPartitions("playlist_id")
sorted_by_map = joined_data.sortWithinPartitions("mapid")

playlist_play_count_sorted = sorted_by_playlist.groupBy("playlist_id")\
    .agg(count("match_id").alias("game_count"))\
    .orderBy(col("game_count").desc())

map_play_count_sorted = sorted_by_map.groupBy("mapid")\
    .agg(count("match_id").alias("game_count"))\
    .orderBy(col("game_count").desc())

playlist_play_count_sorted.show(1)
map_play_count_sorted.show(1)






