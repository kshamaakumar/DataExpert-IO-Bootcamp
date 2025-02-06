from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkNotebook") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()  # Disabling broadcast join

# Load the datasets
medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches_df = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
medals_matches_players_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# Aggregating medals and medals_matches_players
medals_aggregated = medals_df \
    .join(broadcast(medals_matches_players_df), medals_df["medal_id"] == medals_matches_players_df["medal_id"]) \
    .select(
        medals_matches_players_df["match_id"],
        medals_df["medal_id"],
        medals_df["sprite_uri"],
        medals_df["sprite_left"],
        medals_df["sprite_top"],
        medals_df["sprite_sheet_width"],
        medals_df["sprite_sheet_height"],
        medals_df["sprite_width"],
        medals_df["sprite_height"],
        medals_df["classification"],
        medals_df["description"].alias("medals_description"),
        medals_df["name"].alias("medals_name"),
        medals_df["difficulty"]
    )

# Aggregating maps and matches
maps_aggregated = maps_df \
    .join(broadcast(matches_df), maps_df["mapid"] == matches_df["mapid"]) \
    .select(
        matches_df["match_id"],
        maps_df["mapid"],
        maps_df["name"].alias("maps_name"),
        maps_df["description"].alias("maps_description")
    )

# Joining medals and maps fields
final_result = medals_aggregated \
    .join(maps_aggregated, "match_id") \
    .select(
        "medal_id",
        "mapid",
        "sprite_uri",
        "sprite_left",
        "sprite_top",
        "sprite_sheet_width",
        "sprite_sheet_height",
        "sprite_width",
        "sprite_height",
        "classification",
        "medals_description",
        "medals_name",
        "difficulty",
        "maps_name",
        "maps_description"
    )

final_result.show()