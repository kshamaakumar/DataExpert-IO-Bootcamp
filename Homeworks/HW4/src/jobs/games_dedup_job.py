from pyspark.sql import SparkSession

query = """

WITH deduped AS (
    SELECT *,
           CAST(ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS BIGINT) AS row_num
    FROM game_details
)
SELECT *
FROM deduped
WHERE row_num = 1;

"""


def do_games_dedup_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("games_dedup") \
      .getOrCreate()
    output_df = do_games_dedup_transformation(spark, spark.table("games"))
    output_df.write.mode("overwrite").insertInto("games_dedup")