from chispa.dataframe_comparer import *
from ..jobs.games_dedup_job import do_games_dedup_transformation
from collections import namedtuple

GameDetails = namedtuple("GameDetails", "game_id team_id player_id team_name player_name team_city fgm fga")
GameDedup = namedtuple("GameDetails", "game_id team_id player_id team_name player_name team_city fgm fga row_num")


def test_scd_generation(spark):
    source_data = [
        GameDetails("11600001", "1610612744", '2561', "GSW", "David West", "Golden State", 2, 5),
        GameDetails("11600001", "1610612744", '2561', "GSW", "David West", "Golden State", 2, 5),
        GameDetails("11600001", "1610612744", '2561', "GSW", "David West", "Golden State", 2, 5),
        GameDetails("11600004", "1610612761", '201950', "NOP", "Jrue Holiday", "New Orleans", 5, 6),
        GameDetails("11600004", "1610612761", '201950', "NOP", "Jrue Holiday", "New Orleans", 5, 6)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_games_dedup_transformation(spark, source_df)
    expected_data = [
        GameDedup("11600001", "1610612744", '2561', "GSW", "David West", "Golden State", 2, 5, 1),
        GameDedup("11600004", "1610612761", '201950', "NOP", "Jrue Holiday", "New Orleans", 5, 6, 1)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)