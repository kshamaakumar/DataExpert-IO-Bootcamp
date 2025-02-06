from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple
Actors = namedtuple("Actors", "actor_name year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor_name quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    source_data = [
        Actors("Brigitte Bardot", 2001, 'Bad', True),
        Actors("Brigitte Bardot", 2002, 'Bad', True),
        Actors("50 Cent", 2005, 'Bad', True),
        Actors("Aamir Khan", 2014, 'Star', True),
        Actors("Aamir Khan", 2015, 'Star', True),
        Actors("Aamir Khan", 2016, 'Star', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("50 Cent", 'Bad', True, 2005, 2005, 2020),
        ActorScd("Aamir Khan", 'Star', True, 2014, 2016, 2020),
        ActorScd("Brigitte Bardot", 'Bad', True, 2001, 2002, 2020)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)