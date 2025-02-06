import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Session


def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sessionized_events_sink_postgres(t_env):
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_sessionization_and_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sessionized_sink = create_sessionized_events_sink_postgres(t_env)

        t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("session")
            ).group_by(
                col("session"),
                col("ip"),
                col("host")
            )\
            .select(
                col("session").start.alias("session_start"),
                col("session").end.alias("session_end"),
                col("ip"),
                col("host"),
                col("host").count.alias("num_events")
            )\
            .execute_insert(sessionized_sink).wait()


        avg_events_query = """
            SELECT host, AVG(num_events) as avg_num_events
            FROM sessionized_events
            WHERE host LIKE '%techcreator%'
            GROUP BY host
        """
        avg_events_result = t_env.execute_sql(avg_events_query)
        print("Average number of events for Tech Creator:")
        avg_events_result.print()

        comparison_query = """
            SELECT host, AVG(num_events) as avg_num_events
            FROM sessionized_events
            WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
            GROUP BY host
        """
        comparison_result = t_env.execute_sql(comparison_query)
        print("Comparison of average events per session across different hosts:")
        comparison_result.print()

    except Exception as e:
        print("Job failed:", str(e))

if __name__ == '__main__':
    log_sessionization_and_aggregation()
