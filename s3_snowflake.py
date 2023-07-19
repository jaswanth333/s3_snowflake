from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Define the S3 bucket and file details
s3_prefix = 's3://snowflake-airflow-elitesniper/covid/'
s3_bucket = None

with DAG('snowflake_s3_with_email_notification_etl',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key=s3_prefix,
        bucket_name=s3_bucket,
        aws_conn_id='aws_s3_conn',
        wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix
        # timeout=60,  # Optional: Timeout for the sensor (in seconds)
        poke_interval=3,  # Optional: Time interval between S3 checks (in seconds)
        )
        
        create_table = SnowflakeOperator(
            task_id = "create_snowflake_table",
            snowflake_conn_id = 'conn_id_snowflake',           
            sql = '''DROP TABLE IF EXISTS covid_db;
                    CREATE OR REPLACE TABLE covid_db(
                    DAY VARCHAR(16777216),
                    MONTH VARCHAR(16777216),
                    YEAR VARCHAR(16777216),
                    CASES VARCHAR(16777216),
                    DEATHS VARCHAR(16777216),
                    COUNTRY VARCHAR(16777216),
                    CODE VARCHAR(16777216),
                    POPULATION VARCHAR(16777216),
                    CONTINENT VARCHAR(16777216),
                    RATE VARCHAR(16777216)
                );
                 '''
        )
        
        copy_csv_into_snowflake_table = SnowflakeOperator(
            task_id = "tsk_copy_csv_into_snowflake_table",
            snowflake_conn_id = 'conn_id_snowflake',
            sql = '''COPY INTO covid.covid_schema.covid_db from @snowflake_airflow FILE_FORMAT = csv_format
                   '''
        )

is_file_in_s3_available >> create_table >> copy_csv_into_snowflake_table 