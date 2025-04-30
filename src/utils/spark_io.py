from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

import subprocess
import shlex
import threading

import logging


def create_spark_session_using_connect(
    spark_connect_uri: str,
) -> SparkSession:
    # Initialize Spark session
    logging.info("Creating spark connect session using %s", spark_connect_uri)
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .remote(spark_connect_uri)
        # .config("spark.hadoop.metastore.catalog.default", "hive")
        # .config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)
        # .config("spark.sql.catalogImplementation", "hive")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hive.HiveExternalCatalog")
        # .config("hive.metastore.uris", hive_metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    df = spark.sql(
        "select current_catalog() as catalog, current_database() as database, current_schema() as schema, current_user()as user;"
    ).first()

    logging.info(
        "Catalog: %s, Database: %s, Schema: %s, User: %s",
        df["catalog"],
        df["database"],
        df["schema"],
        df["user"],
    )

    return spark

def create_spark_session(
    warehouse_path: str,
    spark_master_uri: str,
    spark_history_log_dir: str,
    spark_local_dir: str,
    postgres_uri: str,
    # hive_metastore_postgres_db: str,
) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .master(spark_master_uri)
        .config("spark.submit.deployMode", "cluster")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("hive.metastore.warehouse.dir", warehouse_path)
        .config("spark.history.fs.logDirectory", spark_history_log_dir)
        .config("spark.local.dir", spark_local_dir)
        # .config("job.local.dir", spark_local_dir)
        # .config("fs.defaultFS", "file:///")
        # The following settings are from hive-site.xml.
        # These should be effective during spark cluster setup.
        # But they are not. So, we have to force set them here.
        # JDBC connect string for a JDBC metastore
        .config(
            "javax.jdo.option.ConnectionURL",
            postgres_uri,
            # f"jdbc:postgresql://{postgres_host}:5432/{hive_metastore_postgres_db}",
        )
        # Driver class name for a JDBC metastore
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        # Username to use against metastore database
        .config("javax.jdo.option.ConnectionUserName", "hive")
        # Password to use against metastore database
        .config("javax.jdo.option.ConnectionPassword", "hivepass123")
        .config("datanucleus.schema.autoCreateTables", "true")
        .config("hive.metastore.schema.verification", "false")
        # End of configs from hive-site.xml.
        # .config("spark.unsafe.sorter.spill.read.ahead.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def create_spark_session_with_derby_metastore(
    warehouse_path, metastore_path
) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("hive.metastore.warehouse.dir", warehouse_path)
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_path}/metastore_db;create=true",
        )
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={metastore_path}",
        )
        # The following are needed for spark-aws integration
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        # .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.782")
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        # .config("spark.jars.packages", "software.amazon.awssdk:bundle:2.31.21")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.catalog.listDatabases()

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def create_spark_session_wo_aws_integration(
    warehouse_path, metastore_path
) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("hive.metastore.warehouse.dir", warehouse_path)
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_path}/metastore_db;create=true",
        )
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={metastore_path}",
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.catalog.listDatabases()

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def read_spark_table_into_list_of_dict(
    qual_target_table_name: str,
    cur_eff_date: str = "",
    spark: SparkSession = None,
) -> list[dict]:
    # if (not spark) and warehouse_path:
    #     spark = create_spark_session(warehouse_path=warehouse_path)

    if cur_eff_date:
        df = spark.sql(
            f"SELECT * FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
        )
    else:
        df = spark.sql(f"SELECT * FROM {qual_target_table_name};")
    # print("Spark dataframe")
    # df.printSchema()
    # df.show(2)
    records = convert_df_to_list_of_dict(df=df)
    # print("Records")
    # print(records[:2])

    return records


def read_spark_table_into_spark_df(
    qual_table_name: str,
    cur_eff_date: str = "",
    spark: SparkSession = None,
) -> DataFrame:
    # if (not spark) and warehouse_path:
    #     spark = create_spark_session(warehouse_path=warehouse_path)

    if cur_eff_date:
        df = spark.sql(
            f"SELECT * FROM {qual_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
        )
    else:
        df = spark.sql(f"SELECT * FROM {qual_table_name};")

    return df


def read_delim_file_into_spark_df(
    file_path: str,
    delim: str,
    spark: SparkSession = None,
) -> DataFrame:
    # if (not spark) and warehouse_path:
    #     spark = create_spark_session(warehouse_path=warehouse_path)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", delim)
        .option("inferSchema", "true")
        .load(file_path)
    )
    return df


def convert_df_to_list_of_dict(df: DataFrame) -> list[dict]:
    rows = df.collect()
    records = [row.asDict() for row in rows]
    return records


# def convert_df_to_pandas_df(df: DataFrame) -> pd.DataFrame:
#     pdf = df.toPandas()
#     print(pdf.info())
#     print(pdf.head(2))
#     return pdf


def create_empty_df(spark: SparkSession = None):
    # if (not spark) and warehouse_path:
    #     spark = create_spark_session(warehouse_path=warehouse_path)

    df = spark.createDataFrame([], "dummy_column: string")
    return df


def get_json_schema_from_df(df: DataFrame) -> dict[str:any]:
    return df.schema.jsonValue()


def get_struct_schema_from_json(json_schema: dict[str:any]) -> StructType:
    return StructType.fromJson(json_schema)


def spark_submit_with_callback(command: str, callback):
    """
    Submits a Spark application using spark-submit and executes a callback function
    after the process completes.

    Args:
        command (str): The spark-submit command to execute.
        callback (function): A function to call after the spark-submit process finishes.
                           It should accept the return code as an argument.
    """

    # logging.info("Spark submit command string: %s", command)
    spark_submit_command = shlex.split(command, posix=False)
    logging.info("Spark submit command: %s", spark_submit_command)
    try:
        process = subprocess.Popen(
            spark_submit_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        logging.info("Spark application submitted with PID: %d", process.pid)
        # You can later interact with the process using process.poll(), process.wait(), process.stdout, process.stderr
        # while True:
        #     stdout_line = process.stdout.readline()
        #     stderr_line = process.stderr.readline()

        #     if stdout_line:
        #         logging.info(f"stdout: {stdout_line.strip()}")
        #     if stderr_line:
        #         logging.error(f"stderr: {stderr_line.strip()}")

        #     if process.poll() is not None:
        #         # Process finished, check for any remaining output
        #         for line in process.stdout:
        #             logging.info(f"stdout: {line.strip()}")
        #         for line in process.stderr:
        #             logging.error(f"stderr: {line.strip()}")
        #         break  # Exit the loop after handling remaining output

    except FileNotFoundError:
        logging.info("Error: spark-submit not found at %s", spark_submit_command[0])

    def on_exit():
        return_code = process.wait()
        callback(return_code)

    thread = threading.Thread(target=on_exit)
    thread.daemon = True
    thread.start()


def spark_submit_callback(return_code: int):
    if return_code == 0:
        logging.info("Spark job completed successfully!")
    else:
        logging.info("Spark job failed with return code: %d", return_code)


def spark_submit_with_wait(command: str):
    logging.info("Spark submit command string: %s", command)
    spark_submit_command = shlex.split(command, posix=False)
    # print(spark_submit_command)
    logging.info("Spark submit command: %s", spark_submit_command)
    status_code = -1
    try:
        result = subprocess.run(
            spark_submit_command, check=True, capture_output=True, text=True
        )
        logging.info("Spark application submitted successfully.")
        for line in result.stdout.split("\n"):
            logging.info("stdout: %s", line)
        for line in result.stderr.split("\n"):
            logging.info("stderr: %s", line)
        status_code = 0
    except subprocess.CalledProcessError as e:
        logging.info("Error submitting Spark application:")
        logging.info("stdout: %s", e.stdout)
        logging.info("stderr: %s", e.stderr)
        raise
    except FileNotFoundError:
        logging.info("Error: spark-submit not found at %s", spark_submit_command[0])
        raise

    return status_code
