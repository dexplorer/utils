from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from utils.enums import AppHostPattern, SparkHostPattern

import logging


def create_spark_session_using_connect(
    warehouse_path: str,
    spark_master: str,
    spark_history_log_dir: str,
    spark_local_dir: str,
    postgres_host: str,
) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .remote("sc://172.18.0.7:15002")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


def create_spark_session(
    warehouse_path: str,
    spark_master: str,
    spark_history_log_dir: str,
    spark_local_dir: str,
    postgres_host: str,
) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Data Framework Spark Workflow")
        .master(spark_master)
        .config("spark.submit.deployMode", "client")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("hive.metastore.warehouse.dir", warehouse_path)
        .config("spark.history.fs.logDirectory", spark_history_log_dir)
        .config("job.local.dir", spark_local_dir)
        .config("fs.defaultFS", "file:///")
        # The following settings are from hive-site.xml.
        # These should be effective during spark cluster setup.
        # But they are not. So, we have to force set them here.
        # JDBC connect string for a JDBC metastore
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:postgresql://{postgres_host}:5432/hive_metastore",
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
