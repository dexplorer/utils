from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def create_spark_session(warehouse_path) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Spark Loader in Ingestion Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        # The following are needed for spark-aws integration
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.782")
        # .config("spark.jars.packages", "software.amazon.awssdk:bundle:2.31.21")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def read_spark_table_into_list_of_dict(
    qual_target_table_name: str,
    cur_eff_date: str = "",
    spark: SparkSession = None,
    warehouse_path: str = "",
) -> list[dict]:
    if (not spark) and warehouse_path:
        spark = create_spark_session(warehouse_path=warehouse_path)

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
    warehouse_path: str = "",
) -> DataFrame:
    if (not spark) and warehouse_path:
        spark = create_spark_session(warehouse_path=warehouse_path)

    if cur_eff_date:
        df = spark.sql(
            f"SELECT * FROM {qual_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
        )
    else:
        df = spark.sql(f"SELECT * FROM {qual_table_name};")

    return df


def read_delim_file_into_spark_df(
    file_path: str, delim: str, spark: SparkSession = None, warehouse_path: str = ""
) -> DataFrame:
    if (not spark) and warehouse_path:
        spark = create_spark_session(warehouse_path=warehouse_path)

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


def create_empty_df(spark: SparkSession = None, warehouse_path: str = ""):
    if (not spark) and warehouse_path:
        spark = create_spark_session(warehouse_path=warehouse_path)

    df = spark.createDataFrame([], "dummy_column: string")
    return df


def get_json_schema_from_df(df: DataFrame) -> dict[str:any]:
    return df.schema.jsonValue()


def get_struct_schema_from_json(json_schema: dict[str:any]) -> StructType:
    return StructType.fromJson(json_schema)
