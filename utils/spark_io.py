from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import col
import pandas as pd 

def create_spark_session(warehouse_path) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Spark Loader in Ingestion Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

def read_spark_table_into_list_of_dict(qual_target_table_name: str, cur_eff_date: str = '', spark: SparkSession = None) -> list[dict]:
    if not spark:
        spark = create_spark_session(warehouse_path=sc.warehouse_path)

    if cur_eff_date:
        df = spark.sql(
            f"SELECT * FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
        )
    else:
        df = spark.sql(
            f"SELECT * FROM {qual_target_table_name};"
        )
    print("Spark dataframe")
    df.printSchema()
    df.show(2)
    records = df.collect()
    print(type(records))
    print(records[:2])

    # pdf = df.toPandas()
    # print(pdf.info())
    # print(pdf.head(2))

    return records 
