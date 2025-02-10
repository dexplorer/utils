from pyspark.sql import SparkSession, DataFrame


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


def read_spark_table_spark_df(
    qual_target_table_name: str,
    cur_eff_date: str = "",
    spark: SparkSession = None,
    warehouse_path: str = "",
) -> DataFrame:
    if (not spark) and warehouse_path:
        spark = create_spark_session(warehouse_path=warehouse_path)

    if cur_eff_date:
        df = spark.sql(
            f"SELECT * FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
        )
    else:
        df = spark.sql(f"SELECT * FROM {qual_target_table_name};")

    return df


def read_delim_file_into_spark_df(
    file_path: str, delim: str, warehouse_path: str = ""
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
