from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    when,
    hour,
    from_unixtime,
    trim,
    expr,
    coalesce,
    lit,
    conv,
    regexp_replace,
    upper,
)

from glob import glob
from pathlib import Path

from logger import log


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"

features_path = DATA_DIR / "NUSW-NB15_features.csv"

files = glob(str(DATA_DIR / "UNSW-NB15_[1-4].csv"))

HEX_PATTERN = r'^0[xX][0-9a-fA-F]+$'
PORT_COLUMNS = ["sport", "dsport"]


spark = SparkSession.builder \
    .appName("App") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()


try:

    # loading features metadata file
    log("Loading features metadata file...")

    features = (
        spark.read
        .option("header", "true")
        .csv(str(features_path))
    )

    features = features.toDF(
        *[c.strip() for c in features.columns]
    )

    feature_info = (
        features
        .select("Name", "Type")
        .collect()
    )

    columns = [
        row["Name"]
        for row in feature_info
    ]

    # loading CSV files
    log(f"Loading {len(files)} CSV files...")

    df = (
        spark.read
        .option("header", "false")
        .csv(files)
        .toDF(*columns)
    )

    df = df.toDF(
        *[c.strip() for c in df.columns]
    )

    raw_count = df.count()
    log(f"Rows loaded (before cleanup): {raw_count}")

    # removing empty strings
    df = df.select([
        when(trim(col(c)) == "", None)
        .otherwise(trim(col(c)))
        .alias(c)
        for c in df.columns
    ])

    # checking hex values in sport/dsport before casting
    log("Checking for hex values in sport/dsport...")

    for port_col in PORT_COLUMNS:
        hex_count = df.filter(col(port_col).rlike(HEX_PATTERN)).count()
        log(f"Hex values in column '{port_col}': {hex_count}")

    # casting according to the metadata file
    # uses try_cast so it doesn't fail on bad values
    # sport/dsport are skipped here and handled separately below
    log("Casting columns according to the metadata file...")

    for row in feature_info:

        name = row["Name"]
        dtype = row["Type"].lower()

        if name in PORT_COLUMNS:
            continue

        if dtype in ["integer", "float"]:

            df = df.withColumn(
                name,
                expr(f"try_cast(`{name}` as double)")
            )

        elif dtype == "nominal":

            df = df.withColumn(
                name,
                trim(col(name))
            )

    # sport/dsport: hex values -> decimal, otherwise -> normal cast to double
    log("Converting hex values in sport/dsport to decimal format...")

    for port_col in PORT_COLUMNS:
        df = df.withColumn(
            port_col,
            when(
                col(port_col).rlike(HEX_PATTERN),
                conv(
                    regexp_replace(upper(col(port_col)), "0X", ""),
                    16,
                    10
                ).cast("double")
            ).otherwise(
                expr(f"try_cast(`{port_col}` as double)")
            )
        )

    df.printSchema()

    # checking null values
    log("Checking for null values by column...")

    null_counts = (
        df.select([
            sum(
                when(col(c).isNull(), 1)
                .otherwise(0)
            ).alias(c)
            for c in df.columns
        ])
        .collect()[0]
    )

    print("Null values by column:")

    for column, count in zip(df.columns, null_counts):
        print(f"{column:20} {count}")

    # removing invalid rows
    log("Removing invalid rows (negative duration, null IP addresses)...")

    df = df.filter(
        (col("dur").isNull() | (col("dur") >= 0)) &
        col("srcip").isNotNull() &
        col("dstip").isNotNull()
    )

    clean_count = df.count()
    log(f"Rows after cleanup: {clean_count} (removed {raw_count - clean_count})")

    # total number of bytes
    df = df.withColumn(
        "total_bytes",
        coalesce(col("sbytes"), lit(0)) +
        coalesce(col("dbytes"), lit(0))
    )

    # traffic volume category
    log("Adding traffic_volume column...")

    df = df.withColumn(
        "traffic_volume",
        when(col("total_bytes") < 1000, "low")
        .when(col("total_bytes") <= 100000, "medium")
        .otherwise("high")
    )

    # timestamp transformation
    df = df.withColumn(
        "Stime",
        from_unixtime(
            expr("try_cast(Stime as long)")
        )
    )

    # time bucket
    log("Adding time_bucket column...")

    df = df.withColumn(
        "time_bucket",
        when(
            (hour(col("Stime")) >= 8) &
            (hour(col("Stime")) < 18),
            "business_hours"
        )
        .when(
            hour(col("Stime")) >= 18,
            "off_hours"
        )
        .otherwise("night")
    )

    # attack category
    df = df.fillna(
        {
            "attack_cat": "Normal"
        }
    )

    # explicit cast of the label column to int (useful for further SQL analysis)
    df = df.withColumn("Label", expr("try_cast(Label as int)"))

    log("Distribution by attack_cat:")
    df.groupBy("attack_cat").count().orderBy(col("count").desc()).show(truncate=False)

    # saving parquet file
    output_path = BASE_DIR / "data" / "processed"

    log(f"Writing parquet files to {output_path} (partitioned by attack_cat)...")

    (
        df.write
        .mode("overwrite")
        .partitionBy("attack_cat")
        .parquet(str(output_path))
    )

    log("Transformation completed.")


finally:
    spark.stop()