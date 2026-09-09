from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg
from pathlib import Path

from logger import log


BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RESULTS_DIR = BASE_DIR / "results"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)


spark = SparkSession.builder \
    .appName("Analysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()


try:

    log(f"Loading processed data from {PROCESSED_DIR}...")

    df = spark.read.parquet(str(PROCESSED_DIR))
    df.createOrReplaceTempView("traffic")

    # 1. Top source IP addresses by number of suspicious connections (label=1)
    log("Query 1: top source IP addresses by suspicious connections...")

    q1 = spark.sql("""
        SELECT srcip, COUNT(*) AS suspicious_connections
        FROM traffic
        WHERE Label = 1
        GROUP BY srcip
        ORDER BY suspicious_connections DESC
        LIMIT 10
    """)
    q1.write.mode("overwrite").parquet(str(RESULTS_DIR / "top_suspicious_ips.parquet"))

    # 2. Most common protocol by attack category
    log("Query 2: most common protocol by attack category...")

    q2 = spark.sql("""
        SELECT attack_cat, proto, cnt FROM (
            SELECT attack_cat, proto, COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (PARTITION BY attack_cat ORDER BY COUNT(*) DESC) AS rn
            FROM traffic
            WHERE Label = 1
            GROUP BY attack_cat, proto
        )
        WHERE rn = 1
        ORDER BY cnt DESC
    """)
    q2.write.mode("overwrite").parquet(str(RESULTS_DIR / "top_protocol_per_attack_cat.parquet"))

    # 3. Attack count over time - rolling count over time windows
    log("Query 3: attack count over time (hourly rolling count)...")

    attacks_per_window = spark.sql("""
        SELECT window(Stime, '1 hour') AS time_window, COUNT(*) AS attack_count
        FROM traffic
        WHERE Label = 1
        GROUP BY window(Stime, '1 hour')
        ORDER BY time_window
    """)

    w = Window.orderBy("time_window.start").rowsBetween(-2, 0)
    q3 = attacks_per_window.withColumn(
        "rolling_avg_attacks", avg("attack_count").over(w)
    )
    q3.write.mode("overwrite").parquet(str(RESULTS_DIR / "attacks_over_time.parquet"))

    # 4. Port + protocol combinations most commonly associated with Reconnaissance attacks
    log("Query 4: port/protocol combinations for Reconnaissance attacks...")

    q4 = spark.sql("""
        SELECT dsport, proto, COUNT(*) AS cnt
        FROM traffic
        WHERE attack_cat = 'Reconnaissance'
        GROUP BY dsport, proto
        ORDER BY cnt DESC
        LIMIT 20
    """)
    q4.write.mode("overwrite").parquet(str(RESULTS_DIR / "reconnaissance_port_proto.parquet"))

    # 5. Ratio of normal to attack traffic by hour of day
    log("Query 5: normal vs. attack traffic ratio by hour...")

    q5 = spark.sql("""
        SELECT HOUR(Stime) AS hour_of_day,
               SUM(CASE WHEN Label = 0 THEN 1 ELSE 0 END) AS normal_count,
               SUM(CASE WHEN Label = 1 THEN 1 ELSE 0 END) AS attack_count
        FROM traffic
        GROUP BY HOUR(Stime)
        ORDER BY hour_of_day
    """)
    q5.write.mode("overwrite").parquet(str(RESULTS_DIR / "normal_vs_attack_by_hour.parquet"))

    # 6. Attack category distribution (for pie chart)
    log("Query 6: attack category distribution...")

    q6 = spark.sql("""
        SELECT attack_cat, COUNT(*) AS cnt
        FROM traffic
        GROUP BY attack_cat
        ORDER BY cnt DESC
    """)
    q6.write.mode("overwrite").parquet(str(RESULTS_DIR / "attack_cat_distribution.parquet"))

    # 7. Attack intensity by hour and day of week (for heatmap)
    log("Query 7: attack intensity by hour and day of week...")

    q7 = spark.sql("""
        SELECT DAYOFWEEK(Stime) AS day_of_week, HOUR(Stime) AS hour_of_day, COUNT(*) AS cnt
        FROM traffic
        WHERE Label = 1
        GROUP BY DAYOFWEEK(Stime), HOUR(Stime)
        ORDER BY day_of_week, hour_of_day
    """)
    q7.write.mode("overwrite").parquet(str(RESULTS_DIR / "attack_intensity_by_hour_day.parquet"))

    log(f"SQL analysis completed, results saved to {RESULTS_DIR}")


finally:
    spark.stop()