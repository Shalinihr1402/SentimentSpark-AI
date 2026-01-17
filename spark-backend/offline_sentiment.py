# -*- coding: utf-8 -*-

import sys, os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_extract,
    monotonically_increasing_id, floor
)

# -------------------------
# INPUTS FROM FLASK
# -------------------------
input_file = sys.argv[1]
dataset_name = sys.argv[2]

OUTPUT_DIR = "D:/spark-output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("SentimentSpark-AI") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print(" Spark started")
print(" Dataset loading...")

# -------------------------
# LOAD FILE
# -------------------------
if input_file.endswith(".txt"):
    df = spark.read \
        .option("encoding", "UTF-8") \
        .text(input_file)

    df = df.withColumn("label", regexp_extract(col("value"), r"(__label__\d)", 1)) \
           .withColumn("text", regexp_extract(col("value"), r"__label__\d\s+(.*)", 1))


elif input_file.endswith(".csv"):
    df = spark.read.option("header", True).csv(input_file)

    df = df.withColumnRenamed(df.columns[0], "label") \
           .withColumnRenamed(df.columns[1], "text")

else:
    raise Exception("Only CSV and TXT supported")

# -------------------------
# CLEANING
# -------------------------
df = df.select("label", "text").dropna()

# -------------------------
# NORMALIZATION
# -------------------------
df = df.withColumn("label", lower(trim(col("label"))))

df = df.withColumn(
    "sentiment",
    when(col("label").isin("__label__2", "positive", "pos", "1"), "positive")
    .when(col("label").isin("__label__1", "negative", "neg", "-1"), "negative")
    .otherwise("neutral")
)

# -------------------------
# LIMIT + CACHE (VERY IMPORTANT)
# -------------------------
df = df.limit(50000)
df = df.cache()
print(" Dataset cleaned and cached")

# -------------------------
# TOTAL + COUNTS (ONE PASS)
# -------------------------
total_records = df.count()

sentiment_counts = df.groupBy("sentiment").count().collect()
counts = {"positive":0, "negative":0, "neutral":0}

for row in sentiment_counts:
    counts[row["sentiment"]] = row["count"]

positive = counts["positive"]
negative = counts["negative"]
neutral  = counts["neutral"]

accuracy = round((positive + negative) / total_records, 3) if total_records > 0 else 0

# -------------------------
# SAVE STATS
# -------------------------
stats = {
    "dataset_name": dataset_name,
    "total_records": total_records,
    "positive": positive,
    "negative": negative,
    "neutral": neutral,
    "accuracy": accuracy
}

with open(os.path.join(OUTPUT_DIR, "stats.json"), "w") as f:
    json.dump(stats, f, indent=4)

# -------------------------
# PREVIEW (NO PANDAS)
# -------------------------
preview = [
    {"text": r["text"], "sentiment": r["sentiment"]}
    for r in df.select("text","sentiment").limit(100).collect()
]

with open(os.path.join(OUTPUT_DIR, "preview.json"), "w", encoding="utf-8") as f:
    json.dump(preview, f, indent=4, ensure_ascii=False)

print("Preview saved")

# -------------------------
# TREND GENERATION (FAST)
# -------------------------
df = df.withColumn("row_id", monotonically_increasing_id())
df = df.withColumn("block", floor(col("row_id") / 500))

trend_df = df.groupBy("block", "sentiment").count()

pivot = trend_df.groupBy("block").pivot("sentiment").sum("count").fillna(0)

trend_data = []
for row in pivot.orderBy("block").limit(30).collect():
    trend_data.append({
        "block": int(row["block"]),
        "positive": int(row["positive"]) if "positive" in row else 0,
        "negative": int(row["negative"]) if "negative" in row else 0,
        "neutral": int(row["neutral"]) if "neutral" in row else 0
    })

with open(os.path.join(OUTPUT_DIR, "trend.json"), "w") as f:
    json.dump(trend_data, f, indent=4)

print(" Trend saved")
print(" All results generated successfully")

spark.stop()
