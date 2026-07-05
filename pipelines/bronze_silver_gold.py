import dlt
from pyspark.sql.functions import col, to_timestamp, from_unixtime, avg, window, min as spark_min, max as spark_max, first, last, count, sum as spark_sum

# Bronze: raw data streamed in from ADLS (landed there by your ingestion notebook)
@dlt.table(
    name="bronze_crypto_trades",
    comment="Raw crypto trades landed from Event Hub"
)
def bronze_crypto_trades():
    return spark.readStream.format("delta").load(
        "abfss://bronze@cryptolakesalman.dfs.core.windows.net/crypto_trades"
    )

# Silver: cleaned, typed, deduplicated
@dlt.table(
    name="silver_crypto_trades",
    comment="Cleaned and typed crypto trade data"
)
def silver_crypto_trades():
    return (
        dlt.read_stream("bronze_crypto_trades")
        .select(
            col("symbol"),
            col("price").cast("double").alias("price"),
            col("quantity").cast("double").alias("quantity"),
            to_timestamp(from_unixtime(col("trade_time") / 1000)).alias("trade_timestamp"),
            col("ingestion_time")
        )
        .dropDuplicates(["symbol", "trade_timestamp", "price", "quantity"])
        .filter(col("price").isNotNull() & col("quantity").isNotNull())
    )



# SILVER TO GOLD 


@dlt.table(
    name="gold_crypto_ohlc_1min",
    comment="1-minute OHLC candles with volume and average price"
)
def gold_crypto_ohlc_1min():
    return (
        dlt.read("silver_crypto_trades")  # static/batch read for this aggregation
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), "1 minute")
        )
        .agg(
            first("price").alias("open_price"),
            spark_max("price").alias("high_price"),
            spark_min("price").alias("low_price"),
            last("price").alias("close_price"),
            spark_sum("quantity").alias("total_volume"),
            count("*").alias("trade_count"),
            avg("price").alias("avg_price")
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "open_price", "high_price", "low_price", "close_price",
            "total_volume", "trade_count", "avg_price"
        )
    )



