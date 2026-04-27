import os
import urllib.parse
import urllib.request

from urllib.error import HTTPError

import clickhouse_connect

from pyspark.sql import SparkSession, functions as F


PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "app")
PGUSER = os.getenv("PGUSER", "user")
PGPASSWORD = os.getenv("PGPASSWORD", "password")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DATABASE = os.getenv("CH_DATABASE", "app")
CH_USER = os.getenv("CH_USER", "user")
CH_PASSWORD = os.getenv("CH_PASSWORD", "password")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

POSTGRES_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
POSTGRES_PROPS = {
    "user": PGUSER,
    "password": PGPASSWORD,
    "driver": "org.postgresql.Driver",
}

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=int(CH_PORT),
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )



def ch_exec(sql: str) -> None:
    query = urllib.parse.urlencode(
        {
            "database": CH_DATABASE,
            "user": CH_USER,
            "password": CH_PASSWORD,
        }
    )
    url = f"http://{CH_HOST}:{CH_PORT}/?{query}"
    req = urllib.request.Request(
    url,
    data=sql.encode("utf-8"),
    method="POST",
    headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {body}") from e


def prepare_clickhouse() -> None:
    client = get_ch_client()

    statements = [
        f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}",

        # product marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_top10_products
        (
            as_of_date Date,
            product_id UInt32,
            product_name String,
            product_category String,
            total_units_sold UInt64,
            total_revenue Decimal(18, 2),
            sales_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, total_units_sold, product_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_revenue_by_category
        (
            as_of_date Date,
            product_category String,
            total_revenue Decimal(18, 2),
            total_units_sold UInt64,
            sales_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, product_category)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_product_rating_reviews
        (
            as_of_date Date,
            product_id UInt32,
            product_name String,
            product_category String,
            avg_rating Decimal(4, 2),
            reviews_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, product_id)
        """,

        # customer marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_top10_customers
        (
            as_of_date Date,
            customer_id UInt32,
            customer_name String,
            customer_country String,
            total_purchase_amount Decimal(18, 2),
            orders_count UInt64,
            total_items_bought UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, total_purchase_amount, customer_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_customer_country_distribution
        (
            as_of_date Date,
            customer_country String,
            customers_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, customer_country)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_customer_avg_check
        (
            as_of_date Date,
            customer_id UInt32,
            customer_name String,
            customer_country String,
            avg_check Decimal(18, 2),
            orders_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, customer_id)
        """,

        # time marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_sales_monthly_trend
        (
            as_of_date Date,
            year UInt16,
            month UInt8,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, year, month)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_sales_yearly_trend
        (
            as_of_date Date,
            year UInt16,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, year)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_revenue_by_quarter
        (
            as_of_date Date,
            year UInt16,
            quarter UInt8,
            total_revenue Decimal(18, 2),
            orders_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, year, quarter)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_avg_order_size_by_month
        (
            as_of_date Date,
            year UInt16,
            month UInt8,
            avg_order_value Decimal(18, 2),
            avg_items_per_order Decimal(18, 2)
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, year, month)
        """,

        # store marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_top5_stores
        (
            as_of_date Date,
            store_id UInt32,
            store_name String,
            store_city String,
            store_country String,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, total_revenue, store_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_store_sales_distribution
        (
            as_of_date Date,
            store_country String,
            store_city String,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, store_country, store_city)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_store_avg_check
        (
            as_of_date Date,
            store_id UInt32,
            store_name String,
            store_city String,
            store_country String,
            avg_check Decimal(18, 2),
            orders_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, store_id)
        """,

        # supplier marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_top5_suppliers
        (
            as_of_date Date,
            supplier_id UInt32,
            supplier_name String,
            supplier_country String,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, total_revenue, supplier_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_supplier_avg_price
        (
            as_of_date Date,
            supplier_id UInt32,
            supplier_name String,
            supplier_country String,
            avg_product_price Decimal(18, 2),
            products_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, supplier_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_supplier_country_sales
        (
            as_of_date Date,
            supplier_country String,
            total_revenue Decimal(18, 2),
            orders_count UInt64,
            total_items_sold UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, supplier_country)
        """,

        # quality marts
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_product_rating_extremes
        (
            as_of_date Date,
            extreme_type String,
            product_id UInt32,
            product_name String,
            product_category String,
            rating Decimal(4, 2),
            reviews_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, extreme_type, product_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_rating_sales_correlation
        (
            as_of_date Date,
            rating_sales_correlation Float64
        )
        ENGINE = MergeTree()
        ORDER BY as_of_date
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.dm_top_reviewed_products
        (
            as_of_date Date,
            product_id UInt32,
            product_name String,
            product_category String,
            rating Decimal(4, 2),
            reviews_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (as_of_date, reviews_count, product_id)
        """,
    ]

    truncate_statements = [
        f"TRUNCATE TABLE {CH_DATABASE}.dm_top10_products",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_revenue_by_category",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_product_rating_reviews",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_top10_customers",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_customer_country_distribution",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_customer_avg_check",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_sales_monthly_trend",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_sales_yearly_trend",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_revenue_by_quarter",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_avg_order_size_by_month",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_top5_stores",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_store_sales_distribution",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_store_avg_check",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_top5_suppliers",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_supplier_avg_price",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_supplier_country_sales",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_product_rating_extremes",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_rating_sales_correlation",
        f"TRUNCATE TABLE {CH_DATABASE}.dm_top_reviewed_products",
    ]

    for stmt in statements:
        client.command(stmt.strip())

    for stmt in truncate_statements:
        client.command(stmt.strip())

def insert_df_to_clickhouse(df, table_name, columns, batch_size = 5000):
    client = get_ch_client()
    full_table = f"{CH_DATABASE}.{table_name}"

    batch: list[tuple] = []
    inserted = 0

    # Сразу отрежем лишние колонки
    selected_df = df.select(*columns)

    for row in selected_df.toLocalIterator(prefetchPartitions=False):
        batch.append(tuple(row[c] for c in columns))

        if len(batch) >= batch_size:
            client.insert(full_table, batch, column_names=columns)
            inserted += len(batch)
            batch.clear()

    if batch:
        client.insert(full_table, batch, column_names=columns)
        inserted += len(batch)

    return inserted

def write_to_clickhouse(df, table_name: str) -> None:
    (
        df.write
        .mode("append")
        .jdbc(
            url=CLICKHOUSE_URL,
            table=f"{CH_DATABASE}.{table_name}",
            properties=CLICKHOUSE_PROPS,
        )
    )

def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("postgres-to-clickhouse-marts")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_pg(spark: SparkSession, table_or_query: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_or_query)
        .option("user", PGUSER)
        .option("password", PGPASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
        .load()
    )


def main() -> None:
    spark = get_spark()

    prepare_clickhouse()

    sales_fact_df = read_pg(
        spark,
        """
        (
            SELECT
                id AS sale_id,
                customer_id,
                store_id,
                product_id,
                date AS sale_date,
                sale_quantity,
                total_price
            FROM sales_fact
        ) sf
        """,
    )

    customer_df = read_pg(
        spark,
        """
        (
            SELECT
                id AS customer_id,
                first_name,
                last_name,
                country AS customer_country
            FROM customer
        ) c
        """,
    )

    store_df = read_pg(
        spark,
        """
        (
            SELECT
                id AS store_id,
                name AS store_name,
                city AS store_city,
                country AS store_country
            FROM store
        ) st
        """,
    )

    product_df = read_pg(
        spark,
        """
        (
            SELECT
                id AS product_id,
                name AS product_name,
                category AS product_category,
                price AS product_price,
                rating AS product_rating,
                reviews AS product_reviews,
                supplier_id
            FROM product
        ) p
        """,
    )

    supplier_df = read_pg(
        spark,
        """
        (
            SELECT
                id AS supplier_id,
                name AS supplier_name,
                country AS supplier_country
            FROM supplier
        ) sup
        """,
    )

    snapshot_date = F.current_date()

    # product marts
    product_sales_df = sales_fact_df.join(product_df, on="product_id", how="inner")

    top10_products_df = (
        product_sales_df
        .groupBy("product_id", "product_name", "product_category")
        .agg(
            F.sum("sale_quantity").cast("long").alias("total_units_sold"),
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("sales_count"),
        )
        .orderBy(F.desc("total_units_sold"), F.desc("total_revenue"), F.asc("product_id"))
        .limit(10)
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("product_id").cast("int").alias("product_id"),
            "product_name",
            "product_category",
            "total_units_sold",
            "total_revenue",
            "sales_count",
        )
    )

    revenue_by_category_df = (
        product_sales_df
        .groupBy("product_category")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.sum("sale_quantity").cast("long").alias("total_units_sold"),
            F.count("sale_id").cast("long").alias("sales_count"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            "product_category",
            "total_revenue",
            "total_units_sold",
            "sales_count",
        )
    )

    product_rating_reviews_df = (
        product_df
        .select(
            snapshot_date.alias("as_of_date"),
            F.col("product_id").cast("int").alias("product_id"),
            "product_name",
            "product_category",
            F.col("product_rating").cast("decimal(4,2)").alias("avg_rating"),
            F.col("product_reviews").cast("long").alias("reviews_count"),
        )
    )

    # customer marts
    customer_sales_df = (
        sales_fact_df
        .join(customer_df, on="customer_id", how="inner")
        .withColumn("customer_name", F.concat_ws(" ", "first_name", "last_name"))
    )

    customer_top10_df = (
        customer_sales_df
        .groupBy("customer_id", "customer_name", "customer_country")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_purchase_amount"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_bought"),
        )
        .orderBy(F.desc("total_purchase_amount"), F.desc("orders_count"), F.asc("customer_id"))
        .limit(10)
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("customer_id").cast("int").alias("customer_id"),
            "customer_name",
            "customer_country",
            "total_purchase_amount",
            "orders_count",
            "total_items_bought",
        )
    )

    customer_country_distribution_df = (
        customer_df
        .groupBy("customer_country")
        .agg(F.count("customer_id").cast("long").alias("customers_count"))
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            "customer_country",
            "customers_count",
        )
    )

    customer_avg_check_df = (
        customer_sales_df
        .groupBy("customer_id", "customer_name", "customer_country")
        .agg(
            F.avg("total_price").cast("decimal(18,2)").alias("avg_check"),
            F.count("sale_id").cast("long").alias("orders_count"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("customer_id").cast("int").alias("customer_id"),
            "customer_name",
            "customer_country",
            "avg_check",
            "orders_count",
        )
    )

    # time marts
    sales_monthly_trend_df = (
        sales_fact_df
        .groupBy(
            F.year("sale_date").alias("year"),
            F.month("sale_date").alias("month"),
        )
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .orderBy("year", "month")
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    sales_yearly_trend_df = (
        sales_fact_df
        .groupBy(F.year("sale_date").alias("year"))
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .orderBy("year")
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("year").cast("int").alias("year"),
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    revenue_by_quarter_df = (
        sales_fact_df
        .groupBy(
            F.year("sale_date").alias("year"),
            F.quarter("sale_date").alias("quarter"),
        )
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
        )
        .orderBy("year", "quarter")
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("year").cast("int").alias("year"),
            F.col("quarter").cast("int").alias("quarter"),
            "total_revenue",
            "orders_count",
        )
    )

    avg_order_size_by_month_df = (
        sales_fact_df
        .groupBy(
            F.year("sale_date").alias("year"),
            F.month("sale_date").alias("month"),
        )
        .agg(
            F.avg("total_price").cast("decimal(18,2)").alias("avg_order_value"),
            F.avg("sale_quantity").cast("decimal(18,2)").alias("avg_items_per_order"),
        )
        .orderBy("year", "month")
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
            "avg_order_value",
            "avg_items_per_order",
        )
    )

    # store marts
    store_sales_df = sales_fact_df.join(store_df, on="store_id", how="inner")

    store_top5_df = (
        store_sales_df
        .groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("orders_count"), F.asc("store_id"))
        .limit(5)
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("store_id").cast("int").alias("store_id"),
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    store_sales_distribution_df = (
        store_sales_df
        .groupBy("store_country", "store_city")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            "store_country",
            "store_city",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    store_avg_check_df = (
        store_sales_df
        .groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.avg("total_price").cast("decimal(18,2)").alias("avg_check"),
            F.count("sale_id").cast("long").alias("orders_count"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("store_id").cast("int").alias("store_id"),
            "store_name",
            "store_city",
            "store_country",
            "avg_check",
            "orders_count",
        )
    )

    # supplier marts
    supplier_sales_df = (
        sales_fact_df
        .join(product_df.select("product_id", "product_price", "supplier_id"), on="product_id", how="inner")
        .join(supplier_df, on="supplier_id", how="inner")
    )

    supplier_top5_df = (
        supplier_sales_df
        .groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("orders_count"), F.asc("supplier_id"))
        .limit(5)
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("supplier_id").cast("int").alias("supplier_id"),
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    supplier_avg_price_df = (
        product_df
        .join(supplier_df, on="supplier_id", how="inner")
        .groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.avg("product_price").cast("decimal(18,2)").alias("avg_product_price"),
            F.count("product_id").cast("long").alias("products_count"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("supplier_id").cast("int").alias("supplier_id"),
            "supplier_name",
            "supplier_country",
            "avg_product_price",
            "products_count",
        )
    )

    supplier_country_sales_df = (
        supplier_sales_df
        .groupBy("supplier_country")
        .agg(
            F.sum("total_price").cast("decimal(18,2)").alias("total_revenue"),
            F.count("sale_id").cast("long").alias("orders_count"),
            F.sum("sale_quantity").cast("long").alias("total_items_sold"),
        )
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            "supplier_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        )
    )

    # quality marts
    product_rating_base_df = (
        product_df
        .select(
            F.col("product_id").cast("int").alias("product_id"),
            "product_name",
            "product_category",
            F.col("product_rating").cast("decimal(4,2)").alias("rating"),
            F.col("product_reviews").cast("long").alias("reviews_count"),
        )
    )

    highest_rated_df = (
        product_rating_base_df
        .orderBy(F.desc("rating"), F.desc("reviews_count"), F.asc("product_id"))
        .limit(10)
        .withColumn("extreme_type", F.lit("highest"))
    )

    lowest_rated_df = (
        product_rating_base_df
        .orderBy(F.asc("rating"), F.desc("reviews_count"), F.asc("product_id"))
        .limit(10)
        .withColumn("extreme_type", F.lit("lowest"))
    )

    product_rating_extremes_df = (
        highest_rated_df
        .unionByName(lowest_rated_df)
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            "extreme_type",
            "product_id",
            "product_name",
            "product_category",
            "rating",
            "reviews_count",
        )
    )

    product_sales_volume_df = (
        sales_fact_df
        .groupBy("product_id")
        .agg(F.sum("sale_quantity").cast("double").alias("total_units_sold"))
    )

    rating_sales_correlation_df = (
        product_df
        .select(
            "product_id",
            F.col("product_rating").cast("double").alias("product_rating"),
        )
        .join(product_sales_volume_df, on="product_id", how="inner")
        .agg(F.corr("product_rating", "total_units_sold").alias("rating_sales_correlation"))
        .withColumn("as_of_date", snapshot_date)
        .select(
            "as_of_date",
            F.col("rating_sales_correlation").cast("double").alias("rating_sales_correlation"),
        )
    )

    top_reviewed_products_df = (
        product_df
        .select(
            snapshot_date.alias("as_of_date"),
            F.col("product_id").cast("int").alias("product_id"),
            "product_name",
            "product_category",
            F.col("product_rating").cast("decimal(4,2)").alias("rating"),
            F.col("product_reviews").cast("long").alias("reviews_count"),
        )
        .orderBy(F.desc("reviews_count"), F.desc("rating"), F.asc("product_id"))
        .limit(10)
    )

    prepare_clickhouse()

    # product marts
    insert_df_to_clickhouse(
        top10_products_df,
        "dm_top10_products",
        [
            "as_of_date",
            "product_id",
            "product_name",
            "product_category",
            "total_units_sold",
            "total_revenue",
            "sales_count",
        ],
    )

    insert_df_to_clickhouse(
        revenue_by_category_df,
        "dm_revenue_by_category",
        [
            "as_of_date",
            "product_category",
            "total_revenue",
            "total_units_sold",
            "sales_count",
        ],
    )

    insert_df_to_clickhouse(
        product_rating_reviews_df,
        "dm_product_rating_reviews",
        [
            "as_of_date",
            "product_id",
            "product_name",
            "product_category",
            "avg_rating",
            "reviews_count",
        ],
    )

    # customer marts
    insert_df_to_clickhouse(
        customer_top10_df,
        "dm_top10_customers",
        [
            "as_of_date",
            "customer_id",
            "customer_name",
            "customer_country",
            "total_purchase_amount",
            "orders_count",
            "total_items_bought",
        ],
    )

    insert_df_to_clickhouse(
        customer_country_distribution_df,
        "dm_customer_country_distribution",
        [
            "as_of_date",
            "customer_country",
            "customers_count",
        ],
    )

    insert_df_to_clickhouse(
        customer_avg_check_df,
        "dm_customer_avg_check",
        [
            "as_of_date",
            "customer_id",
            "customer_name",
            "customer_country",
            "avg_check",
            "orders_count",
        ],
    )

    # time marts
    insert_df_to_clickhouse(
        sales_monthly_trend_df,
        "dm_sales_monthly_trend",
        [
            "as_of_date",
            "year",
            "month",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    insert_df_to_clickhouse(
        sales_yearly_trend_df,
        "dm_sales_yearly_trend",
        [
            "as_of_date",
            "year",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    insert_df_to_clickhouse(
        revenue_by_quarter_df,
        "dm_revenue_by_quarter",
        [
            "as_of_date",
            "year",
            "quarter",
            "total_revenue",
            "orders_count",
        ],
    )

    insert_df_to_clickhouse(
        avg_order_size_by_month_df,
        "dm_avg_order_size_by_month",
        [
            "as_of_date",
            "year",
            "month",
            "avg_order_value",
            "avg_items_per_order",
        ],
    )

    # store marts
    insert_df_to_clickhouse(
        store_top5_df,
        "dm_top5_stores",
        [
            "as_of_date",
            "store_id",
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    insert_df_to_clickhouse(
        store_sales_distribution_df,
        "dm_store_sales_distribution",
        [
            "as_of_date",
            "store_country",
            "store_city",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    insert_df_to_clickhouse(
        store_avg_check_df,
        "dm_store_avg_check",
        [
            "as_of_date",
            "store_id",
            "store_name",
            "store_city",
            "store_country",
            "avg_check",
            "orders_count",
        ],
    )

    # supplier marts
    insert_df_to_clickhouse(
        supplier_top5_df,
        "dm_top5_suppliers",
        [
            "as_of_date",
            "supplier_id",
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    insert_df_to_clickhouse(
        supplier_avg_price_df,
        "dm_supplier_avg_price",
        [
            "as_of_date",
            "supplier_id",
            "supplier_name",
            "supplier_country",
            "avg_product_price",
            "products_count",
        ],
    )

    insert_df_to_clickhouse(
        supplier_country_sales_df,
        "dm_supplier_country_sales",
        [
            "as_of_date",
            "supplier_country",
            "total_revenue",
            "orders_count",
            "total_items_sold",
        ],
    )

    # quality marts
    insert_df_to_clickhouse(
        product_rating_extremes_df,
        "dm_product_rating_extremes",
        [
            "as_of_date",
            "extreme_type",
            "product_id",
            "product_name",
            "product_category",
            "rating",
            "reviews_count",
        ],
    )

    insert_df_to_clickhouse(
        rating_sales_correlation_df,
        "dm_rating_sales_correlation",
        [
            "as_of_date",
            "rating_sales_correlation",
        ],
    )

    insert_df_to_clickhouse(
        top_reviewed_products_df,
        "dm_top_reviewed_products",
        [
            "as_of_date",
            "product_id",
            "product_name",
            "product_category",
            "rating",
            "reviews_count",
        ],
    )

    print("Loaded marts:")
    for name, df in [
        ("dm_top10_products", top10_products_df),
        ("dm_revenue_by_category", revenue_by_category_df),
        ("dm_product_rating_reviews", product_rating_reviews_df),
        ("dm_top10_customers", customer_top10_df),
        ("dm_customer_country_distribution", customer_country_distribution_df),
        ("dm_customer_avg_check", customer_avg_check_df),
        ("dm_sales_monthly_trend", sales_monthly_trend_df),
        ("dm_sales_yearly_trend", sales_yearly_trend_df),
        ("dm_revenue_by_quarter", revenue_by_quarter_df),
        ("dm_avg_order_size_by_month", avg_order_size_by_month_df),
        ("dm_top5_stores", store_top5_df),
        ("dm_store_sales_distribution", store_sales_distribution_df),
        ("dm_store_avg_check", store_avg_check_df),
        ("dm_top5_suppliers", supplier_top5_df),
        ("dm_supplier_avg_price", supplier_avg_price_df),
        ("dm_supplier_country_sales", supplier_country_sales_df),
        ("dm_product_rating_extremes", product_rating_extremes_df),
        ("dm_rating_sales_correlation", rating_sales_correlation_df),
        ("dm_top_reviewed_products", top_reviewed_products_df),
    ]:
        print(f"{name}: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()