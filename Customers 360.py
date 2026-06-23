# ==========================================
# PERFORMANCE OPTIMIZATION
# ==========================================

products = F.broadcast(products)

# ==========================================
# BASE TRANSACTION
# Formula:
# total_spend = SUM(total_amount - discount_amount)
# ==========================================

txn_base = (
    transactions
    .filter(
        F.col("customer_id").isNotNull()
    )
    .withColumn(
        "net_amount",
        F.col("total_amount")
        - F.col("discount_amount")
    )
)

txn_base.cache()

# ==========================================
# CUSTOMER METRICS
# ==========================================

customer_metrics = (
    txn_base
    .groupBy("customer_id")
    .agg(
        F.countDistinct("txn_id").alias("total_transactions"),
        F.sum("net_amount").alias("total_spend"),
        F.min("txn_date").alias("first_purchase_date"),
        F.max("txn_date").alias("last_purchase_date")
    )
)

customer_metrics = (
    customer_metrics
    .withColumn(
        "avg_basket_size",
        F.when(
            F.col("total_transactions") > 0,
            F.col("total_spend") / F.col("total_transactions")
        ).otherwise(0)
    )
    .withColumn(
        "days_since_last_purchase",
        F.datediff(
            F.current_date(),
            F.col("last_purchase_date")
        )
    )
    .withColumn(
        "months_active",
        F.greatest(
            F.months_between(
                F.current_date(),
                F.col("first_purchase_date")
            ),
            F.lit(1)
        )
    )
    .withColumn(
        "avg_monthly_spend",
        F.col("total_spend") /
        F.col("months_active")
    )
)

# ==========================================
# PREFERRED CHANNEL
# channel with MAX transaction count
# ==========================================

channel_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.desc("txn_count"))
)

preferred_channel = (
    txn_base
    .groupBy(
        "customer_id",
        "channel"
    )
    .agg(
        F.countDistinct("txn_id")
        .alias("txn_count")
    )
    .withColumn(
        "rn",
        F.row_number().over(channel_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "customer_id",
        F.col("channel").alias("preferred_channel")
    )
)

# ==========================================
# PREFERRED CATEGORY
# category_l1 with MAX spend
# ==========================================

item_spend = (
    transaction_items
    .withColumn(
        "item_spend",
        (F.col("quantity") * F.col("unit_price"))
        - F.col("discount_amount")
    )
)

category_spend = (
    item_spend
    .join(
        txn_base.select(
            "txn_id",
            "customer_id"
        ),
        "txn_id"
    )
    .join(
        products.select(
            "sku_id",
            "category_l1"
        ),
        "sku_id"
    )
    .groupBy(
        "customer_id",
        "category_l1"
    )
    .agg(
        F.sum("item_spend")
        .alias("category_spend")
    )
)

category_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.desc("category_spend"))
)

preferred_category = (
    category_spend
    .withColumn(
        "rn",
        F.row_number().over(category_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "customer_id",
        F.col("category_l1")
        .alias("preferred_category")
    )
)

# ==========================================
# SPEND TREND
#
# INCREASING:
# last_3m_avg > prev_3m_avg * 1.1
#
# DECREASING:
# last_3m_avg < prev_3m_avg * 0.9
#
# STABLE:
# otherwise
# ==========================================

max_txn_date = (
    txn_base
    .agg(F.max("txn_date"))
    .collect()[0][0]
)

last_3m = (
    txn_base
    .filter(
        F.col("txn_date") >=
        F.add_months(F.lit(max_txn_date), -3)
    )
    .groupBy("customer_id")
    .agg(
        (F.sum("net_amount") / 3)
        .alias("last_3m_avg")
    )
)

prev_3m = (
    txn_base
    .filter(
        (F.col("txn_date") >=
         F.add_months(F.lit(max_txn_date), -6))
        &
        (F.col("txn_date") <
         F.add_months(F.lit(max_txn_date), -3))
    )
    .groupBy("customer_id")
    .agg(
        (F.sum("net_amount") / 3)
        .alias("prev_3m_avg")
    )
)

trend = (
    last_3m
    .join(
        prev_3m,
        "customer_id",
        "left"
    )
    .withColumn(
        "spend_trend",
        F.when(
            F.col("last_3m_avg") >
            F.col("prev_3m_avg") * 1.1,
            "INCREASING"
        )
        .when(
            F.col("last_3m_avg") <
            F.col("prev_3m_avg") * 0.9,
            "DECREASING"
        )
        .otherwise("STABLE")
    )
    .select(
        "customer_id",
        "spend_trend"
    )
)

# ==========================================
# CUSTOMER 360 GOLD
# ==========================================

customer_360 = (
    customers
    .join(
        customer_metrics,
        "customer_id",
        "left"
    )
    .join(
        preferred_channel,
        "customer_id",
        "left"
    )
    .join(
        preferred_category,
        "customer_id",
        "left"
    )
    .join(
        trend,
        "customer_id",
        "left"
    )
)

customer_360 = (
    customer_360
    .withColumn(
        "days_since_registration",
        F.datediff(
            F.current_date(),
            F.col("registration_date")
        )
    )
)

customer_360 = customer_360.fillna({
    "total_transactions": 0,
    "total_spend": 0,
    "avg_basket_size": 0,
    "avg_monthly_spend": 0
})

# ==========================================
# DATA QUALITY CHECKS
# ==========================================

# DQ1
assert (
    customer_360
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
    .count()
) == 0, "Duplicate customer found"

# DQ2
assert (
    customer_360
    .filter(F.col("total_spend") < 0)
    .count()
) == 0, "Negative spend found"

# DQ3
anonymous_txn_count = (
    transactions
    .filter(F.col("customer_id").isNull())
    .count()
)

print(
    f"Anonymous Transactions: {anonymous_txn_count}"
)

# DQ4
orphan_txn_count = (
    transactions
    .filter(F.col("customer_id").isNotNull())
    .join(
        customers.select("customer_id"),
        "customer_id",
        "left_anti"
    )
    .count()
)

assert orphan_txn_count == 0, \
    f"Found {orphan_txn_count} orphan transactions"

# ==========================================
# FINAL OUTPUT
# ==========================================

gold_dim_customer_360 = (
    customer_360
    .withColumn(
        "batch_date",
        F.current_date()
    )
    .withColumn(
        "created_at",
        F.current_timestamp()
    )
)

gold_dim_customer_360.show(20, False)

# OPTIONAL
import duckdb

# convert Spark DF -> Pandas
customer_360_pd = gold_dim_customer_360.toPandas()

# create/connect DuckDB
conn = duckdb.connect("./megamart.duckdb")

# overwrite table
conn.execute("""
DROP TABLE IF EXISTS dim_customer_360
""")

conn.register(
    "customer_360_df",
    customer_360_pd
)

conn.execute("""
CREATE TABLE dim_customer_360 AS
SELECT *
FROM customer_360_df
""")

conn.close()
