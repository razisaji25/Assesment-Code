# =====================================================
# SCD2 CONFIG
# =====================================================

TODAY = F.current_date()
MAX_DATE = F.lit("9999-12-31").cast("date")

# =====================================================
# TRY LOAD EXISTING DIMENSION
# =====================================================

try:

    dim_customer = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:duckdb:megamart.duckdb")
        .option("dbtable", "gold.dim_customers_scd2")
        .load()
    )

    first_load = False

except:

    first_load = True

# =====================================================
# INITIAL LOAD
# =====================================================

if first_load:

    dim_customer = (

        customer_snapshot

        .withColumn(
            "surrogate_key",
            F.monotonically_increasing_id() + 1
        )

        .withColumn(
            "effective_date",
            F.current_date()
        )

        .withColumn(
            "expiry_date",
            MAX_DATE
        )

        .withColumn(
            "is_current",
            F.lit(1)
        )

        .select(
            "surrogate_key",
            "customer_id",
            "full_name",
            "city",
            "loyalty_tier",
            "effective_date",
            "expiry_date",
            "is_current"
        )
    )

# =====================================================
# INCREMENTAL SCD2 MERGE
# =====================================================

else:

    current_dim = (
        dim_customer
        .filter(F.col("is_current") == 1)
    )

    compare_df = (

        customer_snapshot.alias("src")

        .join(
            current_dim.alias("tgt"),
            "customer_id",
            "left"
        )
    )

    # =========================================
    # NEW CUSTOMER
    # =========================================

    new_customer = (

        compare_df

        .filter(
            F.col("tgt.customer_id").isNull()
        )

        .select("src.*")
    )

    # =========================================
    # CHANGED CUSTOMER
    # =========================================

    changed_customer = (

        compare_df

        .filter(
            (F.col("src.city")
             != F.col("tgt.city"))

            |

            (F.col("src.loyalty_tier")
             != F.col("tgt.loyalty_tier"))
        )
    )

    # =========================================
    # EXPIRE OLD RECORD
    # =========================================

    expired_records = (

        dim_customer.alias("hist")

        .join(
            changed_customer.select(
                "customer_id"
            ),
            "customer_id"
        )

        .filter(
            F.col("hist.is_current") == 1
        )

        .withColumn(
            "expiry_date",
            F.date_sub(
                F.col("registration_date"),
                1
            )
        )

        .withColumn(
            "is_current",
            F.lit(0)
        )
    )

    # =========================================
    # NEW VERSION
    # =========================================

    changed_new_version = (

        changed_customer

        .select("src.*")

        .withColumn(
            "surrogate_key",
            F.monotonically_increasing_id()
            + 1000000
        )

        .withColumn(
            "effective_date",
            F.col("registration_date")
        )

        .withColumn(
            "expiry_date",
            MAX_DATE
        )

        .withColumn(
            "is_current",
            F.lit(1)
        )

        .select(
            "surrogate_key",
            "customer_id",
            "full_name",
            "city",
            "loyalty_tier",
            "effective_date",
            "expiry_date",
            "is_current"
        )
    )

    # =========================================
    # INSERT NEW CUSTOMER
    # =========================================

    new_customer_insert = (

        new_customer

        .withColumn(
            "surrogate_key",
            F.monotonically_increasing_id()
            + 2000000
        )

        .withColumn(
            "effective_date",
            F.col("registration_date")
        )

        .withColumn(
            "expiry_date",
            MAX_DATE
        )

        .withColumn(
            "is_current",
            F.lit(1)
        )

        .select(
            "surrogate_key",
            "customer_id",
            "full_name",
            "city",
            "loyalty_tier",
            "effective_date",
            "expiry_date",
            "is_current"
        )
    )

    unchanged = (

        dim_customer.alias("d")

        .join(
            changed_customer.select(
                "customer_id"
            ),
            "customer_id",
            "left_anti"
        )
    )

    dim_customer = (

        unchanged

        .unionByName(expired_records)

        .unionByName(changed_new_version)

        .unionByName(new_customer_insert)
    )

# =====================================================
# SAVE TO DUCKDB
# =====================================================

customer_pd = dim_customer.toPandas()

conn = duckdb.connect("megamart.duckdb")

conn.execute("""
CREATE SCHEMA IF NOT EXISTS gold
""")

conn.register(
    "customer_scd2_df",
    customer_pd
)

conn.execute("""
CREATE OR REPLACE TABLE gold.dim_customers_scd2 AS
SELECT *
FROM customer_scd2_df
""")

print("SCD Type 2 table saved")

# =====================================================
# VALIDATION
# =====================================================

conn.sql("""
SELECT
    customer_id,
    city,
    loyalty_tier,
    effective_date,
    expiry_date,
    is_current
FROM gold.dim_customers_scd2
LIMIT 10
""").show()

conn.close()
