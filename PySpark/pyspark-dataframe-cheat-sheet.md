# PySpark DataFrame Cheat Sheet

Oct 29, 2026

## 1. Setup

Every snippet below assumes these imports and a `spark` session; run them once at the top of a notebook or script.

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, DateType)

spark = SparkSession.builder.appName("cheatsheet").getOrCreate()
```

Convention: import functions as `F` (`F.col`, `F.when`, `F.sum`) so they never shadow Python built-ins like `sum`, `min`, `max`.

## 2. Creating DataFrames

**From a CSV file.** Use `header=True` for a header row. `inferSchema=True` costs an extra pass over the data, so prefer an explicit schema for large files.

```python
# Quick exploration
df = spark.read.csv("sales.csv", header=True, inferSchema=True)

# Production: explicit schema, no inference pass
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("region",   StringType(),  True),
    StructField("product",  StringType(),  True),
    StructField("qty",      IntegerType(), True),
    StructField("price",    DoubleType(),  True),
    StructField("order_dt", DateType(),    True),
])
df = (spark.read
      .option("header", True)
      .option("dateFormat", "yyyy-MM-dd")
      .schema(schema)
      .csv("sales.csv"))

df.printSchema()
df.show(5, truncate=False)
```

**From a list of Python dicts.** Column names come from the keys. Spark infers types, but missing keys become nulls, so keep the keys consistent.

```python
rows = [
    {"order_id": 1, "region": "East", "product": "A", "qty": 3, "price": 9.99},
    {"order_id": 2, "region": "West", "product": "B", "qty": 1, "price": 24.50},
]
df = spark.createDataFrame(rows)
```

**From a list of tuples (control the column order and names).**

```python
df = spark.createDataFrame(
    [(1, "East", 3), (2, "West", 1)],
    schema=["order_id", "region", "qty"],   # or pass a StructType / DDL string
)
# DDL string schema also works:
df = spark.createDataFrame(rows, "order_id INT, region STRING, product STRING, qty INT, price DOUBLE")
```

**From pandas.** `spark.createDataFrame(pdf)`. Back to pandas: `df.toPandas()` (collects everything to the driver, so only for small results).

## 3. Core transformations

DataFrames are immutable: every call returns a new DataFrame, so chain calls or reassign (`df = df.filter(...)`).

```python
# Select / rename / drop
df.select("order_id", "region", F.col("qty") * 2)
df.select(F.col("price").alias("unit_price"))
df.withColumnRenamed("qty", "quantity")
df.drop("product")

# Filter (use & | ~ with parentheses, not and/or/not)
df.filter(F.col("qty") > 1)
df.filter((F.col("region") == "East") & (F.col("price") < 20))
df.filter(F.col("region").isin("East", "West"))
df.filter(F.col("product").like("A%"))
df.filter("qty > 1 AND region = 'East'")     # SQL string also works

# Sort, distinct, limit
df.orderBy(F.col("price").desc(), "region")
df.select("region").distinct()
df.dropDuplicates(["order_id"])

# Types and nulls
df.withColumn("qty", F.col("qty").cast("int"))
df.na.fill({"qty": 0, "region": "unknown"})
df.na.drop(subset=["price"])
df.filter(F.col("price").isNull())

# Join
orders.join(customers, on="customer_id", how="left")                  # same key name
orders.join(customers, orders.cust == customers.id, "inner")          # different key names
```

## 4. New columns from other columns

`withColumn(name, expression)` adds a column, or replaces one with the same name. The expression can reference any existing columns. Use `F.when(...).otherwise(...)` for conditional logic.

```python
# Arithmetic on one or more columns
df = df.withColumn("revenue", F.col("qty") * F.col("price"))

# Constant
df = df.withColumn("source", F.lit("web"))

# Conditional on one column (when/otherwise chain; first match wins)
df = df.withColumn(
    "size",
    F.when(F.col("qty") >= 10, "bulk")
     .when(F.col("qty") >= 3,  "medium")
     .otherwise("single"),
)

# Conditional on several columns
df = df.withColumn(
    "flag",
    F.when((F.col("region") == "East") & (F.col("revenue") > 100), "east_big")
     .when(F.col("price").isNull() | F.col("qty").isNull(), "incomplete")
     .otherwise("normal"),
)

# SQL expression string (handy for CASE WHEN)
df = df.withColumn("tier", F.expr("CASE WHEN revenue > 500 THEN 'A' WHEN revenue > 100 THEN 'B' ELSE 'C' END"))

# Several columns at once (Spark 3.3+)
df = df.withColumns({"revenue": F.col("qty") * F.col("price"),
                     "source": F.lit("web")})

# First non-null of several columns
df = df.withColumn("contact", F.coalesce("mobile", "phone", F.lit("n/a")))
```

**Handy built-in functions for deriving columns:**

```python
# Strings
F.upper("name"), F.lower("name"), F.trim("name"), F.length("name")
F.concat_ws(" ", "first", "last")
F.substring("code", 1, 3)
F.regexp_extract("email", r"@(.+)$", 1)
F.split("tags", ",")                  # string -> array

# Dates (order_dt must be date or timestamp)
F.to_date("dt_str", "yyyy-MM-dd")
F.year("order_dt"), F.month("order_dt"), F.dayofweek("order_dt")
F.datediff("end_dt", "start_dt")
F.date_add("order_dt", 30)
F.date_format("order_dt", "yyyy-MM")

# Numbers
F.round("price", 2), F.abs("delta"), F.greatest("a", "b"), F.least("a", "b")

# Arrays / explode
F.explode("tags")                     # one row per array element
```

Prefer built-in functions over Python UDFs: built-ins run inside Spark's optimizer and are much faster. If you truly need custom Python, use a pandas UDF before a plain `@F.udf`.

## 5. Aggregations

`groupBy` collapses rows to one row per group. Always name your aggregates with `.alias()`, or you get names like `sum(revenue)`.

```python
# Whole-DataFrame summary
df.agg(F.count("*").alias("rows"),
       F.sum("revenue").alias("total_revenue"),
       F.avg("price").alias("avg_price"))

# Group + multiple aggregates
summary = (df.groupBy("region", "product")
             .agg(F.count("*").alias("orders"),
                  F.sum("revenue").alias("revenue"),
                  F.avg("qty").alias("avg_qty"),
                  F.min("order_dt").alias("first_order"),
                  F.max("order_dt").alias("last_order"),
                  F.countDistinct("order_id").alias("distinct_orders"),
                  F.collect_list("order_id").alias("order_ids"))
             .orderBy(F.col("revenue").desc()))

# Conditional aggregate (count/sum only rows matching a condition)
df.groupBy("region").agg(
    F.sum(F.when(F.col("qty") >= 10, 1).otherwise(0)).alias("bulk_orders"),
    F.sum(F.when(F.col("product") == "A", F.col("revenue"))).alias("revenue_A"),
)

# Filter on an aggregate (SQL HAVING)
summary.filter(F.col("revenue") > 1000)

# Pivot (list the values for speed and stable column order)
df.groupBy("region").pivot("product", ["A", "B", "C"]).agg(F.sum("revenue"))

# Quick stats
df.describe("qty", "price").show()
df.approxQuantile("price", [0.25, 0.5, 0.75], 0.01)
```

## 6. Window functions

A window function computes a value across related rows but keeps every row (unlike `groupBy`). Pattern: define a window with `Window.partitionBy(...).orderBy(...)`, then call `function().over(window)` inside `withColumn`.

```python
w = Window.partitionBy("region").orderBy(F.col("revenue").desc())

# Ranking
df = (df.withColumn("row_num",   F.row_number().over(w))   # 1,2,3,4 (no ties)
        .withColumn("rank",      F.rank().over(w))         # 1,2,2,4 (gaps after ties)
        .withColumn("dense_rank", F.dense_rank().over(w))) # 1,2,2,3

# Top N per group
top2 = df.withColumn("rn", F.row_number().over(w)).filter("rn <= 2").drop("rn")

# Aggregate over a whole partition (no orderBy = whole group)
w_all = Window.partitionBy("region")
df = (df.withColumn("region_total", F.sum("revenue").over(w_all))
        .withColumn("pct_of_region", F.col("revenue") / F.col("region_total")))

# Running total (orderBy + explicit frame)
w_run = (Window.partitionBy("region").orderBy("order_dt")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df = df.withColumn("running_revenue", F.sum("revenue").over(w_run))

# Moving average: current row + 6 previous rows
w_7 = Window.partitionBy("region").orderBy("order_dt").rowsBetween(-6, 0)
df = df.withColumn("ma_7", F.avg("revenue").over(w_7))

# Previous / next row
w_t = Window.partitionBy("region").orderBy("order_dt")
df = (df.withColumn("prev_revenue", F.lag("revenue", 1).over(w_t))
        .withColumn("next_revenue", F.lead("revenue", 1).over(w_t))
        .withColumn("change", F.col("revenue") - F.col("prev_revenue")))

# First / last value in the partition
w_f = (Window.partitionBy("region").orderBy("order_dt")
             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
df = df.withColumn("first_order_dt", F.first("order_dt").over(w_f))
```

When a window has `orderBy` but no explicit frame, aggregates default to a running frame (start of partition to current row, by value). Spell out `rowsBetween` whenever you want a specific frame.

## 7. Gotchas and quick tips

- Use `&`, `|`, `~` for combining conditions, and wrap each comparison in parentheses. Python's `and`/`or`/`not` raise an error on columns.
- Reassign results: `df.withColumn(...)` alone changes nothing.
- `null == null` is not true in Spark. Test with `isNull()`/`isNotNull()`, or use `eqNullSafe`.
- A window with `orderBy` and no `partitionBy` moves all rows into one partition. That is slow and memory-heavy on big data.
- Window functions need `.over(w)`; ranking functions need an `orderBy` in the window.
- `show()` is an action that runs the job; transformations (`filter`, `withColumn`, `groupBy`) are lazy. Use `df.explain()` to inspect the plan.
- `df.cache()` helps when you reuse a DataFrame several times; call `df.unpersist()` when done.
- Write results with `df.write.mode("overwrite").parquet("out/")`. Prefer Parquet over CSV for anything you'll read back.
- Anything here can be run as SQL: `df.createOrReplaceTempView("sales")` then `spark.sql("SELECT region, SUM(revenue) FROM sales GROUP BY region")`.

**Official references:** the PySpark "Quickstart: DataFrame" notebook, and the API pages for `pyspark.sql.functions` and `pyspark.sql.Window`, all on spark.apache.org.
