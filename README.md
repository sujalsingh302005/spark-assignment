# Week 5 — Spark Fundamentals: Data Cleaning, Transformation & Aggregation

## Objective
Understand Spark fundamentals and perform data cleaning, transformation, and aggregation
using DataFrames (PySpark).

## What This Covers
- Limitations of traditional MapReduce vs. Spark's in-memory model (Q1, Q2)
- Removing duplicate rows (Q3, Q15)
- Filtering with single and multiple conditions (Q4, Q8, Q12)
- Handling nulls: `.na.drop()` vs `.na.fill()` (Q5, Q9, Q15)
- Grouping and aggregation, including `HAVING`-style filters on aggregates (Q6, Q13, Q15)
- DataFrame immutability and its effect on cleaning pipelines (Q7)
- Schema changes: casting and renaming columns (Q10)
- Shuffle and wide transformations (Q11)
- Risks of `inferSchema=true` on messy data (Q14)
- A full end-to-end cleaning → aggregation pipeline (Q15)

## Folder Structure
```
spark-assignment/
│── data/
│   └── dataset.csv          # synthetic 1,050-row transactions dataset used for all queries
│── notebook/
│   └── spark_basics.ipynb   # PySpark notebook, answers Q1-Q15, fully executed with outputs
│── output/
│   └── results.csv          # final pipeline output (Q15): total revenue per store_id
│── README.md
```

## Dataset
`data/dataset.csv` is a synthetic dataset generated for this assignment with columns:
`user_id, transaction_date, region, product_category, sale_amount, status, city, age,
subscription, raw_timestamp, email, username, price, store_id`.

It intentionally includes:
- Duplicate `(user_id, transaction_date)` pairs
- Null values in `status` and `price`
- Empty strings in `username`, and some null/blank `email` values

...so that every cleaning operation asked about in the assignment (dedup, null handling,
empty-string filtering) has real data to operate on.

## How to Run
```bash
pip install pyspark
cd notebook
jupyter notebook spark_basics.ipynb   # run all cells
```
The notebook creates a local `SparkSession`, loads `../data/dataset.csv`, and writes the
final aggregated result to `../output/results.csv`.

## Key Insights
- Spark's in-memory execution avoids the repeated disk I/O that makes iterative MapReduce
  jobs slow — this matters most for ML-style workloads that scan the same data many times.
- DataFrames are immutable: every cleaning step (`dropDuplicates`, `na.fill`, `withColumn`,
  `filter`) returns a **new** DataFrame, so pipelines are built by chaining/reassigning
  rather than mutating in place.
- `groupBy()`/aggregation operations are **wide transformations** — they require a shuffle
  (redistributing rows by key across partitions), which is more expensive than narrow
  transformations like `filter()` or `select()`.
- Handling nulls **before** aggregating avoids silently skewed `avg()`/`sum()` results, since
  Spark's aggregate functions ignore nulls by default rather than raising an error.
- `inferSchema=true` is convenient but risky on inconsistent date formats — it can silently
  fall back to `string` type or null out unparseable values; an explicit schema with
  `to_date()`/`to_timestamp()` and a known format string is safer for messy data.
- The final pipeline (filter duplicates → fill null prices with 0 → group by `store_id` for
  total revenue) shows all three cleaning/aggregation phases combined into one flow, with the
  result written to `output/results.csv`.
