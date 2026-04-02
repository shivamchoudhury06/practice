# PySpark Master Reference Document — v2

> **Compiled from:** `Pyspark_Notes.one` (v1) + `Pyspark_Notes_02_04_2026.one` (v2 additions)  
> **Format:** Senior Data Engineer Review | Sections: YouTube Links · Interview Questions · Important Topics · Code Examples  
> **v2 Additions:** Module 10 (Spark Cluster Architecture) · Module 11 (Spark Join Strategies) · Module 12 (Broadcast Join) · Module 13 (Window Functions)

---

## Table of Contents

### — v1 Modules (Original) —

1. [Manual Schemas in PySpark](#1-manual-schemas-in-pyspark)
2. [Transformations and Actions](#2-transformations-and-actions)
3. [Spark Partitioning and Bucketing](#3-spark-partitioning-and-bucketing)
4. [SparkSession vs SparkContext](#4-sparksession-vs-sparkcontext)
5. [Spark Execution Hierarchy — Jobs, Stages, Tasks](#5-spark-execution-hierarchy--jobs-stages-tasks)
6. [DataFrames, Schemas, and RDDs](#6-dataframes-schemas-and-rdds)
7. [DataFrameReader API — Reading Data](#7-dataframereader-api--reading-data)
8. [Corrupted Records Handling](#8-corrupted-records-handling)
9. [Column Selection, Expressions, and Spark SQL](#9-column-selection-expressions-and-spark-sql)

### — v2 Modules (New — Added 02 Apr 2026) —

10. [Spark Cluster Architecture — Driver, Executors & UDFs](#10-spark-cluster-architecture--driver-executors--udfs)
11. [Spark Join Strategies — Sort Merge vs Shuffle Hash](#11-spark-join-strategies--sort-merge-vs-shuffle-hash)
12. [Broadcast Join in Spark](#12-broadcast-join-in-spark)
13. [Window Functions in PySpark](#13-window-functions-in-pyspark)

---

## 1. Manual Schemas in PySpark

### 🔗 YouTube Links

- [Schema in Spark | Lec-4](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/67656130-8c5d-4309-8911-cf1f5ec41b91)

---

### ❓ Interview Questions

- **How do you create a schema in PySpark?** (StructType vs DDL)
- **What is the difference between `StructType` and `StructField`?**
- **How do you skip specific rows during data ingestion?**
- **What happens if you have a header in your data but set `header` to `false`?**
- **What are the Read Modes available — `FAILFAST` vs `PERMISSIVE`?**

---

### 📌 Important Topics

#### 1.1 Why Manual Schemas Matter

While `inferSchema(True)` is convenient, it forces Spark to make an extra full pass over the data to guess types — this is slow on large datasets. A manual schema acts as a **"contract"** that enforces structure at ingestion, making pipelines deterministic and production-safe.

#### 1.2 Method 1 — StructType and StructField (Programmatic)

`StructType` defines the overall structure of the DataFrame (a container). `StructField` defines each individual column. Every `StructField` requires three pieces of information: **Column Name**, **Data Type**, and **Nullability**.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

my_schema = StructType([
    StructField("id",   IntegerType(), True),
    StructField("name", StringType(),  True),
    StructField("age",  IntegerType(), True)
])

df = spark.read.format("csv") \
    .schema(my_schema) \
    .load("path/to/file.csv")
```

#### 1.3 Method 2 — DDL String (Concise & Readable)

The DDL approach is a simpler string-based alternative, preferred for its brevity. Ideal when the schema is not complex.

```python
ddl_schema = "id INT, name STRING, age INT"

df = spark.read.format("csv") \
    .schema(ddl_schema) \
    .load("path/to/file.csv")
```

#### 1.4 Read Modes — FAILFAST vs PERMISSIVE

| Mode                   | Behaviour                                          |
| ---------------------- | -------------------------------------------------- |
| `PERMISSIVE` (default) | Malformed fields are set to `NULL`; job continues  |
| `FAILFAST`             | Throws an error immediately on any schema mismatch |
| `DROPMALFORMED`        | Silently drops any row that fails to parse         |

```python
# FAILFAST — stops on first bad record
df = spark.read.format("csv") \
    .option("mode", "failFast") \
    .schema(my_schema) \
    .load("path/to/data.csv")
```

#### 1.5 Advanced Handling — Headers and skipRows

If a CSV has a header row but you want your own column names from a manual schema, set `header` to `False` and use `skipRows` to skip the original header line.

```python
df = spark.read.format("csv") \
    .option("header", "false") \
    .option("skipRows", 1) \
    .schema(my_schema) \
    .load("path/to/file.csv")
```

> **⚠️ Pro-Tip — Order of Operations:** Always define your schema object _before_ the cell that reads data. In Databricks notebooks, referencing an undefined schema variable throws a `NameError`. Define → Read → Transform.

---

## 2. Transformations and Actions

### 🔗 YouTube Links

- [Transformation and Action in Spark — Manish Kumar](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is a Transformation vs an Action?**
- **Explain Lazy Evaluation in Spark. Why does it exist?**
- **What is the difference between Narrow and Wide Dependencies?**
- **What happens internally during a `groupBy`?** _(Data shuffling — moving IDs to the same partition)_
- **When is a Job created in Spark?** _(Whenever an Action is executed)_

---

### 📌 Important Topics

#### 2.1 Core Concept — Transformations

Transformations are **declarative instructions** to process or filter data. Spark does **not execute** them immediately — it builds a logical plan.

Common transformations: `filter()`, `select()`, `join()`, `groupBy()`, `distinct()`

```python
# Spark records this instruction but does NOTHING yet (Transformation)
df = spark.read.csv("data.csv").filter("age < 18")
```

#### 2.2 Core Concept — Actions

Actions trigger the actual computation and bring results back to the user (or write to storage). **Every Action creates a new Job.**

Common actions: `.show()`, `.count()`, `.collect()`, `.write.save()`

```python
# Spark EXECUTES the entire plan only now (Action)
df.show()   # Job 1
df.count()  # Job 2
```

#### 2.3 Lazy Evaluation

Unlike Python which runs line-by-line, Spark waits for an Action to see the **full execution plan** and optimize it via the Catalyst Optimizer before running anything.

#### 2.4 Narrow Dependency Transformations

**No data movement between partitions.** Each executor works independently on its local partition.

- Examples: `filter()`, `select()`, `map()`
- Real-world analogy: Each executor checks employees younger than 18 from its own partition — no cross-talk needed.

```python
# Narrow — each partition filtered independently, no shuffle
df_young = df.filter("age < 18").select("name", "department")
df_young.show()
```

#### 2.5 Wide Dependency Transformations

**Require data movement (Shuffle) across the network.** One output partition depends on data from _multiple_ input partitions. These are expensive — they involve disk I/O and network transfer.

- Examples: `join()`, `groupBy()`, `distinct()`, `repartition()`
- Real-world analogy: To sum total income per Employee ID, all records for ID "2" must be shuffled to the _same_ partition across executors.

```python
# Wide — triggers a shuffle across all executors
df_income = df.groupBy("employee_id").sum("income")
df_income.show()
```

> **⚠️ Production Tip:** Identify Wide Transformations in your pipeline and minimize them. Reducing shuffle stages is the single biggest lever for Spark performance improvement.

#### 2.6 The `.collect()` Danger

`.collect()` brings **ALL** data from every executor back to the Driver. If your executors hold 15 GB total but your Driver only has 10 GB memory → **Driver OOM Exception**.

```python
# DANGEROUS on large datasets — use sparingly
all_rows = df.collect()  # Crashes Driver if data > Driver memory
```

---

## 3. Spark Partitioning and Bucketing

### 🔗 YouTube Links

- [Partitioning and Bucketing in Spark](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is Partitioning in Spark?**
- **What is Bucketing in Spark?**
- **How do you decide between Partitioning vs Bucketing for your data?**
- **Do you know what Partitioning and Bucketing are in Spark?** _(opening question)_
- **What happens internally during a `groupBy`?**
- **⭐ VVI — Why should you avoid partitioning on high-cardinality columns?**
- **⭐ VVI — Why does `bucketBy` require `.saveAsTable()` and not `.save()`?**
- **⭐ VVI — How does Bucketing eliminate the Shuffle phase during Joins?**
- **⭐ VVI — What is the "Small File Problem" and how does it occur?**

---

### 📌 Important Topics

#### 3.1 Spark Partitioning

Partitioning physically divides data into **folder structures** based on distinct values of a column at write time. Spark uses **partition pruning** to skip irrelevant folders at read time.

```python
# Single-column partition
df.write.mode("overwrite") \
    .option("header", "true") \
    .partitionBy("address") \
    .parquet("/mnt/pyspark/partition_by_address")
# Creates: address=India/, address=USA/, address=Germany/
```

> **⭐ VVI — Cardinality Warning:** Partitioning on high-cardinality columns (like `user_id`) creates **thousands of tiny folders/files** — the "Small File Problem." This degrades NameNode metadata performance. **Always partition on low-cardinality columns** like `Country`, `Gender`, or `Year`.

#### 3.2 Multi-Column Partitioning

The **order of columns** in `partitionBy` defines the folder hierarchy. Wrong order = wrong query optimization.

```python
# Creates: address=India/gender=F/, address=India/gender=M/, ...
df.write.mode("overwrite") \
    .partitionBy("address", "gender") \
    .parquet("/mnt/pyspark/partition_address_gender")
```

> **⭐ VVI:** If you partition by `"Address"` then `"Gender"`, you get gender folders _inside_ each country folder — not the other way around. Incorrect ordering leads to unintended storage hierarchies that don't match your query patterns.

#### 3.3 Spark Bucketing

Bucketing distributes data into a **fixed number of files (buckets)** using a hash function on the specified column. It is the preferred strategy for **high-cardinality columns** where partitioning would fail.

```python
# Repartition first to avoid small file explosion, then bucket
df.repartition(3) \
    .write.format("parquet") \
    .bucketBy(3, "id") \
    .saveAsTable("bucket_table_id")
```

> **⭐ VVI:** `bucketBy` **cannot** be used with `.save()` on a file path. It requires `.saveAsTable()` because bucket metadata (number of buckets, bucket key) must be registered in the **Hive Metastore**.

#### 3.4 Bucketing — Shuffle Elimination on Joins

When two tables are bucketed on the **same join key** with the **same number of buckets**, Spark can skip the entire shuffle phase during a join. Bucket N of Table A always joins with Bucket N of Table B — no data movement required.

```python
df1 = spark.table("bucketed_table1")  # Bucketed by 'id' into 500 buckets
df2 = spark.table("bucketed_table2")  # Bucketed by 'id' into 500 buckets

# No shuffle needed — direct bucket-to-bucket join
df1.join(df2, "id").show()
```

> **⭐ VVI — Point Lookup Optimization:** For a search on a bucketed column (e.g., `id = 42`), Spark computes `hash(42) % num_buckets` and reads **only that bucket file** directly — drastically faster than a full table scan.

#### 3.5 Partitioning vs Bucketing — Decision Guide

| Criterion                  | Partitioning           | Bucketing                   |
| -------------------------- | ---------------------- | --------------------------- |
| **Best for**               | Filter / WHERE clauses | Joins & point lookups       |
| **Cardinality**            | Low (Country, Gender)  | High (User ID, Account No.) |
| **Output structure**       | Folder hierarchy       | Fixed N files               |
| **Consistent file count?** | No                     | Yes                         |
| **Hive Metastore needed?** | No                     | **Yes**                     |

> **⭐ VVI — Write Once, Read Many:** Plan your optimization strategy at write time based on _how the data will be queried downstream_ — not how it is ingested.

---

## 4. SparkSession vs SparkContext

### 🔗 YouTube Links

- [SparkSession and SparkContext](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is a Spark Session?**
- **What is a Spark Context?**
- **What are the differences between Spark Session and Spark Context (legacy vs. modern)?**
- **How do these components provide entry points to a Spark Cluster?**
- **What are the differences in terms of when to use `spark.read` vs `sc.textFile`?**
- **⭐ VVI — When would you use SparkContext over SparkSession?**
- **⭐ VVI — When would you use SparkSession over SparkContext?**

---

### 📌 Important Topics

#### 4.1 SparkSession — The Modern Entry Point

`SparkSession` (introduced in **Spark 2.0**) is the unified, single entry point to Spark. It encapsulates `SparkContext`, `SQLContext`, and `HiveContext` into one object. In environments like Databricks, it is pre-initialized as `spark`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# High-level DataFrame read via SparkSession
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("path/to/file")

df.show()
```

> **⭐ VVI — `getOrCreate()`:** This critical method either retrieves an existing SparkSession (with the same config) or creates a new one. It prevents duplicate resource allocation and is the production-safe pattern.

#### 4.2 SparkContext — The Legacy Entry Point (Pre-2.0)

Before Spark 2.0, developers managed separate contexts: `SparkContext` for RDDs, `SQLContext` for SQL, `HiveContext` for Hive, `StreamingContext` for streaming. `SparkContext` is still accessible via `spark.sparkContext` for RDD-level operations.

```python
# Accessing SparkContext from a SparkSession
sc = spark.sparkContext

# Low-level RDD operation (word count pattern)
rdd = sc.textFile("data.txt").flatMap(lambda line: line.split(" "))
word_count = rdd.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
word_count.collect()
```

#### 4.3 When to Use Which

| Use Case                                       | Entry Point                  |
| ---------------------------------------------- | ---------------------------- |
| Reading CSV, Parquet, JSON into DataFrames     | `spark.read` (SparkSession)  |
| Running SQL queries                            | `spark.sql()` (SparkSession) |
| Low-level RDD operations (Word Count, flatMap) | `sc` (SparkContext)          |
| Legacy RDD-based code                          | `sc` (SparkContext)          |
| Modern data engineering (structured data)      | `SparkSession` (default)     |

> **⭐ VVI:** The Spark Session acts as the **gateway to the cluster**. Without initializing it, you cannot request RAM, CPU, or Executors from the Resource Manager.

---

## 5. Spark Execution Hierarchy — Jobs, Stages, Tasks

### 🔗 YouTube Links

- [Jobs, Stages, Tasks in Spark](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is the difference between a Spark Application, Job, Stage, and Task?**
- **When is a Job created?** _(Every time an Action is executed)_
- **How do Narrow and Wide dependencies affect the creation of Stages?**
- **What determines the number of Tasks in a particular Stage?**
- **How many Stages are generated during a shuffle operation?**
- **How do you determine the number of Jobs created for a specific piece of PySpark code?**
- **⭐ VVI — What is the default number of shuffle partitions in Spark?**
- **⭐ VVI — How do you calculate the total number of Tasks in a Job?**
- **⭐ VVI — When does a new Stage get created?**

---

### 📌 Important Topics

#### 5.1 The Execution Hierarchy

```
Application
  └── Job  (1 per Action)
        └── Stage  (1 per Wide Dependency boundary)
              └── Task  (1 per Partition in that Stage)
```

- **Application:** The entire submitted Spark program (`spark-submit`). Can contain many Jobs.
- **Job:** Created every time an Action (`.collect()`, `.show()`, `.save()`) is triggered.
- **Stage:** A logical boundary between sets of tasks, defined by shuffle (Wide Dependency) boundaries.
- **Task:** The atomic unit of work — executes the stage logic on a **single partition** within an Executor.

```python
# This single script = 1 Application, 2 Jobs
df = spark.read.csv("data.csv")  # No job yet (transformation)
df.collect()                      # Job 1 triggered
df.count()                        # Job 2 triggered
```

#### 5.2 Stage Creation Rules

> **⭐ VVI:** A new Stage is created **only when Spark encounters a Wide Dependency** (Shuffle) such as `repartition()`, `groupBy()`, or `join()`.

> **⭐ VVI:** Narrow transformations like `filter()` and `select()` do **not** create new stages — they are "pipelined" and executed within the same stage.

```python
# Stage 1: Read + Repartition (Wide Dependency — creates new Stage)
df_partitioned = df.repartition(2)

# Stage 2: Filter + Select (Narrow — pipelined within same Stage as above's output)
df_filtered = df_partitioned.filter("age > 25").select("name", "age")

df_filtered.show()  # Action — triggers Job with 2 Stages
```

#### 5.3 Task Count Calculation

**One Task is created per Partition per Stage.**

```
Total Tasks in a Job = Σ (Partitions in each Stage)
```

```python
# Check partition count to predict task count
print(df.rdd.getNumPartitions())
```

> **⭐ VVI — Default Shuffle Partitions:** By default, `spark.sql.shuffle.partitions = 200`. This means `groupBy` or `join` operations create **200 tasks** in the shuffle stage, even if your data is tiny. Always tune this for small datasets.

```python
# Tune shuffle partitions for better performance
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

> **⭐ Pro-Tip:** To optimize a Spark Job, minimize the number of Wide Dependencies and ensure your partition count matches your cluster's executor capacity — too few = idle executors, too many = excessive task overhead.

---

## 6. DataFrames, Schemas, and RDDs

### 🔗 YouTube Links

- [DataFrames and Manual Creation](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **How is a DataFrame physically represented in memory?**
- **What constitutes a Spark Schema, and how do you access it programmatically?**
- **How do you manually create a DataFrame from a list of raw data for unit testing?**
- **What are the two primary ways to perform data transformations in Spark?** _(DataFrame API + Spark SQL)_
- **Why is proficiency in the DataFrame API critical for passing technical data engineering interviews?**
- **⭐ VVI — What does `spark.createDataFrame()` do?**
- **⭐ VVI — How do you solve complex logical problems using DataFrames?** _(LeetCode-style)_

---

### 📌 Important Topics

#### 6.1 What is a Spark DataFrame?

A DataFrame is a **distributed data structure** organized into named columns, behaving like a relational table. Physically, it is stored in memory as a collection of **`Row` type objects** in byte form.

A **Schema** = Column Name + Data Type (e.g., `id` as `IntegerType`, `name` as `StringType`).

#### 6.2 Manually Creating a DataFrame

Creating a DataFrame requires two components: **the data** (list of tuples) and **the schema** (list of column names or a `StructType`).

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ManualDF").getOrCreate()

# Step 1: Define the data
data = [(1, "India"), (2, "USA"), (3, "Germany")]

# Step 2: Define the schema
schema = ["id", "address"]  # Simple list of strings (Spark infers types)

# Step 3: Create the DataFrame
df = spark.createDataFrame(data=data, schema=schema)
df.show()
df.printSchema()
```

For explicit type control, use `StructType`:

```python
schema = StructType([
    StructField("id",      IntegerType(), True),
    StructField("address", StringType(),  True)
])
df = spark.createDataFrame(data=data, schema=schema)
```

#### 6.3 High-Level Data Pipeline Architecture

The fundamental data engineering lifecycle: **Read → Transform → Write**.

```python
# READ
df = spark.read.parquet("source_path")

# TRANSFORM
df_transformed = df.filter(df["age"] > 21)

# WRITE
df_transformed.write.mode("overwrite").save("target_path")
```

> **⭐ VVI — Interview Critical:** The "Transformation" stage is the **most tested area** in data engineering interviews. Strong DataFrame API skills are non-negotiable — if you cannot solve transformation problems live, rejection likelihood is high.

> **⭐ Pro-Tip:** Practice converting raw log data or CSV snippets into DataFrames manually. In interviews, you will often be given a small dataset and asked to filter, select, or aggregate it on the fly.

---

## 7. DataFrameReader API — Reading Data

### 🔗 YouTube Links

- [DataFrameReader API and CSV Reading](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is the difference between `inferSchema` set to `true` versus `false`?**
- **What are the three read modes available in Spark?**
- **Which read mode is considered the default?**
- **What happens to a Spark job when it encounters a malformed record in `failFast` mode?**
- **How do you handle CSV files that contain headers?**

---

### 📌 Important Topics

#### 7.1 The DataFrameReader API Structure

The standard structure chains: `format()` → `option()` (multiple) → `schema()` → `load()`.

```python
file_path = "/FileStore/tables/flight_data.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "permissive") \
    .load(file_path)

df.show(5)
df.printSchema()
```

> **⭐ VVI — Default Format:** If no format is specified, Spark defaults to **Parquet**.

> **⭐ VVI — The `spark` variable:** In Databricks, `spark` (the `SparkSession`) is pre-initialized. Always use it as your entry point for `spark.read`.

#### 7.2 inferSchema — True vs False

- `inferSchema=True`: Spark performs an **extra full pass** over the data to determine column types. Slower on large datasets, but convenient.
- `inferSchema=False` (default): All columns are loaded as `StringType`. Fast, but requires explicit casting downstream.

```python
# Always verify schema after reading
df.printSchema()
```

#### 7.3 CSV Headers

By default, `header` is `false`. If your file has column names in the first row, you **must** explicitly set it.

```python
# Without this, the header row becomes a data row — silent corruption
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(path)
```

#### 7.4 Read Modes Summary

| Mode            | Behaviour                                  | When to Use                    |
| --------------- | ------------------------------------------ | ------------------------------ |
| `permissive`    | Sets malformed fields to `null`, continues | Production pipelines (default) |
| `dropMalformed` | Silently drops bad rows                    | Cleaning data at ingestion     |
| `failFast`      | Throws exception on first bad record       | Debugging / financial data     |

```python
# Use dropMalformed to clean data silently at ingestion
df = spark.read.format("csv") \
    .option("mode", "dropMalformed") \
    .option("header", "true") \
    .load(path)

# Use show(n) to spot-check the result
df.show(5)
```

> **⭐ Pro-Tip — Databricks File Paths:** When uploading files in Databricks, always copy the exact **File Store path** provided after upload (e.g., `dbfs:/FileStore/tables/my_file.csv`) and use it in your `load()` call.

---

## 8. Corrupted Records Handling

### 🔗 YouTube Links

- [Corrupted Data Handling in Spark](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **⭐ VVI — When exactly do you define a record as "corrupt" or "bad"?**
- **⭐ VVI — What happens when a corrupted record is encountered in Permissive, DropMalformed, and FailFast modes?**
- **⭐ VVI — How can you identify and print the specific contents of a "bad" record?**
- **⭐ VVI — Where and how should you store corrupted records for downstream auditing or reprocessing?**
- **Have you ever handled corrupted records in your Spark pipelines?**

---

### 📌 Important Topics

#### 8.1 What Makes a Record "Corrupt"?

A record is considered **corrupted** when its structure does not match the expected schema. Common causes:

- **CSV:** A data value contains the delimiter itself (e.g., `"Bangalore, India"` in a comma-delimited file) — Spark miscounts columns.
- **JSON:** A missing closing bracket or malformed nesting.

```
# Expected: ID | Name | Address | Nominee  (4 columns)
# Actual parse: 3 | Preetam | Bangalore | India | Nominee3  (5 columns)
# The extra comma inside "Bangalore, India" is the corruption trigger
```

#### 8.2 Viewing Bad Records with `_corrupt_record`

To **see** the actual content of a bad record, you must define a **manual schema** and include the special reserved column `_corrupt_record`. You cannot rely on `inferSchema` for this.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id",              IntegerType(), True),
    StructField("name",            StringType(),  True),
    StructField("_corrupt_record", StringType(),  True)  # Reserved column
])

df = spark.read \
    .schema(schema) \
    .option("mode", "permissive") \
    .csv("path/to/data.csv")

# Use truncate=False to see the full raw content of bad records
df.filter(df["_corrupt_record"].isNotNull()).show(truncate=False)
```

#### 8.3 Storing Bad Records with `badRecordsPath`

For production pipelines, printing to a console is insufficient. Use `badRecordsPath` to redirect bad records to persistent storage as JSON files for audit and reprocessing.

```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("badRecordsPath", "/FileStore/tables/bad_records_log") \
    .load("path/to/data.csv")

# Read back the stored bad records later
bad_df = spark.read.json("/FileStore/tables/bad_records_log/")
bad_df.show(truncate=False)
```

> **Audit Metadata:** Each stored JSON file includes the **record's content**, the **source file path**, and the **reason for failure** — enabling communication with source systems about data quality issues.

> **⭐ Pro-Tip — Best Practice:** Storing bad records ensures **100% data accountability** — every source record is either in the main table or in the error log. Combine with `permissive` mode for zero data loss at ingestion.

> **⚠️ Note:** `badRecordsPath` has a conflict with certain custom mode settings in some Spark versions — it may require `permissive` behavior to log correctly.

---

## 9. Column Selection, Expressions, and Spark SQL

### 🔗 YouTube Links

- [Column Operations and Spark SQL](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is the difference between selecting a column using a String vs. the `col()` function?**
- **What are the different ways to select columns in PySpark, and when do you use each?**
- **What are Column Expressions (`expr`), and how do they bridge the DataFrame API and Spark SQL?**
- **What constitutes a Spark Schema, and how do you access it programmatically?**
- **How is a DataFrame physically represented in memory?** _(Collection of Row objects)_
- **⭐ VVI — What must you do before running `spark.sql()`?**
- **⭐ VVI — What is `expr()` and what makes it unique vs `col()`?**

---

### 📌 Important Topics

#### 9.1 Schema Inspection

```python
# Full schema with names, types, and nullability
df.printSchema()

# Just column names as a list
column_list = df.columns
```

> **⭐ VVI:** `printSchema()` displays the column names, types, and **nullable** status (whether a column can contain null values). Always run this after a read or join to verify the schema is what you expect.

#### 9.2 Column Selection Methods

There are multiple equivalent ways to select columns. They can be mixed in the same `select()` call.

```python
# Method 1: String (simple, no manipulation)
df.select("name", "salary").show()

# Method 2: col() function (required for calculations)
from pyspark.sql.functions import col
df.select(col("id") + 5, col("name")).show()

# Method 3: DataFrame dot notation
df.select(df["id"], df["name"]).show()
```

> **⭐ VVI:** Simple strings in `select("column_name")` are efficient for straight column selection, but **cannot handle math operations or manipulations**. For anything beyond selection, use `col()` or `expr()`.

> **⭐ VVI:** To use `col()`, you **must** import it from `pyspark.sql.functions`. Forgetting this import throws a `NameError` at runtime.

#### 9.3 Column Expressions — `expr()`

`expr()` takes a SQL-like string, parses it internally, and converts it into a `Column` object. It is the most versatile selection method — supports aliasing, arithmetic, and complex SQL logic in a single string.

```python
from pyspark.sql.functions import expr

# Aliasing and arithmetic in one call
df.select(
    expr("id + 5 AS new_id"),
    expr("name AS employee_name")
).show()
```

> **⭐ VVI:** `expr()` is the **cleanest way to alias** during selection. Internally, Spark parses the string into a Column plan node — identical performance to `col()`.

#### 9.4 Spark SQL via Temporary Views

For engineers more comfortable with SQL, Spark allows running SQL queries directly against DataFrames. **A temporary view must be created first.**

```python
# Step 1: Register the DataFrame as a Temp View
df.createOrReplaceTempView("emp_table")

# Step 2: Run SQL against it
sql_df = spark.sql("SELECT id, name FROM emp_table WHERE id > 1")
sql_df.show()

# Use triple quotes for multi-line SQL
sql_df2 = spark.sql("""
    SELECT department, COUNT(*) AS headcount
    FROM emp_table
    GROUP BY department
""")
```

> **⭐ VVI — Temp View Scope:** `createOrReplaceTempView` creates a virtual view that exists **only for the duration of the SparkSession**. It does not persist to disk. Use `createOrReplaceGlobalTempView` for cross-session sharing.

> **⭐ VVI:** `spark.sql("SELECT ...")` returns a **DataFrame** — you can seamlessly switch between SQL and the DataFrame API on the same data.

---

## 🔑 Quick Reference — VVI Master List (v2 — 35 Points)

### v1 — Modules 1–9

| #   | VVI Point                                                                        | Module            |
| --- | -------------------------------------------------------------------------------- | ----------------- |
| 1   | Partitioning on high-cardinality columns → **Small File Problem**                | Partitioning      |
| 2   | Order of columns in `partitionBy` determines folder nesting                      | Partitioning      |
| 3   | `bucketBy` requires `.saveAsTable()` — not `.save()`                             | Bucketing         |
| 4   | Bucketing eliminates Shuffle phase when both tables have same bucket key + count | Bucketing         |
| 5   | Default `spark.sql.shuffle.partitions = 200` — tune for small data               | Stages & Tasks    |
| 6   | A new Stage is created only at Wide Dependency (Shuffle) boundaries              | Stages & Tasks    |
| 7   | `filter()` and `select()` are Narrow — they do NOT create new stages             | Stages & Tasks    |
| 8   | Total Tasks = Sum of Partitions across all Stages in a Job                       | Stages & Tasks    |
| 9   | A Job is triggered by every Action; 5 Actions = 5 Jobs                           | Jobs              |
| 10  | Use `SparkContext` for RDD ops; `SparkSession` for DataFrame ops                 | Session           |
| 11  | `getOrCreate()` prevents duplicate session/resource conflicts                    | Session           |
| 12  | Default Spark read format is **Parquet**                                         | Reader API        |
| 13  | `inferSchema=True` requires an extra data scan — slow at scale                   | Reader API        |
| 14  | `_corrupt_record` column required to surface bad records in Permissive mode      | Corrupted Records |
| 15  | `badRecordsPath` stores bad records as JSON for audit and reprocessing           | Corrupted Records |
| 16  | `col()` must be imported from `pyspark.sql.functions`                            | Column Ops        |
| 17  | `createOrReplaceTempView()` required before `spark.sql()`                        | Spark SQL         |
| 18  | `spark.sql()` returns a DataFrame — switch freely between SQL and API            | Spark SQL         |
| 19  | `.collect()` on large data → **Driver OOM Exception**                            | Actions           |
| 20  | Transformation skills are the **#1 most tested** area in DE interviews           | General           |

### v2 — Modules 10–13 (New)

| #   | VVI Point                                                                                                                   | Module          |
| --- | --------------------------------------------------------------------------------------------------------------------------- | --------------- |
| 21  | UDFs force a Python Worker inside the JVM → **severe performance overhead**                                                 | Cluster Arch    |
| 22  | If the Driver fails, **all associated Executors are terminated**                                                            | Cluster Arch    |
| 23  | Application Master and Spark Driver are often the **same entity**                                                           | Cluster Arch    |
| 24  | Built-in Spark functions run in JVM directly — always prefer over UDFs                                                      | Cluster Arch    |
| 25  | Spark's **default join strategy is Sort Merge Join** (SMJ) — most stable                                                    | Join Strategies |
| 26  | SMJ complexity: `O(n log n)` — Shuffle Hash Join join phase: `O(1)` but OOM risk                                            | Join Strategies |
| 27  | Shuffle Hash Join risks **OOM** if the hash table exceeds executor memory                                                   | Join Strategies |
| 28  | Broadcast Nested Loop Join is `O(n²)` — used only for **non-equi joins**                                                    | Join Strategies |
| 29  | Default auto-broadcast threshold = **10 MB** (`spark.sql.autoBroadcastJoinThreshold`)                                       | Broadcast Join  |
| 30  | Set threshold to `-1` to **disable** automatic broadcast joins entirely                                                     | Broadcast Join  |
| 31  | Broadcasting a large table → saturates network + OOM on every executor                                                      | Broadcast Join  |
| 32  | Use `.explain()` to verify join strategy — look for `BroadcastHashJoin` or `ShuffleExchange`                                | Broadcast Join  |
| 33  | `row_number()` = unique sequential rank; `rank()` skips after ties; `dense_rank()` does NOT skip                            | Window Funcs    |
| 34  | Without `orderBy`, window aggregation covers the full partition; with `orderBy` → **running total**                         | Window Funcs    |
| 35  | For rolling N-row average: `rowsBetween(-(N-1), 0)`; for last record: `rowsBetween(unboundedPreceding, unboundedFollowing)` | Window Funcs    |

---

_Document v2 generated 02 Apr 2026 from PySpark OneNote study notes. All code examples are idiomatic PySpark, tested against Spark 3.x / Databricks Runtime._

---

---

## 10. Spark Cluster Architecture — Driver, Executors & UDFs

### 🔗 YouTube Links

- [Spark Cluster Architecture](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)
- [Spark Architecture](https://github.com/shivamchoudhury06/practice/blob/master/Spark_arch.png)

---

### ❓ Interview Questions

- **What is a Spark Cluster, and how do Master and Worker nodes interact?**
- **What is the role of a Resource Manager (YARN/Kubernetes) in Spark?**
- **Explain the difference between the Application Master and the Spark Driver.**
- **What is the lifecycle of a Spark application from submission to shutdown?**
- **How does Spark (written in Scala/Java) execute Python code?**
- **⭐ VVI — Why are UDFs considered performance killers in PySpark?**
- **⭐ VVI — What happens to Executors if the Driver is terminated?**

---

### 📌 Important Topics

#### 10.1 Spark Cluster Architecture

A Spark Cluster is a collection of machines connected via a network operating on a **Master-Slave architecture**. One "Master" node coordinates, while multiple "Worker" nodes do the computation. A cluster's total capacity = sum of Cores and RAM across all workers (e.g., 10 × 100 GB = 1 TB cluster).

- **Resource Manager (YARN/Kubernetes):** Lives on the Master node. Allocates "containers" (isolated RAM/CPU slices) on worker nodes. Node Managers on each Worker manage individual machine resources.
- **Application Master:** A containerized process allocated when an application is submitted. Entry point where the main code begins execution.

> **⭐ VVI:** The **Application Master and the Spark Driver are often the same entity** — they act as the central brain orchestrating the entire job.

#### 10.2 Executor Lifecycle

When an application is submitted:

1. Resource Manager allocates a container → Application Master starts
2. Driver initializes → requests more containers for Executors
3. Resource Manager identifies available workers → returns container details to Driver
4. Executors spin up on worker nodes → process data tasks sent by Driver
5. Driver finishes → **Executors automatically shut down**

> **⭐ VVI:** If the **Driver (Application Master) fails or is terminated, all associated Executors are also terminated immediately.**

```python
# Resource requirements defined at session creation
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ArchitectureStudy") \
    .config("spark.executor.memory", "25g") \
    .config("spark.executor.cores", "5") \
    .config("spark.driver.memory", "20g") \
    .getOrCreate()

# Accessing SparkContext through the Driver
sc = spark.sparkContext
print("Driver App Name:", sc.appName)

# Executor activity — data distributed across 5 partitions
rdd = spark.sparkContext.parallelize(range(100), 5)
result = rdd.map(lambda x: x * 2).collect()
```

#### 10.3 How Spark Runs Python Code

Spark's core is written in **Scala/Java (JVM)**. PySpark code is first handled by a Python wrapper (PySpark Driver) and then converted into a JVM-compatible main method (Application Driver). This cross-language bridge introduces overhead.

#### 10.4 UDFs — The Performance Killer

When using Python UDFs, Spark must spin up a **Python Worker process inside the JVM executor container**. Data must cross the JVM ↔ Python boundary — this serialization/deserialization is expensive.

> **⭐ VVI:** UDFs force Spark to create a Python runtime alongside the JVM → **always prefer built-in Spark functions**, which run natively in the JVM.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# ❌ NON-OPTIMIZED: Python UDF — requires Python Worker, data crosses JVM boundary
square_udf = udf(lambda x: x * x, IntegerType())
df.select(square_udf("id")).show()

# ✅ OPTIMIZED: Built-in Spark function — runs directly in JVM, no overhead
df.select(df["id"] ** 2).show()
```

> **⭐ Pro-Tip:** Before writing a UDF, always check if the same logic can be expressed using `pyspark.sql.functions`. In 90%+ of cases, it can.

---

## 11. Spark Join Strategies — Sort Merge vs Shuffle Hash

### 🔗 YouTube Links

- [Spark Join | Sort vs Shuffle | Lec-13](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **Why is a Join operation considered "expensive" in a distributed environment?**
- **What are the different Join Strategies available in Spark?**
- **What is the difference between Shuffle Hash Join and Shuffle Sort Merge Join?**
- **What is a Wide Dependency Transformation?** _(in context of joins)_
- **When and how do we need a Broadcast Join?**
- **⭐ VVI — What is Spark's default join strategy and why?**
- **⭐ VVI — What is the OOM risk specific to Shuffle Hash Join?**
- **⭐ VVI — What is the time complexity of Sort Merge Join? Shuffle Hash Join?**

---

### 📌 Important Topics

#### 11.1 Why Joins Are Expensive

Joins require **Shuffling** — moving data across the cluster so that records with the same key land on the same executor. Keys may start on different worker nodes, requiring network I/O. If data movement is excessive or the cluster is misconfigured → **Cluster Choking**.

> **⭐ VVI:** By default, Spark creates **200 partitions** whenever a Wide Dependency transformation like a Join or GroupBy executes.

```python
# Standard join — defaults to Sort Merge Join for large tables
df1.join(df2, "id").show()

# Always use .explain() to verify which strategy was chosen
df1.join(df2, "id").explain()
```

#### 11.2 The Five Join Strategies in Spark

| Strategy                    | When Used                             | Complexity        | OOM Risk                 |
| --------------------------- | ------------------------------------- | ----------------- | ------------------------ |
| **Broadcast Hash Join**     | One table ≤ 10 MB                     | `O(1)` join       | Low (if sized correctly) |
| **Shuffle Hash Join**       | Smaller table fits in executor memory | `O(1)` join phase | **High**                 |
| **Shuffle Sort Merge Join** | Both tables large (default)           | `O(n log n)`      | Low                      |
| **Broadcast Nested Loop**   | Non-equi joins                        | `O(n²)`           | High                     |
| **Cartesian Join**          | `crossJoin()` — all combinations      | `O(n²)`           | Very High                |

```python
# Forcing a cross join (Cartesian — use only when absolutely necessary)
df1.crossJoin(df2).show()

# Tune shuffle partitions for joins
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

#### 11.3 Shuffle Sort Merge Join (SMJ) — The Default

SMJ is Spark's **default join strategy**. It has two phases after the shuffle:

1. **Sort:** Each executor sorts its partition by the join key — `O(n log n)`
2. **Merge:** Sorted partitions are merged (like merge sort) — efficient, linear pass

> **⭐ VVI:** Spark prefers SMJ by default because it is **highly stable and avoids OOM errors** — it uses CPU for sorting rather than keeping everything in memory.

```python
# SMJ is used automatically when both tables are large
# No special code needed — it's Spark's default
joined_df = df_large1.join(df_large2, df_large1["id"] == df_large2["id"], "inner")
joined_df.explain()  # Look for "SortMergeJoin" in the physical plan
```

#### 11.4 Shuffle Hash Join

Builds a **hash table** of the smaller joining table in executor memory. The larger table is streamed through and probed against the hash table.

- **Join phase complexity:** `O(1)` — faster than SMJ for the join itself
- **Trade-off:** The hash table must stay entirely in memory

> **⭐ VVI:** Shuffle Hash Join risks **OOM exceptions** if the hash table exceeds allocated executor memory. SMJ avoids this by using disk-spillable sorting.

```python
# Spark may choose Hash Join if one side is small — check with explain()
df_large.join(df_small, "id").explain()
```

> **⭐ Pro-Tip:** Always use `.explain()` after a join to verify which strategy Spark selected. If a join is choking the cluster, consider broadcasting the smaller table (Module 12) or pre-partitioning data to reduce shuffling.

---

## 12. Broadcast Join in Spark

### 🔗 YouTube Links

- [Broadcast Join in Spark | Lec-14](https://www.youtube.com/watch?v=f-B6O9iA7a8)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **Why do we need Broadcast Hash Join when we already have Shuffle Hash Join and SMJ?**
- **How does a Broadcast Join work internally within the Spark architecture?**
- **What is the difference between a Broadcast Hash Join and a Shuffle-based join?**
- **In which scenarios does a Broadcast Join fail or create performance issues?**
- **How can you change the default broadcast table size, and what is that default value?**
- **⭐ VVI — What is the default auto-broadcast threshold?**
- **⭐ VVI — What is the primary advantage of a Broadcast Join?**
- **⭐ VVI — What happens if you try to broadcast a table that is too large?**
- **⭐ VVI — Where does OOM occur in a failed Broadcast Join — Driver or Executor?**

---

### 📌 Important Topics

#### 12.1 The Broadcast Concept

In a standard Spark cluster, the Driver coordinates Executors. A "Broadcast" works like a **live TV broadcast**: information is sent from one central source (the Driver) and distributed to ALL listeners (Executors) simultaneously.

> **⭐ VVI — Primary Advantage:** Broadcast Join **eliminates Shuffling** — the expensive process of moving large data across the network that often chokes clusters.

#### 12.2 How Broadcast Join Works Internally

1. Driver collects the entire small table from the cluster into its own memory
2. Driver sends a **full copy** of that table to every single Executor
3. Each Executor becomes "self-sufficient" — it holds the entire small table locally
4. Each Executor joins its partition of the large table with its local copy of the small table
5. **No network traffic between Executors** — zero shuffle exchange

> **⭐ VVI:** In a Broadcast Join, the **Driver collects the entire small table** and distributes it. If the Driver doesn't have enough memory → **Driver OOM**.

```python
from pyspark.sql.functions import broadcast

# Explicitly hint Spark to use Broadcast Join
joined_df = customer_df.join(broadcast(sales_df), "customer_id")
joined_df.show()

# Verify in physical plan — look for "BroadcastHashJoin" and "BroadcastExchange"
joined_df.explain()
```

#### 12.3 Automatic Broadcast — Threshold Configuration

Spark automatically uses Broadcast Join if a table's size is **≤ 10 MB** (default).

```python
# Check current threshold
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# Increase to 50 MB (in bytes) for larger small tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)

# Disable automatic broadcast entirely
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

> **⭐ VVI:** Default threshold = **10 MB**. Increasing it can boost performance but raises OOM risk on both Driver and Executors.

#### 12.4 Failure Scenarios

| Failure Point          | Cause                                                              | Effect                                                            |
| ---------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------- |
| **Driver OOM**         | Small table too large to fit in Driver memory                      | Job fails at broadcast collection step                            |
| **Executor OOM**       | Executor near capacity + receives broadcast + join multiplies data | Executor-level OOM during the join                                |
| **Network saturation** | Broadcasting a 1 GB+ table                                         | Every executor receives 1 GB simultaneously — network overwhelmed |

> **⭐ VVI:** Broadcasting a table that is **too large** will saturate the network and exhaust the memory of **every executor** in the cluster — a cluster-wide failure.

> **⭐ VVI:** If a broadcast fails, Spark may fall back to Sort Merge Join or fail the job entirely.

> **⭐ Pro-Tip:** Use the Spark Web UI to verify join strategy. A successful Broadcast Join shows `BroadcastExchange` in the plan — not `ShuffleExchange`. Always monitor memory usage when increasing broadcast thresholds.

---

## 13. Window Functions in PySpark

### 🔗 YouTube Links

- [Window Function in PySpark | rank and dense_rank | Lec-15](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [lead and lag in Spark | Window Function | Lec-16](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [rowsBetween and rangeBetween in Spark | Window Function | Lec-17](https://www.youtube.com/watch?v=R9I1K5vV-5E)
- [NotebookLM Reference](https://notebooklm.google.com/notebook/33660aad-8fdc-441a-a5c0-bfcd01d92ce8)

---

### ❓ Interview Questions

- **What is the primary difference between `row_number`, `rank`, and `dense_rank`?**
- **What are the three arguments accepted by `lag` and `lead` functions?**
- **What is the default window frame in Spark, and why can it cause `last()` to return incorrect results?**
- **What is the difference between `unboundedPreceding` and `unboundedFollowing`?**
- **How do you calculate the top 2 highest-earning employees in each department?**
- **How do you calculate the percentage growth or decline in sales vs. the previous month?**
- **How do you compute a 3-month rolling average or running sum in PySpark?**
- **⭐ VVI — What happens to window aggregation when orderBy is present vs. absent?**
- **⭐ VVI — How do you use `rowsBetween` for rolling N-row calculations?**
- **⭐ VVI — Why is `dense_rank()` preferred for "Top N per group" logic?**

---

### 📌 Important Topics

#### 13.1 Window Function Fundamentals

Window functions perform calculations across a **set of rows related to the current row**. Unlike `groupBy`, **window functions do NOT collapse rows** — every input row remains in the output with its original detail plus the aggregated value.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

# partitionBy = defines the "window" boundary (e.g., per Department)
# orderBy = defines the sequence within that window
windowSpec = Window.partitionBy("department").orderBy("salary")

# Adds a running total column WITHOUT collapsing rows
df.withColumn("running_total", sum(col("salary")).over(windowSpec)).show()
```

> **⭐ VVI:** `partitionBy` defines the **frame boundary** (like a GROUP BY but non-collapsing), while `orderBy` determines the sequence of rows within that frame.

> **⭐ VVI — Critical Behaviour:** If a window aggregation (like `sum`) has **no `orderBy`**, Spark considers the **entire partition** as the window. If `orderBy` is present, it defaults to a **running total from partition start to the current row**.

#### 13.2 Ranking Functions — row_number, rank, dense_rank

| Function       | Tie Behaviour                     | Output Example (tie at 2nd) |
| -------------- | --------------------------------- | --------------------------- |
| `row_number()` | Unique sequential — no ties       | 1, 2, 3, 4                  |
| `rank()`       | Ties get same rank, **gap after** | 1, 2, 2, 4                  |
| `dense_rank()` | Ties get same rank, **no gap**    | 1, 2, 2, 3                  |

> **⭐ VVI:** Use `dense_rank()` for "Top N per group" logic — it includes all tied participants without skipping ranks. `rank()` leaves gaps which can cause you to miss participants in strict Top-N cuts.

```python
from pyspark.sql.functions import row_number, rank, dense_rank

# Use .desc() in orderBy to rank from highest to lowest
windowSpec = Window.partitionBy("dept").orderBy(col("salary").desc())

df.withColumn("rn",  row_number().over(windowSpec)) \
  .withColumn("rk",  rank().over(windowSpec)) \
  .withColumn("drk", dense_rank().over(windowSpec)) \
  .show()

# Top 2 employees per department
df.withColumn("rank", dense_rank().over(windowSpec)) \
  .filter(col("rank") <= 2) \
  .show()
```

#### 13.3 Analytical Functions — lag and lead

`lag` accesses data from a **previous row**; `lead` accesses data from a **subsequent row** within the same window. Essential for time-series analysis and trend detection.

**Three arguments:** `(column_name, offset, default_value)`

- `offset` = number of rows to go back (lag) or forward (lead)
- `default_value` = value used when no prior/next row exists (e.g., first/last record)

```python
from pyspark.sql.functions import lag

windowSpec = Window.partitionBy("product").orderBy("sales_date")

# Add previous month's sales — default 0 if no prior row
df.withColumn("prev_month_sales", lag("sales", 1, 0).over(windowSpec)) \
  .withColumn(
      "growth_pct",
      (col("sales") - col("prev_month_sales")) / col("sales") * 100
  ) \
  .show()
```

> **⭐ VVI:** To calculate **month-over-month growth**, use `lag` to pull the previous month's value into the current row, then apply: `(Current - Previous) / Current * 100`.

> Always define a clear `orderBy` (usually a date column) — otherwise Spark doesn't know what "previous" or "next" means.

#### 13.4 Advanced Window Frames — rowsBetween and rangeBetween

Frame notation:

- `0` = current row
- Negative numbers = rows **before** the current row (preceding)
- Positive numbers = rows **after** the current row (following)
- `Window.unboundedPreceding` = from the very start of the partition
- `Window.unboundedFollowing` = to the very end of the partition

```python
from pyspark.sql.functions import avg, first, last

# Rolling 3-month average — current row + 2 rows before
rollingWin = Window.partitionBy("product") \
    .orderBy("date") \
    .rowsBetween(-2, 0)

df.withColumn("rolling_avg", avg("sales").over(rollingWin)).show()

# Absolute first and last value across the entire partition
fullWin = Window.partitionBy("product") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn("first_val",  first("sales").over(fullWin)) \
  .withColumn("latest_val", last("sales").over(fullWin)) \
  .show()
```

> **⭐ VVI — Rolling N-row average:** Use `rowsBetween(-(N-1), 0)` where `-( N-1)` is how many rows to look back and `0` is the current row. For a 3-month window: `rowsBetween(-2, 0)`.

> **⭐ VVI — Getting the absolute last record:** The default window frame (start-to-current-row) means `last()` returns the current row's value. To get the true last record of the entire partition, you **must** use `rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)`.

> **⭐ Pro-Tip:** Window functions are more efficient than performing a `groupBy` followed by a `join` to re-attach aggregated values back to individual rows. They do both in one step without collapsing the dataset.
