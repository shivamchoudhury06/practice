# PySpark Master Reference Document

> **Compiled from Pyspark_Notes.one** | Format: Senior Data Engineer Review  
> Sections: YouTube Links · Interview Questions · Important Topics · Code Examples

---

## Table of Contents

1. [Manual Schemas in PySpark](#1-manual-schemas-in-pyspark)
2. [Transformations and Actions](#2-transformations-and-actions)
3. [Spark Partitioning and Bucketing](#3-spark-partitioning-and-bucketing)
4. [SparkSession vs SparkContext](#4-sparksession-vs-sparkcontext)
5. [Spark Execution Hierarchy — Jobs, Stages, Tasks](#5-spark-execution-hierarchy--jobs-stages-tasks)
6. [DataFrames, Schemas, and RDDs](#6-dataframes-schemas-and-rdds)
7. [DataFrameReader API — Reading Data](#7-dataframereader-api--reading-data)
8. [Corrupted Records Handling](#8-corrupted-records-handling)
9. [Column Selection, Expressions, and Spark SQL](#9-column-selection-expressions-and-spark-sql)

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

## 🔑 Quick Reference — VVI Master List

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

---

_Document generated from PySpark OneNote study notes. All code examples are idiomatic PySpark, tested against Spark 3.x / Databricks Runtime._
