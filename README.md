# 📁 Data Pipeline Project Using AWS, Apache Spark & Airflow

## 📌 Project Overview

This project is about building a complete data pipeline using cloud services, mainly AWS. The goal is to collect data from different sources, process it using Apache Spark, and automate everything using Apache Airflow.

---
## 🧰 Tech Stack

Here are the tools and technologies used to build and manage our data pipeline:

### 🔄 Data Ingestion
- **Snowflake** – Cloud data warehouse used as one of the data sources
- **Amazon S3** – Cloud object storage used as both a data source and destination
- **Web API** – External REST API used for real-time data fetching

### ⚡ Data Processing
- **PySpark** – Core engine for distributed data processing and transformations
- **Parquet Format** – Columnar storage format used for efficient read/write operations

### ☁️ Cloud Infrastructure
- **AWS EMR (Elastic MapReduce)** – Managed cluster platform for running big data jobs (using r5.xlarge instances)
- **AWS S3** – Storage layer for both intermediate and final datasets
- **AWS Secret Manager** – Securely stores and retrieves Snowflake credentials

### 📅 Orchestration & Scheduling
- **Apache Airflow** – Used to schedule, orchestrate, and monitor Spark jobs via DAGs stored in S3

### 🔐 Security
- **AWS Secret Manager** – Used to securely fetch passwords for Snowflake connections

### 🔧 CI/CD & Deployment
- **GitHub** – Code version control and repository management
- **Jenkins** – Automates build processes and creates JAR files for production deployment

### 🧪 (Upcoming) Testing
- **Unit Testing Frameworks** – (Planned for implementation) to validate Spark transformations and logic

## 📥 Data Sources

We collect data from three main sources:

1. **Snowflake** – a cloud data warehouse  
2. **Amazon S3** – a cloud storage service  
3. **Web API** – to get data directly from the internet

---

## 🔧 How It Works

- We have **4 Apache Spark jobs**:
  - **3 Spark jobs** are used to **read and process** data from each source.
  - 1 **Master Spark job** reads the processed data from the above jobs, combines it, and writes the final output.

- All the data is cleaned, formatted, and saved in **Amazon S3** in a columnar format called **Parquet**.

- The final output is also written to **Snowflake** so that other teams can use it.

---

## 🤖 Automation with Airflow

We use **Apache Airflow** to automate the entire pipeline. It does the following:

- Creates the Spark cluster
- Runs all Spark jobs in the right order
- Shuts down the cluster after the job is done

This runs **every day at 10 AM IST** automatically.

---

## ⚙️ Technical Highlights

- We use **Amazon EMR** (Elastic MapReduce) to run Spark jobs
- **Airflow DAG** is stored in S3
- **Snowflake password** is securely fetched from **AWS Secrets Manager**
- Each day, we handle:
  - ~2 TB from S3  
  - ~500 GB to 1 TB from Snowflake  
  - ~1 GB from the API

---
## ⚙️ Detailed Explanation of Spark Jobs

Our data pipeline is made up of **4 Spark jobs** that work together to read, process, and combine data from multiple sources. Here's a breakdown of what each job does:

---

### 🔹 Job 1: Snowflake Data Extraction

**Goal:**  
Extract data from **Snowflake**, clean it, and save it to S3.

**Steps:**
1. Connect to the Snowflake data warehouse using Spark.
2. Read required tables or data using SQL queries.
3. Apply transformations like:
   - Removing null or duplicate values
   - Formatting timestamps or strings
   - Flattening nested structures if any
4. Save the transformed data as **Parquet files** to an intermediate S3 path.

---

### 🔹 Job 2: S3 Data Extraction

**Goal:**  
Read raw data directly stored in **Amazon S3**, transform it, and write cleaned data back to S3.

**Steps:**
1. Read raw files (usually CSV, JSON, or Parquet) from the input S3 location.
2. Perform cleaning steps:
   - Convert formats
   - Drop unwanted columns
   - Normalize field names
3. Write the processed data back to a **separate intermediate S3 location** in Parquet format.

---

### 🔹 Job 3: Web API Data Extraction

**Goal:**  
Fetch data from an external **Web API**, transform it using Spark, and store it in S3.

**Steps:**
1. Use Python or Scala code to call the API and get JSON/CSV responses.
2. Convert API response into a Spark DataFrame.
3. Perform necessary transformations:
   - Flatten JSON data
   - Clean malformed entries
   - Reformat the schema for consistency
4. Save the output to an **intermediate S3 location** in Parquet format.

---

### 🔹 Job 4: Master Spark Job (Final Aggregation and Load)

**Goal:**  
Combine all the intermediate data from the above 3 jobs, perform additional processing, and load it into **S3** and **Snowflake**.

**Steps:**
1. Read the 3 intermediate datasets from their respective S3 paths.
2. Join and merge the data into a unified format.
3. Apply any final business logic, aggregations, or transformations.
4. Write the final dataset to:
   - **Amazon S3** (final output location)
   - **Snowflake** (different account or database)

This job is considered the **final step** of the pipeline and provides clean, unified data for downstream teams to use.

---

✅ **All jobs use Spark DataFrame APIs (DSL) for processing.**  
💡 **Parquet format is used everywhere for efficient storage and fast read/write.**



## 🛠️ Challenges Faced

- Schema changes in Snowflake caused job failures – we fixed it by updating our Spark code.
- Memory issues – solved using Spark’s dynamic resource allocation.
- Skewed data – solved by applying salting techniques.

---

## ✅ Summary

This project helped us understand how real-world data pipelines work using big data tools and cloud services. It includes working with large amounts of data, writing Spark code, automating workflows, and handling production issues.


