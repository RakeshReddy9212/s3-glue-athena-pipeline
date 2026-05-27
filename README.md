# AWS Glue ETL Pipeline with Athena

This project demonstrates a simple AWS Data Engineering pipeline using AWS Glue, PySpark, Amazon S3, and Amazon Athena.

## Project Overview

The ETL pipeline performs the following operations:

- Reads parquet data from Amazon S3
- Applies data quality filters using PySpark
- Creates derived columns from timestamp data
- Calculates fare-per-mile metrics
- Writes transformed parquet data back to S3
- Partitions output data for optimized Athena querying

The transformed dataset can be queried directly using Amazon Athena.

---

## Technologies Used

- AWS Glue
- PySpark
- Amazon S3
- Amazon Athena

---

## Architecture

```text
Raw Parquet Data (S3)
        ↓
AWS Glue ETL Job
        ↓
PySpark Transformations
        ↓
Partitioned Parquet Output (S3)
        ↓
Amazon Athena Queries
```

---

## Transformations Performed

### Data Quality Filters

The pipeline removes records where:

- passenger_count <= 0
- trip_distance <= 0
- fare_amount <= 0

---

### Derived Columns

Additional columns created:

- trip_year
- trip_month
- trip_day
- fare_per_mile

---

## Partitioning Strategy

The transformed data is partitioned by:

- trip_year
- trip_month

This improves query performance and reduces Athena query costs through partition pruning.

---

## Output Format

The transformed dataset is stored in:

- Parquet format
- Partitioned structure

Example:

```text
trip_year=2025/trip_month=5/
```

---

## Sample Athena Query

```sql
SELECT
    trip_year,
    COUNT(*) AS total_trips
FROM taxi_data
GROUP BY trip_year;
```

---

## Purpose of the Project

This project was created to practice:

- AWS Glue ETL development
- PySpark transformations
- S3-based data lake processing
- Athena-based analytics
- Partitioned parquet optimization

---

## Author

Rakesh Sarikonda
