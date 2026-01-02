# ================================
# Employee ETL using PySpark (Local)
# ================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------------
# 1️⃣ Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("Employee ETL Local") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created")

# -------------------------------
# 2️⃣ Create Source DataFrame
# -------------------------------
data = [
    (1, "Chiru", "Data Engineer", 80000),
    (2, "Sai", "Analyst", 60000),
    (3, "Ravi", "Developer", 70000)
]

columns = ["id", "name", "role", "salary"]

df = spark.createDataFrame(data, columns)

print("✅ Source DataFrame created")
df.show()
df.printSchema()

# -------------------------------
# 3️⃣ Transformation (ETL Logic)
# -------------------------------
# Business rule: salary > 65000
df_high_salary = df.filter(col("salary") > 65000)

print("✅ Transformation applied (salary > 65000)")
df_high_salary.show()

# -------------------------------
# 4️⃣ Write Output (LOAD)
# -------------------------------
# IMPORTANT:
# Spark creates the folder itself
# Do NOT create it manually

output_path = "data/output/employees_high_salary_v1"

df_high_salary.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"✅ Data written successfully to: {output_path}")

# -------------------------------
# 5️⃣ Validate Output
# -------------------------------
df_check = spark.read.parquet(output_path)

print("✅ Reading back written data")
df_check.show()

# -------------------------------
# 6️⃣ Stop Spark
# -------------------------------
spark.stop()
print("✅ Spark session stopped")
