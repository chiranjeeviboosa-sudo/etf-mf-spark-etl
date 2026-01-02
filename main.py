from pyspark.sql import SparkSession

# 1️⃣ Create Spark Session (THIS WAS MISSING EARLIER)
spark = (
    SparkSession.builder
    .appName("Local PySpark Demo")
    .master("local[*]")
    .getOrCreate()
)

# 2️⃣ Sample data
data = [
    (1, "Chiru", "Data Engineer", 80000),
    (2, "Sai", "Analyst", 60000),
    (3, "Ravi", "Developer", 70000)
]

columns = ["id", "name", "role", "salary"]

# 3️⃣ Create DataFrame
df = spark.createDataFrame(data, columns)

# 4️⃣ Show Data
df.show()

# 5️⃣ Print Schema
df.printSchema()

# 6️⃣ Stop Spark
spark.stop()
