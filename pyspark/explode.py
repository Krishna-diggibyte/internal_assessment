from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode, split

data = [
    ("John", "python, sql"),
    ("Aravind", "Java,SQL,HTML"),
    ("Sridevi", "Python,sql,pyspark")
]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("skills", StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.display()

new_df = df.withColumn('languages', explode(split(df.skills, ','))).drop(df.skills)
new_df.display()