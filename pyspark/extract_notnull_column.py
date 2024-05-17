from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import when

data2 = [
    ("Aravind", None, None),
    ("John", None, None),
    (None, "Sridevi", None)
]
schema2 = StructType([
    StructField("name1", StringType(), True),
    StructField("name2", StringType(), True),
    StructField("name3", StringType(), True)
])

assign2 = spark.createDataFrame(data=data2, schema=schema2)
# assign2.display()

df2 = assign2.withColumn('fullname', when(assign2.name1.isNotNull(), assign2.name1)
                         .when(assign2.name2.isNotNull(), assign2.name2)
                         .when(assign2.name3.isNotNull(), assign2.name3))

df2.select(df2.fullname).display()